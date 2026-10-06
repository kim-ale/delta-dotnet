//! Isolated same-image host for the pinned kernel FFI.

mod exports;
mod store;

#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::mem::{align_of, size_of};
use std::sync::Arc;

use delta_http_headers::{ByteSlice, HeaderCallbacks};
use delta_kernel::{DeltaResult, Engine, Error};
use delta_kernel_default_engine::executor::tokio::TokioMultiThreadExecutor;
use delta_kernel_default_engine::DefaultEngineBuilder;
use ffi::error::{AllocateError, AllocateErrorFn, ExternResult};
use ffi::handle::Handle;
use ffi::{ExternEngine, KernelStringSlice, SharedExternEngine};
use upstream_ffi as ffi;

const MAX_STRING_BYTES: usize = 64 * 1024;
const MAX_OPTIONS: usize = 256;
const MAX_INPUT_BYTES: usize = 1024 * 1024;
const _: () = assert!(size_of::<KernelStringSlice>() == size_of::<ByteSlice>());
const _: () = assert!(align_of::<KernelStringSlice>() == align_of::<ByteSlice>());

struct HeaderEngine {
    engine: Arc<dyn Engine>,
    allocator: AllocateErrorFn,
}

trait IntoExternResult<T> {
    unsafe fn into_extern_result(self, allocator: &dyn AllocateError) -> ExternResult<T>;
}

impl<T> IntoExternResult<T> for DeltaResult<T> {
    unsafe fn into_extern_result(self, allocator: &dyn AllocateError) -> ExternResult<T> {
        match self {
            Ok(value) => ExternResult::Ok(value),
            Err(error) => {
                let message = error.to_string();
                ExternResult::Err(allocator.allocate_error(error.into(), borrowed_string(&message)))
            }
        }
    }
}

impl ExternEngine for HeaderEngine {
    fn engine(&self) -> Arc<dyn Engine> {
        self.engine.clone()
    }

    fn error_allocator(&self) -> &dyn AllocateError {
        &self.allocator
    }
}

/// Registers the shared version-one callback descriptor in this image's broker.
/// Returns the shared ABI status: 0 accepted, 1 invalid, 2 duplicate, 3 capacity.
///
/// # Safety
///
/// The descriptor must be readable for this call. Its callbacks must be prompt,
/// non-unwinding process-lifetime trampolines; context roots survive Released.
#[no_mangle]
pub unsafe extern "C" fn kernel_headers_register(callbacks: *const HeaderCallbacks) -> u32 {
    match callbacks.as_ref() {
        Some(callbacks) => delta_http_headers::register(callbacks),
        None => 1,
    }
}

/// Removes management registration without retiring existing store/request owners.
#[no_mangle]
pub extern "C" fn kernel_headers_unregister(context: u64) {
    delta_http_headers::unregister(context);
}

/// Completes an acquisition using the shared ABI: 0 claimed, 1 ignored.
///
/// # Safety
///
/// A successful known request must supply readable bytes for this call; unknown
/// or failed requests do not require a readable payload. The broker copies it.
#[no_mangle]
pub unsafe extern "C" fn kernel_headers_complete(
    context: u64,
    request: u64,
    status: u32,
    bytes: ByteSlice,
) -> u32 {
    delta_http_headers::complete(context, request, status, bytes)
}

/// Constructs a provider-aware Azure engine in the same image as upstream FFI.
/// Input strings are synchronously copied. Registration must precede this call.
/// Failures use the supplied allocator and fixed, sanitized kernel errors.
///
/// # Safety
///
/// Nonempty counted strings and option arrays must be readable for this call.
/// The allocator is a non-unwinding callback retained through all engine owners.
/// Returned handles must only be used and freed by this host's upstream exports.
#[no_mangle]
pub unsafe extern "C" fn kernel_engine_with_headers(
    path: KernelStringSlice,
    keys: *const KernelStringSlice,
    values: *const KernelStringSlice,
    count: usize,
    context: u64,
    allocate_error: AllocateErrorFn,
) -> ExternResult<Handle<SharedExternEngine>> {
    construct_engine(&path, keys, values, count, context, allocate_error)
        .into_extern_result(&allocate_error)
}

unsafe fn construct_engine(
    path: &KernelStringSlice,
    keys: *const KernelStringSlice,
    values: *const KernelStringSlice,
    count: usize,
    context: u64,
    allocator: AllocateErrorFn,
) -> DeltaResult<Handle<SharedExternEngine>> {
    if count > MAX_OPTIONS
        || (count != 0
            && (keys.is_null()
                || values.is_null()
                || !(keys as usize).is_multiple_of(align_of::<KernelStringSlice>())
                || !(values as usize).is_multiple_of(align_of::<KernelStringSlice>())))
    {
        return Err(Error::generic("invalid header engine input"));
    }
    let mut budget = MAX_INPUT_BYTES;
    let path = copy_string(path, &mut budget)?;
    let mut options = HashMap::new();
    for index in 0..count {
        options.insert(
            copy_string(&*keys.add(index), &mut budget)?,
            copy_string(&*values.add(index), &mut budget)?,
        );
    }
    let url = delta_kernel::try_parse_uri(&path)
        .map_err(|_| Error::generic("invalid header engine location"))?;
    let provider = delta_http_headers::acquire(context)
        .map_err(|_| Error::generic("header context unavailable"))?;
    let store = store::build_store(&url, options, provider)?;
    let executor = TokioMultiThreadExecutor::new_owned_runtime(Some(2), None)
        .map_err(|_| Error::generic("header engine executor unavailable"))?;
    let engine = Arc::new(
        DefaultEngineBuilder::new(store)
            .with_task_executor(Arc::new(executor))
            .build(),
    );
    let engine: Arc<dyn ExternEngine> = Arc::new(HeaderEngine { engine, allocator });
    Ok(engine.into())
}

/// Reads the pinned repr(C) counted-string fields through the shared byte-slice
/// type, never through a copied Rust handle layout.
///
/// # Safety
///
/// Nonempty slices must address readable memory for this call.
unsafe fn copy_string(slice: &KernelStringSlice, budget: &mut usize) -> DeltaResult<String> {
    let bytes = std::ptr::read((slice as *const KernelStringSlice).cast::<ByteSlice>());
    if bytes.len > MAX_STRING_BYTES
        || bytes.len > *budget
        || (bytes.len != 0 && bytes.data.is_null())
    {
        return Err(Error::generic("invalid header engine input"));
    }
    *budget -= bytes.len;
    if bytes.len == 0 {
        return Ok(String::new());
    }
    std::str::from_utf8(std::slice::from_raw_parts(bytes.data, bytes.len))
        .map(str::to_owned)
        .map_err(|_| Error::generic("invalid header engine input"))
}

fn borrowed_string(value: &str) -> KernelStringSlice {
    unsafe {
        std::mem::transmute(ByteSlice {
            data: value.as_ptr(),
            len: value.len(),
        })
    }
}
