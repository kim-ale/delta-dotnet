//! Same-image kernel FFI host with an additive, ID-based Azure credential ABI.

#[cfg(not(all(target_pointer_width = "64", target_endian = "little")))]
compile_error!("credential ABI v1 requires a 64-bit little-endian target");

mod abi;
mod broker;
mod engine;

#[cfg(test)]
mod tests;

pub use abi::{
    KernelCredentialBegin, KernelCredentialCancel, KernelCredentialOptionV1,
    KernelCredentialRegistrationV1, KernelCredentialReleased, KernelCredentialRequestV1,
    KernelCredentialResultV1,
};

use std::panic::{catch_unwind, AssertUnwindSafe};

use abi::*;
use kernel_ffi::error::{AllocateErrorFn, ExternResult};
use kernel_ffi::handle::Handle;
use kernel_ffi::{KernelStringSlice, SharedExternEngine};

/// Stock engine result type; the generated header uses the stock C typedef.
pub type CredentialEngineResult = ExternResult<Handle<SharedExternEngine>>;

/// Creates a provider-backed default engine in the same image as all stock exports.
/// Zero worker threads selects two owned runtime workers; zero blocking threads uses
/// Tokio's default. The registration must be activated and still attachable.
///
/// # Safety
/// URI/options must describe readable, borrowed UTF-8 for this call. The allocator
/// must remain valid through every engine clone and stock operation using it.
#[no_mangle]
pub unsafe extern "C" fn kernel_engine_new_with_credential_v1(
    table_uri: KernelStringSlice,
    options: *const KernelCredentialOptionV1,
    option_count: u64,
    context_id: u64,
    allocate_error: AllocateErrorFn,
    worker_threads: u32,
    max_blocking_threads: u32,
) -> CredentialEngineResult {
    match catch_unwind(AssertUnwindSafe(|| unsafe {
        engine::construct(
            table_uri,
            options,
            option_count,
            context_id,
            allocate_error,
            worker_threads,
            max_blocking_threads,
        )
    })) {
        Ok(Ok(handle)) => ExternResult::Ok(handle),
        Ok(Err(message)) => engine::error(allocate_error, message),
        Err(_) => engine::error(allocate_error, "kernel credential host: internal failure"),
    }
}

fn control(operation: impl FnOnce() -> Result<(), u32>) -> u32 {
    catch_unwind(AssertUnwindSafe(operation))
        .map_or(INTERNAL_FAILURE, |result| result.err().unwrap_or(OK))
}

/// Returns the credential wire ABI version, independently of the stock kernel ABI.
#[no_mangle]
pub extern "C" fn kernel_credential_abi_version() -> u32 {
    ABI_VERSION
}

/// Reserves a nonreused context ID; writes zero on failure.
///
/// # Safety
/// `context_id` must point to writable u64 storage for this call.
#[no_mangle]
pub unsafe extern "C" fn kernel_credential_context_new(context_id: *mut u64) -> u32 {
    control(|| {
        if context_id.is_null() {
            return Err(INVALID_ARGUMENT);
        }
        unsafe { context_id.write_unaligned(0) };
        let identity = broker::context_new()?;
        unsafe { context_id.write_unaligned(identity) };
        Ok(())
    })
}

/// Publishes a validated, dormant callback registration.
///
/// # Safety
/// The descriptor header and, for a supported header, full descriptor must be readable.
/// Callbacks must remain callable until Released and must not unwind or block.
#[no_mangle]
pub unsafe extern "C" fn kernel_credential_register(
    registration: *const KernelCredentialRegistrationV1,
) -> u32 {
    control(|| {
        if registration.is_null() {
            return Err(INVALID_ARGUMENT);
        }
        let header = unsafe { registration.cast::<Header>().read_unaligned() };
        validate_header::<KernelCredentialRegistrationV1>(header)?;
        broker::register(unsafe { registration.read_unaligned() })
    })
}

/// Enables callbacks after the caller publishes its static callback roots.
#[no_mangle]
pub extern "C" fn kernel_credential_activate(context_id: u64) -> u32 {
    control(|| broker::activate(context_id))
}

/// Stops new engine attachments without revoking existing consumers.
#[no_mangle]
pub extern "C" fn kernel_credential_unregister(context_id: u64) -> u32 {
    control(|| broker::unregister(context_id))
}

/// Claims a pending request and copies its bounded payload synchronously.
/// Unknown, retired and duplicate keys return IGNORED without reading `result`.
///
/// # Safety
/// A live key requires a readable descriptor header/full supported descriptor and
/// readable token bytes for the validated count. All memory is borrowed for this call.
#[no_mangle]
pub unsafe extern "C" fn kernel_credential_complete(
    context_id: u64,
    request_id: u64,
    result: *const KernelCredentialResultV1,
) -> u32 {
    catch_unwind(AssertUnwindSafe(|| unsafe {
        broker::complete(context_id, request_id, result)
    }))
    .unwrap_or(2)
}
