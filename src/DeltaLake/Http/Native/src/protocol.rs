use object_store::client::{HttpError, HttpErrorKind};

/// Version of the exact callback descriptor layout accepted by this crate.
pub const ABI_VERSION: u32 = 1;
pub(crate) const MAX_BYTES: usize = 64 * 1024;

/// Borrowed counted bytes, valid only for the duration of the receiving call.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ByteSlice {
    /// Address of the first byte; null is permitted only when no bytes are read.
    pub data: *const u8,
    /// Byte count, not a character count.
    pub len: usize,
}

impl ByteSlice {
    pub(crate) fn borrowed(bytes: &[u8]) -> Self {
        Self {
            data: bytes.as_ptr(),
            len: bytes.len(),
        }
    }
}

/// Copied synchronously by registration. All three callbacks are required.
/// Callbacks must return promptly, never unwind, and remain callable for process lifetime.
/// Begin returns zero only after accepting work, and must copy method/URI before returning.
/// Cancel is cooperative; released retires native ownership, not managed workers.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct HeaderCallbacks {
    /// Must equal [`ABI_VERSION`].
    pub abi_version: u32,
    /// Must equal `size_of::<HeaderCallbacks>()` for version one.
    pub struct_size: u32,
    /// Nonzero identifier supplied by the managed caller.
    pub context_id: u64,
    /// Receives context, never-reused request, method bytes and original URI bytes.
    pub begin: Option<unsafe extern "C" fn(u64, u64, ByteSlice, ByteSlice) -> u32>,
    /// Receives context and request when an accepted pending future is retired.
    pub cancel: Option<unsafe extern "C" fn(u64, u64)>,
    /// Receives context exactly once on final native owner retirement.
    pub released: Option<unsafe extern "C" fn(u64)>,
}

/// Sanitized errors contain no callback text, URIs, header names or header values.
#[derive(Debug, thiserror::Error)]
pub enum HeaderError {
    /// No management registration exists for this identifier.
    #[error("header context unavailable")]
    ContextUnavailable,
    /// The original storage endpoint configuration is invalid.
    #[error("invalid header storage endpoint")]
    InvalidEndpoint,
    /// Acquisition failed, was malformed, exhausted capacity or timed out.
    #[error("header provider failed")]
    ProviderFailed,
}

pub(crate) fn provider_error() -> HttpError {
    HttpError::new(HttpErrorKind::Unknown, HeaderError::ProviderFailed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem::{offset_of, size_of};

    #[test]
    fn callback_descriptor_has_exact_c_field_order_and_pointer_sized_slices() {
        assert_eq!(offset_of!(HeaderCallbacks, abi_version), 0);
        assert_eq!(offset_of!(HeaderCallbacks, struct_size), 4);
        assert_eq!(offset_of!(HeaderCallbacks, context_id), 8);
        assert_eq!(offset_of!(HeaderCallbacks, begin), 16);
        assert_eq!(offset_of!(HeaderCallbacks, cancel), 16 + size_of::<usize>());
        assert_eq!(
            offset_of!(HeaderCallbacks, released),
            16 + 2 * size_of::<usize>()
        );
        assert_eq!(offset_of!(ByteSlice, data), 0);
        assert_eq!(offset_of!(ByteSlice, len), size_of::<usize>());
        assert_eq!(size_of::<ByteSlice>(), 2 * size_of::<usize>());
        assert_eq!(
            size_of::<Option<unsafe extern "C" fn(u64)>>(),
            size_of::<usize>()
        );
        if size_of::<usize>() == 8 {
            assert_eq!(size_of::<HeaderCallbacks>(), 40);
        }
    }
}
