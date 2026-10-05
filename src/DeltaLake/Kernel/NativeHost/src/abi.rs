use std::mem::size_of;

pub(crate) const ABI_VERSION: u32 = 1;
pub(crate) const AZURE_BEARER: u32 = 1;
pub(crate) const MAX_TOKEN_BYTES: u32 = 65_536;
#[cfg(test)]
pub(crate) const DEFAULT_TIMEOUT_MS: u32 = 30_000;
pub(crate) const MINIMUM_LIFETIME_MS: u32 = 90_000;

pub(crate) const OK: u32 = 0;
pub(crate) const INVALID_ARGUMENT: u32 = 1;
pub(crate) const CLOSED: u32 = 2;
pub(crate) const DUPLICATE_ID: u32 = 3;
pub(crate) const UNSUPPORTED_ABI: u32 = 4;
pub(crate) const INTERNAL_FAILURE: u32 = 5;
pub(crate) const CAPACITY_EXHAUSTED: u32 = 6;

/// Nonblocking managed request admission callback.
pub type KernelCredentialBegin = unsafe extern "C" fn(*const KernelCredentialRequestV1) -> u32;
/// Nonblocking cooperative cancellation callback.
pub type KernelCredentialCancel = unsafe extern "C" fn(u64, u64, u32);
/// Final callback, after all native consumer and acquisition leases retire.
pub type KernelCredentialReleased = unsafe extern "C" fn(u64);

/// Borrowed request descriptor. The callback must copy it before returning.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct KernelCredentialRequestV1 {
    pub abi_version: u32,
    pub struct_size: u32,
    pub context_id: u64,
    pub request_id: u64,
    pub credential_kind: u32,
    pub timeout_ms: u32,
    pub minimum_lifetime_ms: u32,
    pub flags: u32,
    pub reserved: [u64; 2],
}

/// Borrowed completion descriptor; accepted bytes are copied during completion.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct KernelCredentialResultV1 {
    pub abi_version: u32,
    pub struct_size: u32,
    pub status: u32,
    pub credential_kind: u32,
    pub token_utf8: *const u8,
    pub token_len: u64,
    pub expires_unix_ms: i64,
    pub reserved: [u64; 2],
}

/// Immutable registration descriptor, copied only after validation.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct KernelCredentialRegistrationV1 {
    pub abi_version: u32,
    pub struct_size: u32,
    pub context_id: u64,
    pub credential_kind: u32,
    pub acquisition_timeout_ms: u32,
    pub max_token_bytes: u32,
    pub flags: u32,
    pub begin: Option<unsafe extern "C" fn(*const KernelCredentialRequestV1) -> u32>,
    pub cancel: Option<unsafe extern "C" fn(u64, u64, u32)>,
    pub released: Option<unsafe extern "C" fn(u64)>,
    pub reserved: [u64; 2],
}

/// Counted, borrowed UTF-8 option pair for the additive engine constructor.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct KernelCredentialOptionV1 {
    pub key_utf8: *const u8,
    pub key_len: u64,
    pub value_utf8: *const u8,
    pub value_len: u64,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub(crate) struct Header {
    pub abi_version: u32,
    pub struct_size: u32,
}

pub(crate) fn validate_header<Type>(header: Header) -> Result<(), u32> {
    if header.abi_version != ABI_VERSION {
        return Err(UNSUPPORTED_ABI);
    }
    if header.struct_size as usize != size_of::<Type>() {
        return Err(INVALID_ARGUMENT);
    }
    Ok(())
}

pub(crate) fn validate_registration(value: &KernelCredentialRegistrationV1) -> Result<(), u32> {
    validate_header::<KernelCredentialRegistrationV1>(Header {
        abi_version: value.abi_version,
        struct_size: value.struct_size,
    })?;
    if value.context_id == 0
        || value.credential_kind != AZURE_BEARER
        || !(1_000..=120_000).contains(&value.acquisition_timeout_ms)
        || !(1..=MAX_TOKEN_BYTES).contains(&value.max_token_bytes)
        || value.flags != 0
        || value.reserved != [0; 2]
        || value.begin.is_none()
        || value.cancel.is_none()
        || value.released.is_none()
    {
        return Err(INVALID_ARGUMENT);
    }
    Ok(())
}

pub(crate) fn validate_result_descriptor(
    value: &KernelCredentialResultV1,
    limit: u32,
) -> Result<(), u32> {
    validate_header::<KernelCredentialResultV1>(Header {
        abi_version: value.abi_version,
        struct_size: value.struct_size,
    })?;
    if value.credential_kind != AZURE_BEARER
        || value.reserved != [0; 2]
        || value.status > 5
        || !(1..=MAX_TOKEN_BYTES).contains(&limit)
    {
        return Err(INVALID_ARGUMENT);
    }
    if value.status == 0 {
        if value.token_utf8.is_null() || value.token_len == 0 || value.token_len > u64::from(limit)
        {
            return Err(INVALID_ARGUMENT);
        }
    } else if !value.token_utf8.is_null() || value.token_len != 0 || value.expires_unix_ms != 0 {
        return Err(INVALID_ARGUMENT);
    }
    Ok(())
}

pub(crate) fn validate_token(bytes: &[u8], expiry: i64, now: i64) -> Result<u64, u32> {
    if bytes.is_empty()
        || bytes.len() > MAX_TOKEN_BYTES as usize
        || std::str::from_utf8(bytes).is_err()
    {
        return Err(INVALID_ARGUMENT);
    }
    let unpadded = bytes
        .iter()
        .position(|byte| *byte == b'=')
        .unwrap_or(bytes.len());
    if unpadded == 0
        || !bytes[..unpadded].iter().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~' | b'+' | b'/')
        })
        || !bytes[unpadded..].iter().all(|byte| *byte == b'=')
    {
        return Err(INVALID_ARGUMENT);
    }
    let useful_ms = expiry
        .checked_sub(now)
        .and_then(|remaining| remaining.checked_sub(i64::from(MINIMUM_LIFETIME_MS)))
        .filter(|remaining| *remaining > 0)
        .ok_or(INVALID_ARGUMENT)?;
    Ok((useful_ms as u64).min(86_400_000))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem::{align_of, offset_of};

    #[test]
    fn layouts_match_p0_x64_contract() {
        assert_eq!(size_of::<KernelCredentialRequestV1>(), 56);
        assert_eq!(size_of::<KernelCredentialResultV1>(), 56);
        assert_eq!(size_of::<KernelCredentialRegistrationV1>(), 72);
        assert_eq!(size_of::<KernelCredentialOptionV1>(), 32);
        assert_eq!(align_of::<KernelCredentialRequestV1>(), 8);
        assert_eq!(offset_of!(KernelCredentialRequestV1, context_id), 8);
        assert_eq!(offset_of!(KernelCredentialRequestV1, request_id), 16);
        assert_eq!(offset_of!(KernelCredentialRequestV1, credential_kind), 24);
        assert_eq!(offset_of!(KernelCredentialRequestV1, timeout_ms), 28);
        assert_eq!(
            offset_of!(KernelCredentialRequestV1, minimum_lifetime_ms),
            32
        );
        assert_eq!(offset_of!(KernelCredentialRequestV1, flags), 36);
        assert_eq!(offset_of!(KernelCredentialRequestV1, reserved), 40);
        assert_eq!(offset_of!(KernelCredentialResultV1, status), 8);
        assert_eq!(offset_of!(KernelCredentialResultV1, credential_kind), 12);
        assert_eq!(offset_of!(KernelCredentialResultV1, token_utf8), 16);
        assert_eq!(offset_of!(KernelCredentialResultV1, token_len), 24);
        assert_eq!(offset_of!(KernelCredentialResultV1, expires_unix_ms), 32);
        assert_eq!(offset_of!(KernelCredentialResultV1, reserved), 40);
        assert_eq!(offset_of!(KernelCredentialRegistrationV1, begin), 32);
        assert_eq!(offset_of!(KernelCredentialRegistrationV1, cancel), 40);
        assert_eq!(offset_of!(KernelCredentialRegistrationV1, released), 48);
        assert_eq!(offset_of!(KernelCredentialRegistrationV1, reserved), 56);
    }

    #[test]
    fn token_validation_rejects_padding_utf8_and_unusable_expiry() {
        for bytes in [
            b"".as_slice(),
            b"=",
            b"==",
            b"a=b",
            b"a\r\n",
            b"a b",
            b"\xff",
            b"\0",
        ] {
            assert!(validate_token(bytes, 1_000_000, 0).is_err());
        }
        assert_eq!(
            validate_token(b"synthetic.A-_/+~==", 1_000_000, 0),
            Ok(910_000)
        );
        assert!(validate_token(b"A", 90_000, 0).is_err());
        assert!(validate_token(b"A", i64::MAX, -1).is_err());
        assert!(validate_token(&vec![b'A'; 65_537], 1_000_000, 0).is_err());
    }

    #[test]
    fn descriptor_enforces_registration_cap_before_copy() {
        let mut result = KernelCredentialResultV1 {
            abi_version: 1,
            struct_size: 56,
            status: 0,
            credential_kind: 1,
            token_utf8: b"AAAA".as_ptr(),
            token_len: 4,
            expires_unix_ms: 1_000_000,
            reserved: [0; 2],
        };
        assert!(validate_result_descriptor(&result, 3).is_err());
        assert!(validate_result_descriptor(&result, 4).is_ok());
        result.status = 2;
        assert!(validate_result_descriptor(&result, 4).is_err());
    }
}
