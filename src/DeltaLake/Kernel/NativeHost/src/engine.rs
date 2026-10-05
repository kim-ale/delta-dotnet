use std::collections::HashMap;
use std::os::raw::c_char;
use std::sync::Arc;

use delta_kernel::engine::default::executor::tokio::TokioMultiThreadExecutor;
use delta_kernel::engine::default::DefaultEngineBuilder;
use delta_kernel::object_store::azure::{AzureConfigKey, MicrosoftAzureBuilder};
use delta_kernel::object_store::{DynObjectStore, ObjectStoreScheme};
use delta_kernel::Engine;
use kernel_ffi::error::{AllocateError, AllocateErrorFn, ExternResult, KernelError};
use kernel_ffi::handle::Handle;
use kernel_ffi::{ExternEngine, KernelStringSlice, SharedExternEngine};

use crate::abi::KernelCredentialOptionV1;
use crate::broker::{self, Provider, Registration};

const MAX_OPTION_COUNT: u64 = 256;
const MAX_STRING_BYTES: u64 = 65_536;
const MAX_OPTION_BYTES: u64 = 1_048_576;

pub(crate) struct CredentialEngine {
    engine: Arc<dyn Engine>,
    allocate_error: AllocateErrorFn,
    _registration: Arc<Registration>,
}

#[repr(C)]
struct StringSliceParts {
    ptr: *const c_char,
    len: usize,
}

impl ExternEngine for CredentialEngine {
    fn engine(&self) -> Arc<dyn Engine> {
        self.engine.clone()
    }
    fn error_allocator(&self) -> &dyn AllocateError {
        &self.allocate_error
    }
}

pub(crate) unsafe fn construct(
    table_uri: KernelStringSlice,
    options: *const KernelCredentialOptionV1,
    option_count: u64,
    context_id: u64,
    allocate_error: AllocateErrorFn,
    worker_threads: u32,
    max_blocking_threads: u32,
) -> Result<Handle<SharedExternEngine>, &'static str> {
    let uri = unsafe { copy_kernel_string(&table_uri) }?;
    let options = unsafe { copy_options(options, option_count) }?;
    let registration =
        broker::attach(context_id).map_err(|_| "kernel credential host: context closed")?;
    let store = build_store(&uri, options, registration.clone())?;
    if worker_threads > 256 || max_blocking_threads > 65_536 {
        return Err("kernel credential host: invalid executor limits");
    }
    let executor = TokioMultiThreadExecutor::new_owned_runtime(
        Some(if worker_threads == 0 {
            2
        } else {
            worker_threads as usize
        }),
        (max_blocking_threads != 0).then_some(max_blocking_threads as usize),
    )
    .map_err(|_| "kernel credential host: executor unavailable")?;
    let engine = Arc::new(
        DefaultEngineBuilder::new(store)
            .with_task_executor(Arc::new(executor))
            .build(),
    );
    let wrapper: Arc<dyn ExternEngine> = Arc::new(CredentialEngine {
        engine,
        allocate_error,
        _registration: registration,
    });
    Ok(wrapper.into())
}

pub(crate) fn build_store(
    uri: &str,
    options: Vec<(String, String)>,
    registration: Arc<Registration>,
) -> Result<Arc<DynObjectStore>, &'static str> {
    let url = delta_kernel::try_parse_uri(uri)
        .map_err(|_| "kernel credential host: invalid table URI")?;
    if !matches!(url.scheme(), "az" | "adl" | "azure" | "abfs" | "abfss")
        || !matches!(
            ObjectStoreScheme::parse(&url),
            Ok((ObjectStoreScheme::MicrosoftAzure, _))
        )
        || url.query().is_some()
        || url.fragment().is_some()
        || url.password().is_some()
    {
        return Err("kernel credential host: unsupported table URI");
    }
    let mut recognized = HashMap::new();
    let mut builder = MicrosoftAzureBuilder::new().with_url(url.to_string());
    for (key, value) in options {
        let Ok(key) = key.to_ascii_lowercase().parse::<AzureConfigKey>() else {
            continue;
        };
        validate_option(key, &value)?;
        if let Some(previous) = recognized.insert(key, value.clone()) {
            if previous != value {
                return Err("kernel credential host: conflicting option aliases");
            }
        }
        builder = builder.with_config(key, value);
    }
    let store = builder
        .with_credentials(Arc::new(Provider(registration)))
        .build()
        .map_err(|_| "kernel credential host: invalid Azure configuration")?;
    Ok(Arc::new(store))
}

fn validate_option(key: AzureConfigKey, value: &str) -> Result<(), &'static str> {
    match key {
        AzureConfigKey::Endpoint => {
            let endpoint =
                url::Url::parse(value).map_err(|_| "kernel credential host: invalid endpoint")?;
            if !matches!(endpoint.scheme(), "http" | "https")
                || !endpoint.username().is_empty()
                || endpoint.password().is_some()
                || endpoint.query().is_some()
                || endpoint.fragment().is_some()
            {
                return Err("kernel credential host: endpoint authentication forbidden");
            }
            Ok(())
        }
        AzureConfigKey::AccessKey
        | AzureConfigKey::ClientSecret
        | AzureConfigKey::SasKey
        | AzureConfigKey::Token
        | AzureConfigKey::MsiEndpoint
        | AzureConfigKey::ObjectId
        | AzureConfigKey::MsiResourceId
        | AzureConfigKey::FederatedTokenFile
        | AzureConfigKey::FabricTokenServiceUrl
        | AzureConfigKey::FabricSessionToken => {
            Err("kernel credential host: conflicting authentication option")
        }
        AzureConfigKey::UseEmulator
        | AzureConfigKey::SkipSignature
        | AzureConfigKey::UseAzureCli => {
            if value.eq_ignore_ascii_case("false") {
                Ok(())
            } else {
                Err("kernel credential host: authentication mode forbidden")
            }
        }
        _ => Ok(()),
    }
}

unsafe fn copy_options(
    options: *const KernelCredentialOptionV1,
    count: u64,
) -> Result<Vec<(String, String)>, &'static str> {
    if count > MAX_OPTION_COUNT || (count > 0 && options.is_null()) {
        return Err("kernel credential host: invalid option array");
    }
    let mut copied = Vec::with_capacity(count as usize);
    let mut total = 0u64;
    for index in 0..count as usize {
        let pair = unsafe { options.add(index).read_unaligned() };
        total = total
            .checked_add(pair.key_len)
            .and_then(|size| size.checked_add(pair.value_len))
            .filter(|size| *size <= MAX_OPTION_BYTES)
            .ok_or("kernel credential host: option limit exceeded")?;
        if pair.key_len == 0 || pair.key_len > 1_024 {
            return Err("kernel credential host: invalid option key");
        }
        let key = unsafe { copy_utf8(pair.key_utf8, pair.key_len) }?;
        let value = unsafe { copy_utf8(pair.value_utf8, pair.value_len) }?;
        copied.push((key, value));
    }
    Ok(copied)
}

unsafe fn copy_utf8(pointer: *const u8, count: u64) -> Result<String, &'static str> {
    if count > MAX_STRING_BYTES || (count > 0 && pointer.is_null()) {
        return Err("kernel credential host: invalid string descriptor");
    }
    if count == 0 {
        return Ok(String::new());
    }
    let bytes = unsafe { std::slice::from_raw_parts(pointer, count as usize) };
    let text = std::str::from_utf8(bytes).map_err(|_| "kernel credential host: invalid UTF-8")?;
    if text.contains('\0') {
        return Err("kernel credential host: invalid string content");
    }
    Ok(text.to_owned())
}

unsafe fn copy_kernel_string(slice: &KernelStringSlice) -> Result<String, &'static str> {
    let parts = unsafe {
        (slice as *const KernelStringSlice)
            .cast::<StringSliceParts>()
            .read_unaligned()
    };
    if parts.len == 0 {
        return Err("kernel credential host: empty table URI");
    }
    unsafe { copy_utf8(parts.ptr.cast(), parts.len as u64) }
}

pub(crate) fn string_slice(text: &str) -> KernelStringSlice {
    unsafe {
        std::mem::transmute(StringSliceParts {
            ptr: text.as_ptr().cast(),
            len: text.len(),
        })
    }
}

pub(crate) fn error(
    allocator: AllocateErrorFn,
    message: &'static str,
) -> ExternResult<Handle<SharedExternEngine>> {
    ExternResult::Err(allocator(KernelError::GenericError, string_slice(message)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_aliases_reject_authentication_secrets_and_enabled_bypass() {
        for key in [
            "AZURE_STORAGE_ACCESS_KEY",
            "bearer_token",
            "token",
            "azure_client_secret",
            "sas_token",
            "msi_endpoint",
            "fabric_session_token",
            "fabric_token_service_url",
        ] {
            let typed = key.to_ascii_lowercase().parse::<AzureConfigKey>().unwrap();
            assert!(
                validate_option(typed, "synthetic").is_err(),
                "alias must be rejected"
            );
        }
        for key in [
            AzureConfigKey::UseEmulator,
            AzureConfigKey::SkipSignature,
            AzureConfigKey::UseAzureCli,
        ] {
            for value in ["false", "FALSE", "False", "fAlSe"] {
                assert!(
                    validate_option(key, value).is_ok(),
                    "disabled mode must be accepted: {key:?}={value:?}"
                );
            }
            for value in ["true", "TRUE", "True", "", "0", "1", " false ", "invalid"] {
                assert!(
                    validate_option(key, value).is_err(),
                    "enabled or invalid mode must be rejected: {key:?}={value:?}"
                );
            }
        }
        assert!(validate_option(AzureConfigKey::ClientId, "public-client").is_ok());
        assert!(validate_option(AzureConfigKey::UseFabricEndpoint, "true").is_ok());
    }

    #[test]
    fn endpoints_are_nonsecret_and_cannot_select_another_authentication_path() {
        for endpoint in [
            "https://user:secret@example.invalid",
            "https://example.invalid?sig=synthetic",
            "https://example.invalid#synthetic",
            "file:///endpoint",
        ] {
            assert!(validate_option(AzureConfigKey::Endpoint, endpoint).is_err());
        }
        assert!(validate_option(AzureConfigKey::Endpoint, "http://127.0.0.1:12345").is_ok());
        assert!(validate_option(
            AzureConfigKey::Endpoint,
            "https://account.blob.core.windows.net"
        )
        .is_ok());
    }
}
