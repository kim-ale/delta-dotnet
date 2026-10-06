use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use delta_http_headers::{
    ByteSlice, HeaderCallbacks, Provider, StockConnector, StockHeaderConnector, StorageEndpoint,
};
use deltalake::logstore::object_store::azure::{AzureConfigKey, MicrosoftAzureBuilder};
use deltalake::logstore::object_store::client::{ReqwestConnector, SpawnedReqwestConnector};
use deltalake::logstore::{client_options_from_certificate, StorageConfig};
use deltalake::DeltaTableBuilder;
use url::Url;

#[no_mangle]
pub extern "C" fn bridge_headers_register(callbacks: *const HeaderCallbacks) -> u32 {
    let Some(callbacks) = (unsafe { callbacks.as_ref() }) else {
        return 1;
    };
    unsafe { delta_http_headers::register(callbacks) }
}

#[no_mangle]
pub extern "C" fn bridge_headers_unregister(context: u64) {
    delta_http_headers::unregister(context);
}

#[no_mangle]
pub extern "C" fn bridge_headers_complete(
    context: u64,
    request: u64,
    status: u32,
    bytes: ByteSlice,
) -> u32 {
    unsafe { delta_http_headers::complete(context, request, status, bytes) }
}

pub(crate) fn configuration_error() -> deltalake::DeltaTableError {
    deltalake::DeltaTableError::Generic("invalid header storage configuration".to_owned())
}

pub(crate) fn table_url(table_uri: &str) -> deltalake::DeltaResult<Url> {
    let url = Url::parse(table_uri).map_err(|_| configuration_error())?;
    if !matches!(url.scheme(), "az" | "adl" | "azure" | "abfs" | "abfss") {
        return Err(configuration_error());
    }
    Ok(url)
}

pub(crate) async fn with_provider(
    builder: DeltaTableBuilder,
    url: &Url,
    provider: Arc<Provider>,
) -> deltalake::DeltaResult<DeltaTableBuilder> {
    if !matches!(url.scheme(), "az" | "adl" | "azure" | "abfs" | "abfss") {
        return Err(configuration_error());
    }
    let config = StorageConfig::parse_options(builder.storage_options())
        .map_err(|_| configuration_error())?;
    let mut azure = MicrosoftAzureBuilder::new()
        .with_url(url.to_string())
        .with_retry(config.retry.clone());
    if let Some(runtime) = &config.runtime {
        azure = azure.with_http_connector(SpawnedReqwestConnector::new(runtime.get_handle()));
    }
    if let Some(path) = config
        .certificate
        .as_ref()
        .and_then(|certificate| certificate.certificate_path.as_ref())
    {
        azure = azure.with_client_options(
            client_options_from_certificate(path).map_err(|_| configuration_error())?,
        );
    }
    let explicit = config
        .raw
        .iter()
        .filter_map(|(key, value)| {
            AzureConfigKey::from_str(&key.to_ascii_lowercase())
                .ok()
                .map(|key| (key, value.clone()))
        })
        .collect();
    let environment = std::env::vars_os()
        .filter_map(|(key, value)| {
            let (key, value) = (key.to_str()?, value.to_str()?);
            if !key.starts_with("AZURE_") {
                return None;
            }
            AzureConfigKey::from_str(&key.to_ascii_lowercase())
                .ok()
                .map(|key| (key, value.to_owned()))
        })
        .collect();
    for (key, value) in azure_configuration(explicit, environment) {
        azure = azure.with_config(key, value);
    }
    if azure
        .get_config_value(&AzureConfigKey::UseEmulator)
        .as_deref()
        != Some("false")
    {
        return Err(configuration_error());
    }
    azure.clone().build().map_err(|_| configuration_error())?;
    let endpoint =
        StorageEndpoint::from_azure_builder(&azure).map_err(|_| configuration_error())?;
    let stock = match &config.runtime {
        Some(runtime) => {
            StockConnector::Spawned(SpawnedReqwestConnector::new(runtime.get_handle()))
        }
        None => StockConnector::Reqwest(ReqwestConnector::default()),
    };
    let store = azure
        .with_http_connector(StockHeaderConnector::new(stock, provider, endpoint))
        .build()
        .map_err(|_| configuration_error())?;
    Ok(builder.with_storage_backend(Arc::new(store), url.clone()))
}

fn azure_configuration(
    mut explicit: HashMap<AzureConfigKey, String>,
    environment: HashMap<AzureConfigKey, String>,
) -> HashMap<AzureConfigKey, String> {
    use AzureConfigKey::*;
    let priority: &[&[AzureConfigKey]] = &[
        &[AccessKey],
        &[SasKey],
        &[Token],
        &[ClientId, ClientSecret, AuthorityId],
        &[AuthorityId, ClientId, FederatedTokenFile],
    ];
    let complete = |keys: &&[AzureConfigKey]| keys.iter().all(|key| explicit.contains_key(key));
    let available = |keys: &&[AzureConfigKey]| {
        keys.iter()
            .all(|key| explicit.contains_key(key) || environment.contains_key(key))
    };
    let selected = if explicit.contains_key(&UseAzureCli) || priority.iter().any(complete) {
        Some(&[][..])
    } else {
        priority
            .iter()
            .find(|keys| keys.iter().any(|key| explicit.contains_key(key)) && available(keys))
            .or_else(|| priority.iter().find(|keys| available(keys)))
            .copied()
    };
    if let Some(keys) = selected {
        for key in keys {
            if let Some(value) = environment.get(key) {
                explicit.entry(*key).or_insert_with(|| value.clone());
            }
        }
    }
    let credential_keys = [
        ClientId,
        ClientSecret,
        FederatedTokenFile,
        SasKey,
        Token,
        MsiEndpoint,
        ObjectId,
        MsiResourceId,
    ];
    for (key, value) in environment {
        if selected.is_none() || !credential_keys.contains(&key) {
            explicit.entry(key).or_insert(value);
        }
    }
    explicit
}

#[cfg(test)]
mod tests {
    use super::*;
    use AzureConfigKey::*;

    fn configuration(values: &[(AzureConfigKey, &str)]) -> HashMap<AzureConfigKey, String> {
        values
            .iter()
            .map(|(key, value)| (*key, (*value).to_owned()))
            .collect()
    }

    #[test]
    fn p2_complete_explicit_credential_omits_environment_identity() {
        let result = azure_configuration(
            configuration(&[(Token, "explicit"), (AccountName, "account")]),
            configuration(&[
                (ClientId, "environment"),
                (ClientSecret, "secret"),
                (AuthorityId, "tenant"),
                (MsiEndpoint, "identity"),
            ]),
        );
        assert_eq!(result.get(&Token).unwrap(), "explicit");
        assert!(!result.contains_key(&ClientId));
        assert!(!result.contains_key(&ClientSecret));
        assert!(!result.contains_key(&MsiEndpoint));
        assert_eq!(result.get(&AuthorityId).unwrap(), "tenant");
    }

    #[test]
    fn p2_partial_explicit_credential_precedes_complete_environment_credential() {
        let result = azure_configuration(
            configuration(&[(ClientId, "explicit")]),
            configuration(&[
                (Token, "token"),
                (ClientSecret, "secret"),
                (AuthorityId, "tenant"),
            ]),
        );
        assert_eq!(result.get(&ClientId).unwrap(), "explicit");
        assert_eq!(result.get(&ClientSecret).unwrap(), "secret");
        assert!(!result.contains_key(&Token));
    }

    #[test]
    fn p2_environment_priority_and_noncredential_merge_match_stock_factory() {
        let result = azure_configuration(
            HashMap::new(),
            configuration(&[
                (AccessKey, "key"),
                (Token, "token"),
                (SasKey, "sas"),
                (Endpoint, "https://storage.example"),
            ]),
        );
        assert_eq!(result.get(&AccessKey).unwrap(), "key");
        assert!(!result.contains_key(&Token));
        assert!(!result.contains_key(&SasKey));
        assert_eq!(result.get(&Endpoint).unwrap(), "https://storage.example");
    }

    #[test]
    fn p2_explicit_cli_presence_and_default_workload_keys_match_stock_factory() {
        let result = azure_configuration(
            configuration(&[(UseAzureCli, "false")]),
            configuration(&[(ClientId, "environment"), (FederatedTokenFile, "file")]),
        );
        assert!(!result.contains_key(&ClientId));
        let fallback =
            azure_configuration(HashMap::new(), configuration(&[(ClientId, "environment")]));
        assert_eq!(fallback.get(&ClientId).unwrap(), "environment");
    }
}
