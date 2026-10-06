use std::collections::HashMap;
use std::sync::Arc;

use delta_http_headers::{Provider, StockHeaderConnector, StorageEndpoint};
use delta_kernel::object_store::azure::MicrosoftAzureBuilder;
use delta_kernel::object_store::client::ReqwestConnector;
use delta_kernel::object_store::path::Path;
use delta_kernel::object_store::{ObjectStore, ObjectStoreScheme};
use delta_kernel::{DeltaResult, Error};
use url::Url;

pub(crate) fn build_store(
    url: &Url,
    options: HashMap<String, String>,
    provider: Arc<Provider>,
) -> DeltaResult<Arc<dyn ObjectStore>> {
    let (scheme, path) = ObjectStoreScheme::parse(url).map_err(|_| store_error())?;
    Path::parse(path).map_err(|_| store_error())?;
    if scheme != ObjectStoreScheme::MicrosoftAzure {
        return Err(Error::generic("header engine storage provider unsupported"));
    }
    let builder = options.into_iter().fold(
        MicrosoftAzureBuilder::new().with_url(url.to_string()),
        |builder, (key, value)| match key.to_ascii_lowercase().parse() {
            Ok(key) => builder.with_config(key, value),
            Err(_) => builder,
        },
    );
    let endpoint = StorageEndpoint::from_azure_builder(&builder).map_err(|_| store_error())?;
    let connector = StockHeaderConnector::new(ReqwestConnector::default(), provider, endpoint);
    let store = builder
        .with_http_connector(connector)
        .build()
        .map_err(|_| store_error())?;
    Ok(Arc::new(store))
}

fn store_error() -> Error {
    Error::generic("header engine storage configuration invalid")
}
