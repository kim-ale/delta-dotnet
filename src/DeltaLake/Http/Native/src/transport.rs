use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use async_trait::async_trait;
use http::{Method, Uri};
use object_store::azure::{AzureAccessKey, AzureCredential, MicrosoftAzureBuilder};
use object_store::client::{
    HttpClient, HttpConnector, HttpError, HttpRequest, HttpResponse, HttpService, ReqwestConnector,
    SpawnedReqwestConnector, StaticCredentialProvider,
};
use object_store::path::Path;
use object_store::signer::Signer;
use object_store::ClientOptions;

use crate::protocol::provider_error;
use crate::{HeaderError, Provider};

/// Exact storage target captured by the Azure construction caller from its actual
/// effective builder endpoint. No host suffix or cloud-wide trust is inferred.
#[derive(Clone)]
pub struct StorageEndpoint {
    origin: Uri,
    path_prefix: String,
}

/// Only the pinned stock connectors can be decorated. No replacement reqwest client.
pub enum StockConnector {
    /// Uses the original ReqwestConnector and its effective ClientOptions.
    Reqwest(ReqwestConnector),
    /// Preserves the original SpawnedReqwestConnector runtime for transport I/O.
    Spawned(SpawnedReqwestConnector),
}

/// Acquires fresh headers before each approved original send. Internal redirect
/// hops stay inside the stock client, bypass the callback, and may forward custom
/// headers to other targets according to stock redirect policy. Callers accept this.
/// Direct identity/unapproved requests pass unchanged and do not invoke the provider.
pub struct StockHeaderConnector {
    inner: StockConnector,
    provider: Arc<Provider>,
    endpoint: StorageEndpoint,
}

#[derive(Clone)]
struct ScopedService {
    inner: HttpClient,
    provider: Arc<Provider>,
    endpoint: StorageEndpoint,
}

impl fmt::Debug for StorageEndpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("StorageEndpoint { target: <redacted> }")
    }
}

impl fmt::Debug for StockConnector {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Reqwest(_) => "StockConnector::Reqwest",
            Self::Spawned(_) => "StockConnector::Spawned",
        })
    }
}

impl fmt::Debug for StockHeaderConnector {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("StockHeaderConnector { provider: <redacted>, target: <redacted> }")
    }
}

impl fmt::Debug for ScopedService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ScopedService { provider: <redacted>, client: <redacted> }")
    }
}

impl From<ReqwestConnector> for StockConnector {
    fn from(connector: ReqwestConnector) -> Self {
        Self::Reqwest(connector)
    }
}

impl From<SpawnedReqwestConnector> for StockConnector {
    fn from(connector: SpawnedReqwestConnector) -> Self {
        Self::Spawned(connector)
    }
}

impl StorageEndpoint {
    /// Captures the stock builder's storage root without I/O or credential acquisition.
    ///
    /// # Errors
    /// Rejects invalid builders, endpoints, or a signer that is not immediately ready.
    /// Errors never include builder options or signing data.
    pub fn from_azure_builder(builder: &MicrosoftAzureBuilder) -> Result<Self, HeaderError> {
        let access_key = "c2NvcGUtcHJvYmU=";
        let key = AzureAccessKey::try_new(access_key).map_err(|_| HeaderError::InvalidEndpoint)?;
        let probe = builder
            .clone()
            .with_access_key(access_key)
            .with_credentials(Arc::new(StaticCredentialProvider::new(
                AzureCredential::AccessKey(key),
            )))
            .with_skip_signature(false)
            .build()
            .map_err(|_| HeaderError::InvalidEndpoint)?;
        let path = Path::default();
        let mut signing =
            std::pin::pin!(probe.signed_url(Method::GET, &path, Duration::from_secs(1)));
        let mut root = match signing
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        {
            Poll::Ready(Ok(root)) => root,
            _ => return Err(HeaderError::InvalidEndpoint),
        };
        root.set_query(None);
        Self::new(
            root.as_str()
                .parse()
                .map_err(|_| HeaderError::InvalidEndpoint)?,
            root.path(),
        )
    }

    /// Approves only an exact scheme/authority and a path prefix on segment boundaries.
    /// The prefix must include any path in the effective builder endpoint (for example
    /// an emulator account path), and normally includes the Azure container root.
    ///
    /// # Errors
    /// Rejects non-HTTP, relative, credential-bearing, query-bearing or normalized
    /// endpoints and ambiguous/mismatched prefixes. Errors never include supplied data.
    pub fn new(origin: Uri, path_prefix: &str) -> Result<Self, HeaderError> {
        let parsed =
            url::Url::parse(&origin.to_string()).map_err(|_| HeaderError::InvalidEndpoint)?;
        if !matches!(origin.scheme_str(), Some("http" | "https"))
            || origin.authority().is_none()
            || !parsed.username().is_empty()
            || parsed.password().is_some()
            || origin.query().is_some()
            || parsed.fragment().is_some()
            || parsed.path() != origin.path()
            || !path_prefix.starts_with('/')
            || path_prefix.len() > crate::protocol::MAX_BYTES
            || !path_matches(origin.path(), path_prefix)
        {
            return Err(HeaderError::InvalidEndpoint);
        }
        let prefix_uri: Uri = path_prefix
            .parse()
            .map_err(|_| HeaderError::InvalidEndpoint)?;
        let mut prefix_url = parsed;
        prefix_url.set_path(path_prefix);
        if prefix_uri.query().is_some()
            || prefix_uri.path() != path_prefix
            || prefix_url.path() != path_prefix
        {
            return Err(HeaderError::InvalidEndpoint);
        }
        Ok(Self {
            origin,
            path_prefix: path_prefix.to_owned(),
        })
    }

    fn approves(&self, destination: &Uri) -> bool {
        if !self.matches_original_scope(destination) {
            return false;
        }
        url::Url::parse(&destination.to_string())
            .is_ok_and(|parsed| parsed.path() == destination.path())
    }

    fn matches_original_scope(&self, destination: &Uri) -> bool {
        destination.scheme() == self.origin.scheme()
            && destination.authority() == self.origin.authority()
            && path_matches(&self.path_prefix, destination.path())
    }
}

impl StockHeaderConnector {
    /// Keeps the exact stock connector, native provider owner, and captured endpoint.
    /// Header acquisition runs on the calling time-enabled Tokio runtime, even when
    /// stock transport I/O is delegated to a SpawnedReqwestConnector runtime.
    pub fn new(
        inner: impl Into<StockConnector>,
        provider: Arc<Provider>,
        endpoint: StorageEndpoint,
    ) -> Self {
        Self {
            inner: inner.into(),
            provider,
            endpoint,
        }
    }
}

impl HttpConnector for StockHeaderConnector {
    fn connect(&self, options: &ClientOptions) -> object_store::Result<HttpClient> {
        let inner = match &self.inner {
            StockConnector::Reqwest(connector) => connector.connect(options)?,
            StockConnector::Spawned(connector) => connector.connect(options)?,
        };
        Ok(HttpClient::new(ScopedService {
            inner,
            provider: self.provider.clone(),
            endpoint: self.endpoint.clone(),
        }))
    }
}

#[async_trait]
impl HttpService for ScopedService {
    async fn call(&self, mut request: HttpRequest) -> Result<HttpResponse, HttpError> {
        if self.endpoint.matches_original_scope(request.uri()) {
            if !self.endpoint.approves(request.uri()) {
                return Err(provider_error());
            }
            let headers = self.provider.headers(&request).await?;
            crate::headers::validate_for_request(&headers, request.headers())
                .map_err(|_| provider_error())?;
            request.headers_mut().extend(headers);
        }
        self.inner.execute(request).await
    }
}

fn path_matches(prefix: &str, path: &str) -> bool {
    path == prefix
        || path
            .strip_prefix(prefix)
            .is_some_and(|rest| prefix.ends_with('/') || rest.starts_with('/'))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broker::tests::{request, serial, Fixture};
    use http::{HeaderMap, HeaderValue};
    use object_store::azure::{
        AzureAccessKey, AzureAuthorizer, AzureCredential, MicrosoftAzureBuilder,
    };
    use object_store::path::Path;
    use object_store::{ObjectStoreExt, RetryConfig};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

    const TEST_ACCESS_KEY: &str = "c3ludGhldGljLWtleQ==";

    fn endpoint(server: &MockServer) -> StorageEndpoint {
        StorageEndpoint::new(server.uri().parse().unwrap(), "/container/").unwrap()
    }

    fn connector(server: &MockServer, fixture: &Fixture) -> StockHeaderConnector {
        StockHeaderConnector::new(
            ReqwestConnector::default(),
            fixture.provider(),
            endpoint(server),
        )
    }

    fn store(
        server: &MockServer,
        connector: StockHeaderConnector,
    ) -> object_store::azure::MicrosoftAzure {
        MicrosoftAzureBuilder::new()
            .with_account("test")
            .with_container_name("container")
            .with_endpoint(server.uri())
            .with_allow_http(true)
            .with_bearer_token_authorization("synthetic-storage-credential")
            .with_retry(RetryConfig {
                max_retries: 2,
                ..Default::default()
            })
            .with_http_connector(connector)
            .build()
            .unwrap()
    }

    fn shared_key_store(
        server: &MockServer,
        connector: StockHeaderConnector,
    ) -> object_store::azure::MicrosoftAzure {
        MicrosoftAzureBuilder::new()
            .with_account("test")
            .with_container_name("container")
            .with_endpoint(server.uri())
            .with_allow_http(true)
            .with_access_key(TEST_ACCESS_KEY)
            .with_retry(RetryConfig {
                max_retries: 2,
                ..Default::default()
            })
            .with_http_connector(connector)
            .build()
            .unwrap()
    }

    fn successful_head() -> ResponseTemplate {
        ResponseTemplate::new(200)
            .insert_header("etag", "\"synthetic\"")
            .insert_header("last-modified", "Mon, 05 Oct 2026 00:00:00 GMT")
            .insert_header("content-length", "4")
    }

    #[test]
    fn azure_builder_capture_is_ready_without_runtime() {
        let builder = MicrosoftAzureBuilder::new()
            .with_url("abfss://container@test.dfs.core.windows.net/table")
            .with_bearer_token_authorization("synthetic-storage-credential")
            .with_skip_signature(true);
        for (builder, root) in [
            (
                builder.clone(),
                "https://test.blob.core.windows.net/container",
            ),
            (
                builder
                    .clone()
                    .with_endpoint("http://127.0.0.1:10000/base".to_owned())
                    .with_allow_http(true),
                "http://127.0.0.1:10000/base/container",
            ),
        ] {
            let endpoint = StorageEndpoint::from_azure_builder(&builder).unwrap();
            assert!(endpoint.approves(&format!("{root}/object").parse().unwrap()));
            assert!(!endpoint.approves(&format!("{root}-sibling/object").parse().unwrap()));
            assert!(endpoint.origin.query().is_none());
        }
        let emulator =
            StorageEndpoint::from_azure_builder(&builder.with_use_emulator(true)).unwrap();
        assert!(emulator.path_prefix.ends_with("/test/container"));
    }

    #[tokio::test]
    async fn azure_builder_capture_performs_no_io_or_header_callback() {
        let _serial = serial().await;
        let server = MockServer::start().await;
        let fixture = Fixture::new(Some(br#"{"x-custom":"provider-secret"}"#.to_vec()));
        let builder = MicrosoftAzureBuilder::new()
            .with_account("test")
            .with_container_name("container")
            .with_endpoint(server.uri())
            .with_allow_http(true)
            .with_bearer_token_authorization("synthetic-storage-credential")
            .with_http_connector(connector(&server, &fixture));
        let endpoint = StorageEndpoint::from_azure_builder(&builder).unwrap();
        assert!(server.received_requests().await.unwrap().is_empty());
        assert_eq!(fixture.state.calls.load(Ordering::SeqCst), 0);
        Mock::given(method("HEAD"))
            .and(path("/container/object"))
            .respond_with(successful_head())
            .expect(1)
            .mount(&server)
            .await;
        let store = builder
            .with_http_connector(StockHeaderConnector::new(
                ReqwestConnector::default(),
                fixture.provider(),
                endpoint,
            ))
            .build()
            .unwrap();
        store.head(&Path::from("object")).await.unwrap();
        assert_eq!(fixture.state.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn shared_key_signed_headers_fail_closed_without_send() {
        let _serial = serial().await;
        let server = MockServer::start().await;
        for name in [
            "X-MS-Client-Request-ID",
            "x-ms-meta-custom",
            "x-mscustom",
            "Content-Encoding",
            "Content-Language",
            "Content-MD5",
            "Content-Type",
            "Date",
            "If-Modified-Since",
            "If-Match",
            "If-None-Match",
            "If-Unmodified-Since",
            "Range",
            "Content-Length",
        ] {
            let payload = serde_json::json!({name: "provider-secret", "x-custom": "allowed"});
            let fixture = Fixture::new(Some(serde_json::to_vec(&payload).unwrap()));
            let store = shared_key_store(&server, connector(&server, &fixture));
            let error = store.head(&Path::from("sample")).await.unwrap_err();
            assert!(error.to_string().contains("header provider failed"));
            let debug = format!("{error:?}");
            assert!(!debug.contains("provider-secret"));
            assert!(!debug.contains(name));
            assert!(!debug.contains("SharedKey"));
            assert!(!debug.contains(TEST_ACCESS_KEY));
            assert!(
                server.received_requests().await.unwrap().is_empty(),
                "{name}"
            );
            assert_eq!(fixture.state.calls.load(Ordering::SeqCst), 1);
            drop(store);
        }
    }

    #[tokio::test]
    async fn shared_key_custom_header_preserves_native_authorization() {
        let _serial = serial().await;
        let fixture = Fixture::new(Some(br#"{"x-custom":"provider-secret"}"#.to_vec()));
        let server = MockServer::start().await;
        Mock::given(method("HEAD"))
            .respond_with(successful_head())
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
        let store = shared_key_store(&server, connector(&server, &fixture));
        store.head(&Path::from("sample")).await.unwrap();
        let requests = server.received_requests().await.unwrap();
        assert_eq!(requests.len(), 1);
        assert!(requests[0].headers["authorization"]
            .to_str()
            .unwrap()
            .starts_with("SharedKey test:"));
        assert_eq!(requests[0].headers["x-custom"], "provider-secret");

        let client = connector(&server, &fixture)
            .connect(&ClientOptions::new().with_allow_http(true))
            .unwrap();
        let mut request = request(&format!("{}/container/blob", server.uri()));
        let credential =
            AzureCredential::AccessKey(AzureAccessKey::try_new(TEST_ACCESS_KEY).unwrap());
        AzureAuthorizer::new(&credential, "test").authorize(&mut request);
        let authorization = request.headers()["authorization"].clone();
        let date = request.headers()["date"].clone();
        let version = request.headers()["x-ms-version"].clone();
        client.execute(request).await.unwrap();
        let requests = server.received_requests().await.unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[1].headers["authorization"], authorization);
        assert_eq!(requests[1].headers["date"], date);
        assert_eq!(requests[1].headers["x-ms-version"], version);
        assert_eq!(requests[1].headers["x-custom"], "provider-secret");
        assert_eq!(fixture.state.calls.load(Ordering::SeqCst), 2);
        drop(client);
        drop(store);
    }

    #[tokio::test]
    async fn bearer_fabric_headers_preserve_native_authorization() {
        let _serial = serial().await;
        let fixture = Fixture::new(Some(
            br#"{"x-ms-client-request-id":"request-id","x-ms-proxy-host":"fabric.invalid","x-ms-fabric-proxy-authorization":"fabric-secret"}"#
                .to_vec(),
        ));
        let server = MockServer::start().await;
        Mock::given(method("HEAD"))
            .respond_with(successful_head())
            .mount(&server)
            .await;
        let store = store(&server, connector(&server, &fixture));
        store.head(&Path::from("sample")).await.unwrap();
        let requests = server.received_requests().await.unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0].headers["authorization"],
            "Bearer synthetic-storage-credential"
        );
        assert_eq!(requests[0].headers["x-ms-client-request-id"], "request-id");
        assert_eq!(requests[0].headers["x-ms-proxy-host"], "fabric.invalid");
        assert_eq!(
            requests[0].headers["x-ms-fabric-proxy-authorization"],
            "fabric-secret"
        );
        assert_eq!(fixture.state.calls.load(Ordering::SeqCst), 1);
        drop(store);
    }

    #[tokio::test]
    async fn sas_fabric_headers_preserve_native_query() {
        let _serial = serial().await;
        let fixture = Fixture::new(Some(
            br#"{"x-ms-client-request-id":"request-id","x-ms-proxy-host":"fabric.invalid","x-ms-fabric-proxy-authorization":"fabric-secret"}"#
                .to_vec(),
        ));
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
        let client = connector(&server, &fixture)
            .connect(&ClientOptions::new().with_allow_http(true))
            .unwrap();
        let mut request = request(&format!("{}/container/blob", server.uri()));
        let credential =
            AzureCredential::SASToken(vec![("sig".to_owned(), "synthetic-sas".to_owned())]);
        AzureAuthorizer::new(&credential, "test").authorize(&mut request);
        let uri = request.uri().clone();
        let date = request.headers()["date"].clone();
        client.execute(request).await.unwrap();
        let requests = server.received_requests().await.unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].url.query(), uri.query());
        assert!(!requests[0].headers.contains_key("authorization"));
        assert_eq!(requests[0].headers["date"], date);
        assert_eq!(requests[0].headers["x-ms-client-request-id"], "request-id");
        assert_eq!(requests[0].headers["x-ms-proxy-host"], "fabric.invalid");
        assert_eq!(
            requests[0].headers["x-ms-fabric-proxy-authorization"],
            "fabric-secret"
        );
        assert_eq!(fixture.state.calls.load(Ordering::SeqCst), 1);
        drop(client);
    }

    #[test]
    fn exact_endpoint_scope_has_no_suffix_or_path_confusion() {
        let endpoint = StorageEndpoint::new(
            "https://storage.example/base".parse().unwrap(),
            "/base/container",
        )
        .unwrap();
        for uri in [
            "https://storage.example/base/container",
            "https://storage.example/base/container/blob?sig=private",
        ] {
            assert!(endpoint.approves(&uri.parse().unwrap()));
        }
        for uri in [
            "http://storage.example/base/container/blob",
            "https://storage.example:443/base/container/blob",
            "https://other.storage.example/base/container/blob",
            "https://storage.example.attacker/base/container/blob",
            "https://storage.example/base/container-other/blob",
            "https://storage.example/base/identity",
            "https://storage.example/base/container/../identity",
            "https://storage.example/base/container/%2e%2e/identity",
        ] {
            assert!(!endpoint.approves(&uri.parse().unwrap()), "{uri}");
        }
        assert!(!format!("{endpoint:?}").contains("storage.example"));
    }

    #[test]
    fn invalid_endpoint_configuration_is_sanitized() {
        for (origin, prefix) in [
            ("/relative", "/container/"),
            ("ftp://storage.example", "/container/"),
            ("https://user:secret@storage.example", "/container/"),
            ("https://storage.example?secret=value", "/container/"),
            ("https://storage.example/base", "/container/"),
            ("https://storage.example", "container/"),
            ("https://storage.example", "/container/../identity"),
            ("https://storage.example", "/container/?secret=value"),
        ] {
            let error = StorageEndpoint::new(origin.parse().unwrap(), prefix).unwrap_err();
            assert_eq!(error.to_string(), "invalid header storage endpoint");
        }
    }

    #[tokio::test]
    async fn stock_redirects_forward_headers_without_another_callback() {
        let _serial = serial().await;
        for status in [301, 302, 303, 307, 308] {
            let fixture = Fixture::new(Some(br#"{"x-custom":"secret"}"#.to_vec()));
            let server = MockServer::start().await;
            let target = MockServer::start().await;
            Mock::given(method("HEAD"))
                .respond_with(
                    ResponseTemplate::new(status)
                        .insert_header("location", format!("{}/redirected", target.uri())),
                )
                .mount(&server)
                .await;
            Mock::given(method("HEAD"))
                .respond_with(successful_head())
                .mount(&target)
                .await;
            let store = store(&server, connector(&server, &fixture));
            store.head(&Path::from("sample")).await.unwrap();
            assert_eq!(fixture.state.calls.load(Ordering::SeqCst), 1);
            let requests = target.received_requests().await.unwrap();
            assert_eq!(requests.len(), 1);
            assert_eq!(requests[0].headers["x-custom"], "secret");
            drop(store);
        }
    }

    #[tokio::test]
    async fn stock_options_and_native_authorization_are_preserved() {
        let _serial = serial().await;
        let fixture = Fixture::new(Some(br#"{"x-custom":"secret"}"#.to_vec()));
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
        let connector = connector(&server, &fixture);
        let mut defaults = HeaderMap::new();
        defaults.insert("x-default", HeaderValue::from_static("preserved"));
        let client = connector
            .connect(
                &ClientOptions::new()
                    .with_allow_http(true)
                    .with_user_agent(HeaderValue::from_static("stock-options"))
                    .with_default_headers(defaults),
            )
            .unwrap();
        let mut request = request(&format!("{}/container/blob", server.uri()));
        request
            .headers_mut()
            .insert("authorization", HeaderValue::from_static("Bearer native"));
        client.execute(request).await.unwrap();
        let requests = server.received_requests().await.unwrap();
        assert_eq!(requests[0].headers["authorization"], "Bearer native");
        assert_eq!(requests[0].headers["user-agent"], "stock-options");
        assert_eq!(requests[0].headers["x-default"], "preserved");
        assert_eq!(requests[0].headers["x-custom"], "secret");
        let invalid = ClientOptions::new().with_config(
            object_store::ClientConfigKey::UserAgent,
            "invalid\r\nsecret",
        );
        assert!(connector.connect(&invalid).is_err());
        assert!(!format!("{connector:?}").contains("secret"));
        drop(client);
        drop(connector);
    }

    #[tokio::test]
    async fn direct_identity_and_unapproved_requests_pass_unchanged() {
        let _serial = serial().await;
        let fixture = Fixture::new(Some(br#"{"x-custom":"provider-secret"}"#.to_vec()));
        let server = MockServer::start().await;
        let identity = MockServer::start().await;
        for target in [&server, &identity] {
            Mock::given(method("GET"))
                .respond_with(ResponseTemplate::new(200))
                .mount(target)
                .await;
        }
        let client = connector(&server, &fixture)
            .connect(&ClientOptions::new().with_allow_http(true))
            .unwrap();
        for uri in [
            format!("{}/identity", server.uri()),
            format!("{}/container/blob", identity.uri()),
        ] {
            let mut request = request(&uri);
            request
                .headers_mut()
                .insert("x-custom", HeaderValue::from_static("original"));
            request
                .headers_mut()
                .insert("authorization", HeaderValue::from_static("Bearer original"));
            client.execute(request).await.unwrap();
        }
        assert_eq!(fixture.state.calls.load(Ordering::SeqCst), 0);
        for target in [&server, &identity] {
            let requests = target.received_requests().await.unwrap();
            assert_eq!(requests[0].headers["x-custom"], "original");
            assert_eq!(requests[0].headers["authorization"], "Bearer original");
        }
        drop(client);
    }

    #[tokio::test]
    async fn native_oauth_request_bypasses_storage_callback() {
        let _serial = serial().await;
        let fixture = Fixture::new(Some(br#"{"x-custom":"provider-secret"}"#.to_vec()));
        let server = MockServer::start().await;
        let identity = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/tenant/oauth2/v2.0/token"))
            .respond_with(ResponseTemplate::new(200).set_body_raw(
                r#"{"access_token":"synthetic-oauth-token","expires_in":3600}"#,
                "application/json",
            ))
            .mount(&identity)
            .await;
        Mock::given(method("HEAD"))
            .respond_with(successful_head())
            .mount(&server)
            .await;
        let store = MicrosoftAzureBuilder::new()
            .with_account("test")
            .with_container_name("container")
            .with_endpoint(server.uri())
            .with_allow_http(true)
            .with_client_secret_authorization("client", "secret", "tenant")
            .with_authority_host(identity.uri())
            .with_http_connector(connector(&server, &fixture))
            .build()
            .unwrap();
        store.head(&Path::from("sample")).await.unwrap();
        let token_requests = identity.received_requests().await.unwrap();
        assert_eq!(token_requests.len(), 1);
        assert!(!token_requests[0].headers.contains_key("x-custom"));
        let storage_requests = server.received_requests().await.unwrap();
        assert_eq!(
            storage_requests[0].headers["authorization"],
            "Bearer synthetic-oauth-token"
        );
        assert_eq!(storage_requests[0].headers["x-custom"], "provider-secret");
        assert_eq!(fixture.state.calls.load(Ordering::SeqCst), 1);
        drop(store);
    }

    #[tokio::test]
    async fn ambiguous_storage_path_fails_closed_without_callback_or_send() {
        let _serial = serial().await;
        let fixture = Fixture::new(Some(br#"{"x-custom":"secret"}"#.to_vec()));
        let server = MockServer::start().await;
        let client = connector(&server, &fixture)
            .connect(&ClientOptions::new().with_allow_http(true))
            .unwrap();
        for path in [
            "/container/../identity",
            "/container/%2e%2e/identity",
            "/container/part/../blob",
        ] {
            let error = client
                .execute(request(&format!("{}{path}", server.uri())))
                .await
                .unwrap_err();
            assert_eq!(error.kind(), object_store::client::HttpErrorKind::Unknown);
            assert!(!format!("{error:?}").contains("secret"));
        }
        assert_eq!(fixture.state.calls.load(Ordering::SeqCst), 0);
        assert!(server.received_requests().await.unwrap().is_empty());
        drop(client);
    }

    #[tokio::test]
    async fn provider_failure_sends_nothing_and_does_not_retry() {
        let _serial = serial().await;
        let fixture = Fixture::new(Some(br#"{"x-custom":"secret\r\ninjected:yes"}"#.to_vec()));
        let server = MockServer::start().await;
        let store = store(&server, connector(&server, &fixture));
        let error = store.head(&Path::from("sample")).await.unwrap_err();
        assert!(error.to_string().contains("header provider failed"));
        assert!(!format!("{error:?}").contains("secret"));
        assert!(server.received_requests().await.unwrap().is_empty());
        assert_eq!(fixture.state.calls.load(Ordering::SeqCst), 1);
        drop(store);
    }

    struct FailOnce(AtomicUsize);

    impl Respond for FailOnce {
        fn respond(&self, _request: &Request) -> ResponseTemplate {
            if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
                ResponseTemplate::new(500)
            } else {
                successful_head()
            }
        }
    }

    #[tokio::test]
    async fn retries_and_later_sends_invoke_provider_each_time() {
        let _serial = serial().await;
        let fixture = Fixture::new(Some(br#"{"x-custom":"first"}"#.to_vec()));
        let server = MockServer::start().await;
        Mock::given(method("HEAD"))
            .respond_with(FailOnce(AtomicUsize::new(0)))
            .mount(&server)
            .await;
        let store = store(&server, connector(&server, &fixture));
        store.head(&Path::from("sample")).await.unwrap();
        *fixture.state.payload.lock().unwrap() = Some(br#"{"x-custom":"next"}"#.to_vec());
        store.head(&Path::from("sample")).await.unwrap();
        assert_eq!(fixture.state.calls.load(Ordering::SeqCst), 3);
        let requests = server.received_requests().await.unwrap();
        assert_eq!(requests.len(), 3);
        assert_eq!(requests[2].headers["x-custom"], "next");
        drop(store);
    }

    #[tokio::test]
    async fn spawned_stock_connector_and_actual_store_clones_retain_provider() {
        let _serial = serial().await;
        let mut fixture = Fixture::new(Some(br#"{"x-custom":"secret"}"#.to_vec()));
        let server = MockServer::start().await;
        Mock::given(method("HEAD"))
            .respond_with(successful_head())
            .mount(&server)
            .await;
        let connector = StockHeaderConnector::new(
            SpawnedReqwestConnector::new(tokio::runtime::Handle::current()),
            fixture.provider(),
            endpoint(&server),
        );
        let store = Arc::new(store(&server, connector));
        let clone = store.clone();
        crate::unregister(fixture.id);
        fixture.provider.take();
        drop(store);
        assert_eq!(fixture.state.releases.load(Ordering::SeqCst), 0);
        clone.head(&Path::from("sample")).await.unwrap();
        assert_eq!(fixture.state.calls.load(Ordering::SeqCst), 1);
        drop(clone);
        assert_eq!(fixture.state.releases.load(Ordering::SeqCst), 1);
    }
}
