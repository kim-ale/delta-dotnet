use super::*;
use crate::headers::{bridge_headers_complete, bridge_headers_register, bridge_headers_unregister};
use crate::runtime::runtime_free;
use delta_http_headers::{ByteSlice, HeaderCallbacks, ABI_VERSION};
use deltalake::logstore::object_store::{path::Path, ObjectStoreExt};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{mpsc, Mutex, OnceLock};
use std::time::Duration as StdDuration;
use wiremock::matchers::any;
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

static NEXT_CONTEXT: AtomicU64 = AtomicU64::new(1_000_000);
static STATES: OnceLock<Mutex<HashMap<u64, Arc<ProviderState>>>> = OnceLock::new();
static SERIAL: Mutex<()> = Mutex::new(());
static TABLE_RESULT: Mutex<Option<mpsc::Sender<(usize, usize)>>> = Mutex::new(None);
static EMPTY_RESULT: Mutex<Option<mpsc::Sender<(bool, bool)>>> = Mutex::new(None);
static REENTRY: AtomicUsize = AtomicUsize::new(0);
static WORK_RETIRED: AtomicBool = AtomicBool::new(false);

fn serial() -> std::sync::MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[derive(Default)]
struct ProviderState {
    requests: Mutex<Vec<String>>,
    released: AtomicUsize,
    reject: AtomicBool,
}

struct Fixture {
    context: u64,
    state: Arc<ProviderState>,
}

impl Fixture {
    fn new() -> Self {
        let context = NEXT_CONTEXT.fetch_add(1, Ordering::Relaxed);
        let state = Arc::new(ProviderState::default());
        STATES
            .get_or_init(Mutex::default)
            .lock()
            .unwrap()
            .insert(context, state.clone());
        let callbacks = HeaderCallbacks {
            abi_version: ABI_VERSION,
            struct_size: std::mem::size_of::<HeaderCallbacks>() as u32,
            context_id: context,
            begin: Some(begin),
            cancel: Some(cancel),
            released: Some(released),
        };
        assert_eq!(bridge_headers_register(&callbacks), 0);
        Self { context, state }
    }

    fn provider(&self) -> Arc<Provider> {
        delta_http_headers::acquire(self.context).unwrap()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        bridge_headers_unregister(self.context);
        STATES.get().unwrap().lock().unwrap().remove(&self.context);
    }
}

unsafe extern "C" fn begin(context: u64, request: u64, _method: ByteSlice, uri: ByteSlice) -> u32 {
    let state = STATES
        .get()
        .unwrap()
        .lock()
        .unwrap()
        .get(&context)
        .cloned()
        .unwrap();
    let uri = std::str::from_utf8(std::slice::from_raw_parts(uri.data, uri.len))
        .unwrap()
        .to_owned();
    let sequence = {
        let mut requests = state.requests.lock().unwrap();
        requests.push(uri);
        requests.len()
    };
    if state.reject.load(Ordering::Relaxed) {
        return 1;
    }
    let bytes = format!(r#"{{"x-p2-header":"fresh-{sequence}"}}"#).into_bytes();
    bridge_headers_complete(
        context,
        request,
        0,
        ByteSlice {
            data: bytes.as_ptr(),
            len: bytes.len(),
        },
    )
}

unsafe extern "C" fn cancel(_context: u64, _request: u64) {}

unsafe extern "C" fn released(context: u64) {
    if let Some(state) = STATES.get().unwrap().lock().unwrap().get(&context) {
        state.released.fetch_add(1, Ordering::Relaxed);
    }
}

unsafe extern "C" fn table_result(table: *mut RawDeltaTable, error: *const DeltaTableError) {
    if let Some(sender) = TABLE_RESULT.lock().unwrap().as_ref() {
        let _ = sender.send((table as usize, error as usize));
    }
}

unsafe extern "C" fn empty_result(error: *const DeltaTableError) {
    let retired = WORK_RETIRED.load(Ordering::Acquire);
    let address = REENTRY.swap(0, Ordering::Relaxed);
    let reentered = if retired {
        let table = NonNull::new(address as *mut RawDeltaTable).unwrap();
        let valid = table_version(table) == -1;
        table_free(table);
        valid
    } else {
        false
    };
    if !error.is_null() {
        drop(Box::from_raw(error as *mut DeltaTableError));
    }
    if let Some(sender) = EMPTY_RESULT.lock().unwrap().as_ref() {
        let _ = sender.send((reentered, error.is_null()));
    }
}

fn storage_options(endpoint: Option<&str>) -> HashMap<String, String> {
    let mut options = HashMap::from([
        (
            "azure_storage_account_name".to_owned(),
            "account".to_owned(),
        ),
        ("azure_storage_token".to_owned(), "native-token".to_owned()),
        ("allow_http".to_owned(), "true".to_owned()),
        ("max_retries".to_owned(), "0".to_owned()),
    ]);
    if let Some(endpoint) = endpoint {
        options.insert("azure_storage_endpoint".to_owned(), endpoint.to_owned());
    }
    options
}

fn byte_ref(value: &str) -> ByteArrayRef {
    ByteArrayRef {
        data: value.as_ptr(),
        size: value.len(),
    }
}

fn options_map(options: HashMap<String, String>) -> *mut Map {
    Box::into_raw(Box::new(Map {
        data: options
            .into_iter()
            .map(|(key, value)| (key, Some(value)))
            .collect(),
        disable_free: false,
    }))
}

#[derive(Clone, Default)]
struct BlobServer(Arc<Mutex<HashMap<String, Vec<u8>>>>);

fn blob_response(status: u16, size: usize) -> ResponseTemplate {
    ResponseTemplate::new(status)
        .insert_header("etag", "\"p2\"")
        .insert_header("last-modified", "Mon, 05 Oct 2026 00:00:00 GMT")
        .insert_header("content-length", size.to_string())
}

impl Respond for BlobServer {
    fn respond(&self, request: &Request) -> ResponseTemplate {
        let key = request
            .url
            .path()
            .strip_prefix("/base/container/")
            .unwrap_or("");
        let mut blobs = self.0.lock().unwrap();
        if request
            .url
            .query_pairs()
            .any(|(key, value)| key == "comp" && value == "list")
        {
            let prefix = request
                .url
                .query_pairs()
                .find(|(key, _)| key == "prefix")
                .map(|(_, value)| value.into_owned())
                .unwrap_or_default();
            let entries = blobs.iter().filter(|(key, _)| key.starts_with(&prefix)).map(|(key, bytes)| {
                format!("<Blob><Name>{key}</Name><Properties><Last-Modified>Mon, 05 Oct 2026 00:00:00 GMT</Last-Modified><Etag>p2</Etag><Content-Length>{}</Content-Length><Content-Type>application/octet-stream</Content-Type><BlobType>BlockBlob</BlobType></Properties></Blob>", bytes.len())
            }).collect::<String>();
            return ResponseTemplate::new(200).set_body_string(format!(
                "<EnumerationResults><Blobs>{entries}</Blobs><NextMarker /></EnumerationResults>"
            ));
        }
        match request.method.as_str() {
            "PUT" => {
                blobs.insert(key.to_owned(), request.body.clone());
                blob_response(201, 0)
            }
            "GET" | "HEAD" => match blobs.get(key) {
                Some(bytes) if request.method.as_str() == "HEAD" => blob_response(200, bytes.len()),
                Some(bytes) => {
                    if let Some(range) = request
                        .headers
                        .get("range")
                        .and_then(|value| value.to_str().ok())
                        .and_then(|value| value.strip_prefix("bytes="))
                    {
                        let (start, end) = range.split_once('-').unwrap();
                        let start = start.parse::<usize>().unwrap();
                        let end = end
                            .parse::<usize>()
                            .unwrap_or(bytes.len() - 1)
                            .min(bytes.len() - 1);
                        blob_response(206, end - start + 1)
                            .insert_header(
                                "content-range",
                                format!("bytes {start}-{end}/{}", bytes.len()),
                            )
                            .set_body_bytes(bytes[start..=end].to_vec())
                    } else {
                        blob_response(200, bytes.len()).set_body_bytes(bytes.clone())
                    }
                }
                None => ResponseTemplate::new(404),
            },
            "DELETE" => {
                blobs.remove(key);
                ResponseTemplate::new(202)
            }
            _ => ResponseTemplate::new(400),
        }
    }
}

#[tokio::test]
async fn p2_custom_endpoint_create_load_and_unregister_keep_fresh_headers() {
    let _serial = serial();
    let fixture = Fixture::new();
    let server = MockServer::start().await;
    let blobs = BlobServer::default();
    Mock::given(any())
        .respond_with(blobs.clone())
        .mount(&server)
        .await;
    let endpoint = format!("{}/base", server.uri());
    let mut runtime = Runtime::new(&crate::runtime_options::RuntimeOptions::new()).unwrap();
    let schema = Schema::new(vec![arrow::datatypes::Field::new(
        "value",
        arrow::datatypes::DataType::Int32,
        true,
    )]);
    let table = create_delta_table(
        &mut runtime,
        "az://container/table".to_owned(),
        schema,
        Vec::new(),
        SaveMode::ErrorIfExists,
        None,
        None,
        None,
        Some(storage_options(Some(&endpoint))),
        None,
        Some(fixture.provider()),
    )
    .await
    .unwrap();
    assert_eq!(table.version(), Some(0));
    assert!(blobs
        .0
        .lock()
        .unwrap()
        .contains_key("table/_delta_log/00000000000000000000.json"));
    let provider = fixture.provider();
    bridge_headers_unregister(fixture.context);
    let loaded = table_new_impl(
        "az://container/table",
        -1,
        Some(storage_options(Some(&endpoint))),
        true,
        8,
        Some(provider),
    )
    .await
    .unwrap();
    assert_eq!(loaded.version(), Some(0));
    let requests = server.received_requests().await.unwrap();
    assert!(requests.len() > 1);
    let mut values = std::collections::HashSet::new();
    for request in &requests {
        assert!(
            request.url.path() == "/base/container"
                || request.url.path().starts_with("/base/container/")
        );
        assert_eq!(
            request.headers.get("authorization").unwrap(),
            "Bearer native-token"
        );
        assert!(values.insert(
            request
                .headers
                .get("x-p2-header")
                .unwrap()
                .to_str()
                .unwrap()
                .to_owned()
        ));
    }
    for sequence in 1..=requests.len() {
        assert!(values.contains(&format!("fresh-{sequence}")));
    }
    assert_eq!(fixture.state.released.load(Ordering::Relaxed), 0);
    drop((table, loaded));
    assert_eq!(fixture.state.released.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn p2_standard_abfs_and_fabric_scope_comes_from_stock_builder() {
    let _serial = serial();
    for (uri, expected) in [
        (
            "az://container/table",
            "https://account.blob.core.windows.net/container/table/object",
        ),
        (
            "abfs://container/table",
            "https://account.blob.core.windows.net/container/table/object",
        ),
        (
            "abfss://container@actual.dfs.core.windows.net/table",
            "https://actual.blob.core.windows.net/container/table/object",
        ),
        (
            "abfss://workspace@onelake.dfs.fabric.microsoft.com/table",
            "https://onelake.blob.fabric.microsoft.com/workspace/table/object",
        ),
    ] {
        let fixture = Fixture::new();
        fixture.state.reject.store(true, Ordering::Relaxed);
        let url = crate::headers::table_url(uri).unwrap();
        let builder = DeltaTableBuilder::from_url(url.clone())
            .unwrap()
            .with_storage_options(storage_options(None));
        let table = crate::headers::with_provider(builder, &url, fixture.provider())
            .await
            .unwrap()
            .build()
            .unwrap();
        let result = table
            .log_store()
            .root_object_store(None)
            .head(&Path::from("table/object"))
            .await;
        assert!(result.is_err());
        assert_eq!(
            fixture.state.requests.lock().unwrap().as_slice(),
            &[expected.to_owned()]
        );
        drop(table);
    }
}

#[tokio::test]
async fn p2_unsupported_and_emulator_configuration_errors_are_sanitized() {
    let _serial = serial();
    let fixture = Fixture::new();
    let error = table_new_impl(
        "s3://private-secret/table",
        -1,
        None,
        false,
        0,
        Some(fixture.provider()),
    )
    .await
    .unwrap_err();
    assert_eq!(
        error.to_string(),
        crate::headers::configuration_error().to_string()
    );
    let mut options = storage_options(None);
    options.insert("azure_storage_use_emulator".to_owned(), "true".to_owned());
    let error = table_new_impl(
        "az://container/table",
        -1,
        Some(options),
        false,
        0,
        Some(fixture.provider()),
    )
    .await
    .unwrap_err();
    assert_eq!(
        error.to_string(),
        crate::headers::configuration_error().to_string()
    );
    assert!(fixture.state.requests.lock().unwrap().is_empty());
}

#[test]
fn p2_runtime_clone_survives_runtime_free_and_last_drop_on_worker() {
    let _serial = serial();
    let runtime = Box::into_raw(Box::new(
        Runtime::new(&crate::runtime_options::RuntimeOptions::new()).unwrap(),
    ));
    let owned = unsafe { &*runtime }.clone();
    runtime_free(runtime);
    let (sender, receiver) = mpsc::channel();
    owned.handle().spawn(async move {
        drop(owned);
        sender.send(true).unwrap();
    });
    assert!(receiver.recv_timeout(StdDuration::from_secs(10)).unwrap());
}

fn check_work_retirement(cancelled: bool, fail: bool) {
    struct DropProbe<'a> {
        table: &'a mut RawDeltaTable,
    }
    impl Drop for DropProbe<'_> {
        fn drop(&mut self) {
            let _ = self.table.table.version();
            WORK_RETIRED.store(true, Ordering::Release);
        }
    }
    let runtime = NonNull::new(Box::into_raw(Box::new(
        Runtime::new(&crate::runtime_options::RuntimeOptions::new()).unwrap(),
    )))
    .unwrap();
    let table = DeltaTableBuilder::from_url(url::Url::parse("memory:///p2").unwrap())
        .unwrap()
        .build()
        .unwrap();
    let table = NonNull::new(Box::into_raw(Box::new(RawDeltaTable::new(table)))).unwrap();
    REENTRY.store(table.as_ptr() as usize, Ordering::Relaxed);
    WORK_RETIRED.store(false, Ordering::Release);
    let (sender, receiver) = mpsc::channel();
    *EMPTY_RESULT.lock().unwrap() = Some(sender);
    let (started, admitted) = mpsc::channel();
    let (gate, ready) = tokio::sync::oneshot::channel::<()>();
    let token = CancellationToken { token: tokio_util::sync::CancellationToken::new() };
    let callback: TableEmptyCallback = empty_result;
    run_async_with_cancellation!(
        runtime,
        table,
        cancelled.then_some(&token),
        rt,
        tbl,
        callback,
        {
            let _probe = DropProbe { table: tbl };
            let _ = rt.handle();
            started.send(()).unwrap();
            ready.await.unwrap();
            if fail {
                callback(DeltaTableError::new(rt, DeltaTableErrorCode::DataFusion, "drop probe").into_raw());
                return;
            }
            callback(std::ptr::null());
        },
        { callback(std::ptr::null()) }
    );
    admitted.recv_timeout(StdDuration::from_secs(10)).unwrap();
    assert!(!WORK_RETIRED.load(Ordering::Acquire));
    runtime_free(runtime.as_ptr());
    if cancelled {
        token.token.cancel();
    } else {
        gate.send(()).unwrap();
    }
    assert_eq!(receiver.recv_timeout(StdDuration::from_secs(10)).unwrap(), (true, !fail));
    *EMPTY_RESULT.lock().unwrap() = None;
}

#[test]
fn p2_queued_work_retires_before_success_and_error_callback_reentry_and_free() {
    let _serial = serial();
    check_work_retirement(false, false);
    check_work_retirement(false, true);
}

#[test]
fn p2_cancelled_work_retires_before_callback_reentry_and_free() {
    let _serial = serial();
    check_work_retirement(true, false);
}

#[tokio::test]
async fn p2_additive_ffi_create_copies_inputs_and_acquires_before_spawn() {
    let _serial = serial();
    let fixture = Fixture::new();
    let server = MockServer::start().await;
    Mock::given(any())
        .respond_with(BlobServer::default())
        .mount(&server)
        .await;
    let runtime = NonNull::new(Box::into_raw(Box::new(
        Runtime::new(&crate::runtime_options::RuntimeOptions::new()).unwrap(),
    )))
    .unwrap();
    let (sender, receiver) = mpsc::channel();
    *TABLE_RESULT.lock().unwrap() = Some(sender);
    {
        let uri = "az://container/owned-input".to_owned();
        let mode = "error".to_owned();
        let schema = Schema::new(vec![arrow::datatypes::Field::new(
            "value",
            arrow::datatypes::DataType::Int32,
            true,
        )]);
        let schema = arrow::ffi::FFI_ArrowSchema::try_from(&schema).unwrap();
        let mut options = TableCreatOptions {
            table_uri: byte_ref(&uri),
            schema: &schema as *const _ as *const c_void,
            partition_by: std::ptr::null(),
            partition_count: 0,
            mode: byte_ref(&mode),
            name: ByteArrayRef::null(),
            description: ByteArrayRef::null(),
            configuration: std::ptr::null_mut(),
            storage_options: options_map(storage_options(Some(&format!("{}/base", server.uri())))),
            custom_metadata: std::ptr::null_mut(),
        };
        create_deltalake_with_headers(
            runtime,
            NonNull::from(&mut options),
            None,
            fixture.context,
            table_result,
        );
    }
    bridge_headers_unregister(fixture.context);
    runtime_free(runtime.as_ptr());
    let (table, error) =
        tokio::task::spawn_blocking(move || receiver.recv_timeout(StdDuration::from_secs(15)))
            .await
            .unwrap()
            .unwrap();
    if error != 0 {
        let error = unsafe { Box::from_raw(error as *mut DeltaTableError) };
        panic!("FFI create failed: {}", error.message());
    }
    assert_ne!(table, 0);
    table_free(NonNull::new(table as *mut RawDeltaTable).unwrap());
    assert_eq!(fixture.state.released.load(Ordering::Relaxed), 1);
    *TABLE_RESULT.lock().unwrap() = None;
}
