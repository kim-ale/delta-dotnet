use super::*;

use std::cell::RefCell;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};

use delta_kernel::object_store::path::Path;
use delta_kernel::object_store::ObjectStoreExt;
use ffi::error::{EngineError, KernelError};
use wiremock::matchers::method;
use wiremock::{Mock, MockServer, ResponseTemplate};

static NEXT_CONTEXT: AtomicU64 = AtomicU64::new(0x3000_0000);
static STATES: OnceLock<Mutex<HashMap<u64, Arc<State>>>> = OnceLock::new();

thread_local! {
    static ERRORS: RefCell<Vec<(KernelError, String)>> = const { RefCell::new(Vec::new()) };
}

#[derive(Default)]
struct State {
    calls: AtomicUsize,
    releases: AtomicUsize,
    reject: bool,
    uris: Mutex<Vec<String>>,
}

struct Fixture {
    context: u64,
    state: Arc<State>,
}

impl Fixture {
    fn new(reject: bool) -> Self {
        let context = NEXT_CONTEXT.fetch_add(1, Ordering::Relaxed);
        let state = Arc::new(State {
            reject,
            ..Default::default()
        });
        STATES
            .get_or_init(Default::default)
            .lock()
            .unwrap()
            .insert(context, state.clone());
        let callbacks = HeaderCallbacks {
            abi_version: delta_http_headers::ABI_VERSION,
            struct_size: size_of::<HeaderCallbacks>() as u32,
            context_id: context,
            begin: Some(begin),
            cancel: Some(cancel),
            released: Some(released),
        };
        assert_eq!(unsafe { kernel_headers_register(&callbacks) }, 0);
        Self { context, state }
    }

    fn provider(&self) -> Arc<delta_http_headers::Provider> {
        delta_http_headers::acquire(self.context).unwrap()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        kernel_headers_unregister(self.context);
        STATES.get().unwrap().lock().unwrap().remove(&self.context);
    }
}

fn state(context: u64) -> Option<Arc<State>> {
    STATES
        .get()?
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(&context)
        .cloned()
}

unsafe extern "C" fn begin(context: u64, request: u64, _method: ByteSlice, uri: ByteSlice) -> u32 {
    let Some(state) = state(context) else {
        return 1;
    };
    let number = state.calls.fetch_add(1, Ordering::SeqCst) + 1;
    let uri = String::from_utf8_lossy(std::slice::from_raw_parts(uri.data, uri.len)).into_owned();
    state
        .uris
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .push(uri);
    if state.reject {
        return 1;
    }
    let payload = format!("{{\"x-provider\":\"{number}\"}}");
    kernel_headers_complete(
        context,
        request,
        0,
        ByteSlice {
            data: payload.as_ptr(),
            len: payload.len(),
        },
    );
    0
}

unsafe extern "C" fn cancel(_context: u64, _request: u64) {}

unsafe extern "C" fn released(context: u64) {
    if let Some(state) = state(context) {
        state.releases.fetch_add(1, Ordering::SeqCst);
    }
}

extern "C" fn allocate_error(kind: KernelError, message: KernelStringSlice) -> *mut EngineError {
    let mut budget = MAX_INPUT_BYTES;
    let message = unsafe { copy_string(&message, &mut budget) }.unwrap_or_default();
    ERRORS.with_borrow_mut(|errors| errors.push((kind, message)));
    std::ptr::null_mut()
}

fn options(values: &[(&str, &str)]) -> HashMap<String, String> {
    values
        .iter()
        .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
        .collect()
}

unsafe fn construct(
    path: &str,
    options: &[(&str, &str)],
    context: u64,
) -> ExternResult<Handle<SharedExternEngine>> {
    let keys: Vec<_> = options
        .iter()
        .map(|(key, _)| borrowed_string(key))
        .collect();
    let values: Vec<_> = options
        .iter()
        .map(|(_, value)| borrowed_string(value))
        .collect();
    kernel_engine_with_headers(
        borrowed_string(path),
        keys.as_ptr(),
        values.as_ptr(),
        keys.len(),
        context,
        allocate_error,
    )
}

fn assert_sanitized_failure(result: ExternResult<Handle<SharedExternEngine>>, expected: &str) {
    assert!(matches!(result, ExternResult::Err(pointer) if pointer.is_null()));
    ERRORS.with_borrow_mut(|errors| {
        let (kind, message) = errors.pop().unwrap();
        assert_eq!(kind, KernelError::GenericError);
        assert!(message.contains(expected), "{message}");
        assert!(!message.contains("caller-secret"));
    });
}

#[test]
fn engine_clone_and_upstream_free_retain_provider_until_last_owner() {
    let fixture = Fixture::new(false);
    let result = unsafe {
        construct(
            "az://container/table",
            &[
                ("azure_storage_account_name", "test"),
                ("azure_skip_signature", "true"),
            ],
            fixture.context,
        )
    };
    let ExternResult::Ok(engine) = result else {
        panic!("constructor failed");
    };
    let clone = unsafe { engine.clone_handle() };
    kernel_headers_unregister(fixture.context);
    unsafe { ffi::free_engine(engine) };
    assert_eq!(fixture.state.releases.load(Ordering::SeqCst), 0);
    unsafe { ffi::free_engine(clone) };
    assert_eq!(fixture.state.releases.load(Ordering::SeqCst), 1);
}

#[test]
fn unavailable_context_fails_without_provider_fallback() {
    assert_sanitized_failure(
        unsafe { construct("az://container/table", &[], 0) },
        "header context unavailable",
    );
}

#[test]
fn non_azure_provider_is_rejected_with_generic_error() {
    let fixture = Fixture::new(false);
    assert_sanitized_failure(
        unsafe { construct("s3://caller-secret/table", &[], fixture.context) },
        "storage provider unsupported",
    );
}

#[test]
fn invalid_storage_options_are_sanitized() {
    let fixture = Fixture::new(false);
    assert_sanitized_failure(
        unsafe {
            construct(
                "az://container/table",
                &[
                    ("azure_storage_account_name", "test"),
                    ("azure_storage_access_key", "caller-secret"),
                ],
                fixture.context,
            )
        },
        "storage configuration invalid",
    );
}

#[test]
fn bounded_input_rejects_oversized_slices_before_reading() {
    let path = unsafe {
        std::mem::transmute::<ByteSlice, KernelStringSlice>(ByteSlice {
            data: std::ptr::dangling::<u8>(),
            len: MAX_STRING_BYTES + 1,
        })
    };
    assert_sanitized_failure(
        unsafe {
            kernel_engine_with_headers(
                path,
                std::ptr::null(),
                std::ptr::null(),
                0,
                0,
                allocate_error,
            )
        },
        "invalid header engine input",
    );
}

#[test]
fn bounded_input_rejects_null_arrays_and_excessive_counts() {
    for count in [1, MAX_OPTIONS + 1] {
        assert_sanitized_failure(
            unsafe {
                kernel_engine_with_headers(
                    borrowed_string("az://container"),
                    std::ptr::null(),
                    std::ptr::null(),
                    count,
                    0,
                    allocate_error,
                )
            },
            "invalid header engine input",
        );
    }
}

#[test]
fn bounded_input_rejects_invalid_utf8_and_exhausted_budget() {
    let raw = [0xff];
    let slice = unsafe {
        std::mem::transmute::<ByteSlice, KernelStringSlice>(ByteSlice {
            data: raw.as_ptr(),
            len: raw.len(),
        })
    };
    let mut budget = MAX_INPUT_BYTES;
    assert!(unsafe { copy_string(&slice, &mut budget) }.is_err());
    assert!(unsafe { copy_string(&borrowed_string("too long"), &mut 1) }.is_err());
}

#[test]
fn broker_wrappers_keep_exact_invalid_and_late_completion_statuses() {
    assert_eq!(unsafe { kernel_headers_register(std::ptr::null()) }, 1);
    assert_eq!(
        unsafe {
            kernel_headers_complete(
                0,
                0,
                0,
                ByteSlice {
                    data: std::ptr::dangling::<u8>(),
                    len: usize::MAX,
                },
            )
        },
        1
    );
}

fn head_response() -> ResponseTemplate {
    ResponseTemplate::new(200)
        .insert_header("etag", "\"synthetic\"")
        .insert_header("last-modified", "Mon, 05 Oct 2026 00:00:00 GMT")
        .insert_header("content-length", "4")
}

#[tokio::test]
async fn custom_endpoint_keeps_native_auth_and_acquires_fresh_headers_after_unregister() {
    let fixture = Fixture::new(false);
    let server = MockServer::start().await;
    Mock::given(method("HEAD"))
        .respond_with(head_response())
        .mount(&server)
        .await;
    let endpoint = format!("{}/base", server.uri());
    let store = store::build_store(
        &"az://container/table".parse().unwrap(),
        options(&[
            ("AZURE_STORAGE_ACCOUNT_NAME", "test"),
            ("azure_storage_token", "synthetic-native-token"),
            ("azure_storage_endpoint", &endpoint),
            ("azure_allow_http", "true"),
            ("azure_use_fabric_endpoint", "invalid-but-unused"),
            ("unknown-option", "ignored"),
        ]),
        fixture.provider(),
    )
    .unwrap();
    kernel_headers_unregister(fixture.context);
    for _ in 0..2 {
        store.head(&Path::from("sample")).await.unwrap();
    }
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 2);
    for (index, request) in requests.iter().enumerate() {
        assert_eq!(request.url.path(), "/base/container/sample");
        assert_eq!(
            request.headers["authorization"],
            "Bearer synthetic-native-token"
        );
        assert_eq!(request.headers["x-provider"], (index + 1).to_string());
    }
    assert_eq!(fixture.state.calls.load(Ordering::SeqCst), 2);
    assert_eq!(fixture.state.releases.load(Ordering::SeqCst), 0);
    drop(store);
    assert_eq!(fixture.state.releases.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn acquisition_failure_sends_no_storage_request() {
    let fixture = Fixture::new(true);
    let server = MockServer::start().await;
    let store = store::build_store(
        &"az://container/table".parse().unwrap(),
        options(&[
            ("azure_storage_account_name", "test"),
            ("azure_skip_signature", "true"),
            ("azure_storage_endpoint", &server.uri()),
            ("azure_allow_http", "true"),
        ]),
        fixture.provider(),
    )
    .unwrap();
    assert!(store.head(&Path::from("sample")).await.is_err());
    assert_eq!(fixture.state.calls.load(Ordering::SeqCst), 1);
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[test]
fn stock_url_account_and_container_override_conflicting_options() {
    let fixture = Fixture::new(false);
    let store = store::build_store(
        &"abfss://container@urlaccount.dfs.core.windows.net/table"
            .parse()
            .unwrap(),
        options(&[
            ("azure_storage_account_name", "optionaccount"),
            ("azure_container_name", "optioncontainer"),
            ("azure_skip_signature", "true"),
        ]),
        fixture.provider(),
    )
    .unwrap();
    let stock = delta_kernel_default_engine::storage::store_from_url_opts(
        &"abfss://container@urlaccount.dfs.core.windows.net/table"
            .parse()
            .unwrap(),
        options(&[
            ("azure_storage_account_name", "optionaccount"),
            ("azure_container_name", "optioncontainer"),
            ("azure_skip_signature", "true"),
        ]),
    )
    .unwrap();
    assert_eq!(store.to_string(), stock.to_string());
}
