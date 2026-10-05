use std::collections::{HashMap, VecDeque};
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use delta_kernel::object_store::azure::AzureCredential;
use delta_kernel::object_store::path::Path;
use delta_kernel::object_store::ObjectStoreExt;
use kernel_ffi::error::{EngineError, ExternResult, KernelError};
use tokio::sync::{mpsc, Barrier, Notify};
use wiremock::matchers::{header, method};
use wiremock::{Mock, MockServer, ResponseTemplate};

use crate::abi::*;
use crate::broker::{self, lock, BrokerError, Registration};
use crate::*;

static CALLBACKS: OnceLock<Mutex<HashMap<u64, Arc<Callbacks>>>> = OnceLock::new();

struct Reply {
    bytes: Vec<u8>,
    status: u32,
    expiry: i64,
}

struct Callbacks {
    replies: Mutex<VecDeque<Reply>>,
    requests: mpsc::UnboundedSender<KernelCredentialRequestV1>,
    cancels: mpsc::UnboundedSender<(u64, u32)>,
    cancel_resume: Mutex<Option<std::sync::mpsc::Receiver<()>>>,
    releases: AtomicUsize,
    released: Notify,
    admission: AtomicUsize,
}

struct Fixture {
    context_id: u64,
    callbacks: Arc<Callbacks>,
    requests: mpsc::UnboundedReceiver<KernelCredentialRequestV1>,
    cancels: mpsc::UnboundedReceiver<(u64, u32)>,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        kernel_credential_unregister(self.context_id);
    }
}

impl Fixture {
    fn reserved() -> Self {
        let mut context_id = 0;
        assert_eq!(unsafe { kernel_credential_context_new(&mut context_id) }, 0);
        let (requests, request_receiver) = mpsc::unbounded_channel();
        let (cancels, cancel_receiver) = mpsc::unbounded_channel();
        let callbacks = Arc::new(Callbacks {
            replies: Mutex::new(VecDeque::new()),
            requests,
            cancels,
            cancel_resume: Mutex::new(None),
            releases: AtomicUsize::new(0),
            released: Notify::new(),
            admission: AtomicUsize::new(0),
        });
        lock(callbacks_map()).insert(context_id, callbacks.clone());
        Self {
            context_id,
            callbacks,
            requests: request_receiver,
            cancels: cancel_receiver,
        }
    }

    fn new() -> Self {
        let fixture = Self::reserved();
        assert_eq!(
            unsafe { kernel_credential_register(&fixture.descriptor()) },
            0
        );
        assert_eq!(kernel_credential_activate(fixture.context_id), 0);
        fixture
    }

    fn descriptor(&self) -> KernelCredentialRegistrationV1 {
        KernelCredentialRegistrationV1 {
            abi_version: 1,
            struct_size: 72,
            context_id: self.context_id,
            credential_kind: 1,
            acquisition_timeout_ms: DEFAULT_TIMEOUT_MS,
            max_token_bytes: MAX_TOKEN_BYTES,
            flags: 0,
            begin: Some(begin),
            cancel: Some(cancel),
            released: Some(released),
            reserved: [0; 2],
        }
    }

    fn registration(&self) -> Arc<Registration> {
        broker::attach(self.context_id).unwrap()
    }
    fn reply(&self, bytes: &[u8], status: u32, expiry: i64) {
        lock(&self.callbacks.replies).push_back(Reply {
            bytes: bytes.to_vec(),
            status,
            expiry,
        });
    }
    fn success(&self, bytes: &[u8]) {
        self.reply(bytes, 0, broker::unix_ms().unwrap() + 3_600_000);
    }
    async fn request(&mut self) -> KernelCredentialRequestV1 {
        tokio::time::timeout(Duration::from_secs(5), self.requests.recv())
            .await
            .unwrap()
            .unwrap()
    }
    async fn canceled(&mut self) -> (u64, u32) {
        tokio::time::timeout(Duration::from_secs(5), self.cancels.recv())
            .await
            .unwrap()
            .unwrap()
    }
    async fn wait_released(&self) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while self.callbacks.releases.load(Ordering::Acquire) == 0 {
                let notified = self.callbacks.released.notified();
                if self.callbacks.releases.load(Ordering::Acquire) != 0 {
                    break;
                }
                notified.await;
            }
        })
        .await
        .unwrap();
        assert_eq!(self.callbacks.releases.load(Ordering::Acquire), 1);
    }
}

fn callbacks_map() -> &'static Mutex<HashMap<u64, Arc<Callbacks>>> {
    CALLBACKS.get_or_init(|| Mutex::new(HashMap::new()))
}

unsafe extern "C" fn begin(request: *const KernelCredentialRequestV1) -> u32 {
    let request = unsafe { *request };
    let callbacks = lock(callbacks_map())
        .get(&request.context_id)
        .unwrap()
        .clone();
    assert_eq!(
        (
            request.abi_version,
            request.struct_size,
            request.minimum_lifetime_ms
        ),
        (1, 56, 90_000)
    );
    assert_eq!((request.flags, request.reserved), (0, [0; 2]));
    let _ = callbacks.requests.send(request);
    let reply = lock(&callbacks.replies).pop_front();
    if let Some(reply) = reply {
        complete_reply(request, &reply);
    }
    callbacks.admission.load(Ordering::Acquire) as u32
}

unsafe extern "C" fn cancel(context_id: u64, request_id: u64, reason: u32) {
    assert_eq!(
        unsafe { kernel_credential_complete(context_id, request_id, ptr::dangling()) },
        1
    );
    let callbacks = lock(callbacks_map()).get(&context_id).unwrap().clone();
    let resume = lock(&callbacks.cancel_resume).take();
    let _ = callbacks.cancels.send((request_id, reason));
    if let Some(resume) = resume {
        let _ = resume.recv();
    }
}

unsafe extern "C" fn released(context_id: u64) {
    assert_eq!(kernel_credential_activate(context_id), CLOSED);
    let callbacks = lock(callbacks_map()).remove(&context_id).unwrap();
    callbacks.releases.fetch_add(1, Ordering::AcqRel);
    callbacks.released.notify_one();
}

fn complete_reply(request: KernelCredentialRequestV1, reply: &Reply) -> u32 {
    let result = KernelCredentialResultV1 {
        abi_version: 1,
        struct_size: 56,
        status: reply.status,
        credential_kind: 1,
        token_utf8: if reply.bytes.is_empty() {
            ptr::null()
        } else {
            reply.bytes.as_ptr()
        },
        token_len: reply.bytes.len() as u64,
        expires_unix_ms: reply.expiry,
        reserved: [0; 2],
    };
    unsafe { kernel_credential_complete(request.context_id, request.request_id, &result) }
}

fn complete_success(request: KernelCredentialRequestV1, token: &[u8]) -> u32 {
    complete_reply(
        request,
        &Reply {
            bytes: token.to_vec(),
            status: 0,
            expiry: broker::unix_ms().unwrap() + 3_600_000,
        },
    )
}

fn assert_token(cached: &broker::CachedToken, expected: &str) {
    match cached.credential.as_ref() {
        AzureCredential::BearerToken(token) => assert_eq!(token, expected),
        _ => panic!("only bearer credentials are supported"),
    }
}

async fn interests(registration: &Registration, count: usize) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while registration.waiter_count() != count {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[test]
fn failed_register_never_releases_and_dormant_registration_needs_activation() {
    let fixture = Fixture::reserved();
    let mut descriptor = fixture.descriptor();
    descriptor.flags = 1;
    assert_eq!(
        unsafe { kernel_credential_register(&descriptor) },
        INVALID_ARGUMENT
    );
    assert_eq!(fixture.callbacks.releases.load(Ordering::Acquire), 0);
    descriptor.flags = 0;
    assert_eq!(unsafe { kernel_credential_register(&descriptor) }, 0);
    assert!(broker::attach(fixture.context_id).is_err());
    assert_eq!(
        unsafe { kernel_credential_register(&descriptor) },
        DUPLICATE_ID
    );
    assert_eq!(kernel_credential_unregister(fixture.context_id), 0);
    assert_eq!(fixture.callbacks.releases.load(Ordering::Acquire), 1);
    assert_eq!(kernel_credential_activate(fixture.context_id), CLOSED);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn synchronous_completion_is_copied_before_begin_returns_and_cached() {
    let mut fixture = Fixture::new();
    fixture.success(b"A.synthetic==");
    let registration = fixture.registration();
    assert_eq!(
        format!("{:?}", broker::Provider(registration.clone())),
        "KernelAzureBearerProvider(<redacted>)"
    );
    let token = registration.token().await.unwrap();
    assert_token(&token, "A.synthetic==");
    let request = fixture.request().await;
    assert!(Arc::ptr_eq(&token, &registration.token().await.unwrap()));
    assert!(fixture.requests.try_recv().is_err());
    assert_eq!(
        unsafe {
            kernel_credential_complete(fixture.context_id, request.request_id, ptr::dangling())
        },
        1
    );
    drop(token);
    kernel_credential_unregister(fixture.context_id);
    drop(registration);
    fixture.wait_released().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_waiters_share_one_real_callback_flight() {
    let mut fixture = Fixture::new();
    let registration = fixture.registration();
    let barrier = Arc::new(Barrier::new(33));
    let mut tasks = Vec::new();
    for _index in 0..32 {
        let registration = registration.clone();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            registration.token().await
        }));
    }
    barrier.wait().await;
    let request = fixture.request().await;
    interests(&registration, 32).await;
    assert!(fixture.requests.try_recv().is_err());
    assert_eq!(complete_success(request, b"A"), 0);
    for task in tasks {
        assert_token(&task.await.unwrap().unwrap(), "A");
    }
    assert!(fixture.cancels.try_recv().is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_completions_have_exactly_one_terminal_winner() {
    let mut fixture = Fixture::new();
    let registration = fixture.registration();
    let waiter_registration = registration.clone();
    let waiter = tokio::spawn(async move { waiter_registration.token().await });
    let request = fixture.request().await;
    let barrier = Arc::new(std::sync::Barrier::new(3));
    let mut completers = Vec::new();
    for _index in 0..2 {
        let barrier = barrier.clone();
        completers.push(std::thread::spawn(move || {
            barrier.wait();
            complete_success(request, b"A")
        }));
    }
    barrier.wait();
    let mut statuses: Vec<_> = completers
        .into_iter()
        .map(|thread| thread.join().unwrap())
        .collect();
    statuses.sort();
    assert_eq!(statuses, [0, 1]);
    assert_token(&waiter.await.unwrap().unwrap(), "A");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn canceling_one_waiter_does_not_cancel_another() {
    let mut fixture = Fixture::new();
    let registration = fixture.registration();
    let first_registration = registration.clone();
    let first = tokio::spawn(async move { first_registration.token().await });
    let request = fixture.request().await;
    let second_registration = registration.clone();
    let second = tokio::spawn(async move { second_registration.token().await });
    interests(&registration, 2).await;
    first.abort();
    assert!(first.await.is_err());
    interests(&registration, 1).await;
    assert!(fixture.cancels.try_recv().is_err());
    assert_eq!(complete_success(request, b"A"), 0);
    assert_token(&second.await.unwrap().unwrap(), "A");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_last_waiter_cancels_and_late_unknown_pointers_are_not_read() {
    let mut fixture = Fixture::new();
    let registration = fixture.registration();
    let waiter_registration = registration.clone();
    let waiter = tokio::spawn(async move { waiter_registration.token().await });
    let request = fixture.request().await;
    waiter.abort();
    let _ = waiter.await;
    assert_eq!(fixture.canceled().await, (request.request_id, 1));
    assert_eq!(
        unsafe {
            kernel_credential_complete(fixture.context_id, request.request_id, ptr::dangling())
        },
        1
    );
    assert_eq!(
        unsafe { kernel_credential_complete(u64::MAX, u64::MAX, ptr::dangling()) },
        1
    );
    kernel_credential_unregister(fixture.context_id);
    drop(registration);
    fixture.wait_released().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(non_snake_case)]
async fn dropFirstWaiter_NewCallerDuringRetirement_Succeeds() {
    let mut fixture = Fixture::new();
    let registration = fixture.registration();
    let (resume, resume_receiver) = std::sync::mpsc::channel();
    *lock(&fixture.callbacks.cancel_resume) = Some(resume_receiver);
    let first_registration = registration.clone();
    let first = tokio::spawn(async move { first_registration.token().await });
    let first_request = fixture.request().await;
    first.abort();
    assert!(first.await.err().unwrap().is_cancelled());
    assert_eq!(fixture.canceled().await, (first_request.request_id, 1));

    fixture.success(b"B");
    let mut next = Box::pin(registration.token());
    assert!(futures::poll!(next.as_mut()).is_pending());
    assert!(fixture.requests.try_recv().is_err());
    resume.send(()).unwrap();
    let token = tokio::time::timeout(Duration::from_secs(5), next)
        .await
        .unwrap()
        .unwrap();
    assert_token(&token, "B");
    assert_ne!(fixture.request().await.request_id, first_request.request_id);
    assert!(fixture.cancels.try_recv().is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deadline_retires_pending_and_cancels_only_an_accepted_begin() {
    let mut fixture = Fixture::reserved();
    let mut descriptor = fixture.descriptor();
    descriptor.acquisition_timeout_ms = 1_000;
    assert_eq!(unsafe { kernel_credential_register(&descriptor) }, 0);
    kernel_credential_activate(fixture.context_id);
    let registration = fixture.registration();
    let waiter_registration = registration.clone();
    let waiter = tokio::spawn(async move { waiter_registration.token().await });
    let request = fixture.request().await;
    assert_eq!(waiter.await.unwrap().err(), Some(BrokerError::TimedOut));
    assert_eq!(fixture.canceled().await, (request.request_id, 2));
    assert_eq!(complete_success(request, b"late"), 1);
    fixture.callbacks.admission.store(2, Ordering::Release);
    assert_eq!(
        registration.token().await.err(),
        Some(BrokerError::Canceled)
    );
    assert!(fixture.cancels.try_recv().is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unregister_keeps_pending_completion_and_future_consumer_requests_alive() {
    let mut fixture = Fixture::new();
    let registration = fixture.registration();
    let waiter_registration = registration.clone();
    let waiter = tokio::spawn(async move { waiter_registration.token().await });
    let request = fixture.request().await;
    assert_eq!(kernel_credential_unregister(fixture.context_id), 0);
    assert!(broker::attach(fixture.context_id).is_err());
    assert_eq!(fixture.callbacks.releases.load(Ordering::Acquire), 0);
    assert_eq!(complete_success(request, b"A"), 0);
    assert_token(&waiter.await.unwrap().unwrap(), "A");
    registration.invalidate_cache();
    fixture.success(b"B");
    assert_token(&registration.token().await.unwrap(), "B");
    drop(registration);
    fixture.wait_released().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_tokens_caps_expiry_and_failure_payloads_terminate_without_cache() {
    let fixture = Fixture::reserved();
    let mut descriptor = fixture.descriptor();
    descriptor.max_token_bytes = 4;
    assert_eq!(unsafe { kernel_credential_register(&descriptor) }, 0);
    kernel_credential_activate(fixture.context_id);
    let registration = fixture.registration();
    for bytes in [b"=".as_slice(), b"a=b", b"\xff", b"A\r\n", b"AAAAA"] {
        fixture.success(bytes);
        assert_eq!(
            registration.token().await.err(),
            Some(BrokerError::InvalidResult)
        );
    }
    fixture.reply(b"A", 0, broker::unix_ms().unwrap() + 90_000);
    assert_eq!(
        registration.token().await.err(),
        Some(BrokerError::InvalidResult)
    );
    fixture.reply(b"A", 1, 0);
    assert_eq!(
        registration.token().await.err(),
        Some(BrokerError::InvalidResult)
    );
    fixture.reply(b"", 1, 0);
    assert_eq!(
        registration.token().await.err(),
        Some(BrokerError::Permanent)
    );
    fixture.success(b"A==");
    assert_token(&registration.token().await.unwrap(), "A==");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn header_validation_precedes_full_descriptor_read_and_copy() {
    let mut fixture = Fixture::new();
    let registration = fixture.registration();
    let waiter_registration = registration.clone();
    let waiter = tokio::spawn(async move { waiter_registration.token().await });
    let request = fixture.request().await;
    let short_header = Header {
        abi_version: 1,
        struct_size: 8,
    };
    assert_eq!(
        unsafe {
            kernel_credential_complete(
                fixture.context_id,
                request.request_id,
                (&short_header as *const Header).cast(),
            )
        },
        2
    );
    assert_eq!(
        waiter.await.unwrap().err(),
        Some(BrokerError::InvalidResult)
    );
    assert_eq!(
        unsafe { kernel_credential_register((&short_header as *const Header).cast()) },
        INVALID_ARGUMENT
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transient_retries_are_bounded_to_two_within_original_budget() {
    let mut fixture = Fixture::new();
    fixture.reply(b"", 2, 0);
    fixture.reply(b"", 2, 0);
    let registration = fixture.registration();
    assert_eq!(
        registration.token().await.err(),
        Some(BrokerError::Transient)
    );
    let first = fixture.request().await;
    let second = fixture.request().await;
    assert_ne!(first.request_id, second.request_id);
    assert!(second.timeout_ms < first.timeout_ms);
    assert_eq!(
        registration.token().await.err(),
        Some(BrokerError::Transient)
    );
    assert!(fixture.requests.try_recv().is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transient_refresh_can_use_still_valid_cache_but_backoff_does_not_create_fallback() {
    let fixture = Fixture::new();
    fixture.success(b"A");
    let registration = fixture.registration();
    assert_token(&registration.token().await.unwrap(), "A");
    registration.force_refresh();
    fixture.reply(b"", 2, 0);
    fixture.reply(b"", 2, 0);
    assert_token(&registration.token().await.unwrap(), "A");
    registration.invalidate_cache();
    assert_eq!(
        registration.token().await.err(),
        Some(BrokerError::Transient)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn permanent_refresh_failure_discards_a_still_usable_cache() {
    let fixture = Fixture::new();
    fixture.success(b"A");
    let registration = fixture.registration();
    assert_token(&registration.token().await.unwrap(), "A");
    registration.force_refresh();
    fixture.reply(b"", 1, 0);
    assert_eq!(
        registration.token().await.err(),
        Some(BrokerError::Permanent)
    );
    fixture.success(b"B");
    assert_token(&registration.token().await.unwrap(), "B");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_azure_requests_roll_authorization_from_a_to_b_without_rebuilding_store() {
    let fixture = Fixture::new();
    let registration = fixture.registration();
    let server = MockServer::start().await;
    for token in ["A", "B"] {
        Mock::given(method("HEAD"))
            .and(header("authorization", format!("Bearer {token}")))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("etag", "\"synthetic\"")
                    .insert_header("last-modified", "Fri, 02 Oct 2026 00:00:00 GMT")
                    .insert_header("content-length", "0"),
            )
            .expect(1)
            .mount(&server)
            .await;
    }
    let store = crate::engine::build_store(
        "az://container/table",
        vec![
            ("ACCOUNT_NAME".into(), "syntheticaccount".into()),
            ("endpoint".into(), server.uri()),
            ("allow_http".into(), "true".into()),
            ("use_emulator".into(), "FALSE".into()),
            ("skip_signature".into(), "False".into()),
            ("use_azure_cli".into(), "fAlSe".into()),
        ],
        registration.clone(),
    )
    .unwrap();
    fixture.success(b"A");
    store.head(&Path::from("table/blob")).await.unwrap();
    registration.invalidate_cache();
    assert_eq!(kernel_credential_unregister(fixture.context_id), 0);
    fixture.success(b"B");
    store.head(&Path::from("table/blob")).await.unwrap();
    drop(registration);
    assert_eq!(fixture.callbacks.releases.load(Ordering::Acquire), 0);
    drop(store);
    fixture.wait_released().await;
    server.verify().await;
}

#[repr(C)]
struct TestError {
    kind: KernelError,
    message: String,
}

#[repr(C)]
struct ErrorMessageSlice {
    pointer: *const u8,
    count: usize,
}

extern "C" fn allocate_error(
    kind: KernelError,
    message: kernel_ffi::KernelStringSlice,
) -> *mut EngineError {
    let message: ErrorMessageSlice = unsafe { std::mem::transmute(message) };
    let text = unsafe { std::slice::from_raw_parts(message.pointer, message.count) };
    Box::into_raw(Box::new(TestError {
        kind,
        message: std::str::from_utf8(text).unwrap().to_owned(),
    }))
    .cast()
}

#[test]
fn constructor_failures_use_sanitized_stock_errors_and_retire_consumer_leases() {
    let fixture = Fixture::new();
    for (uri, key, value) in [
        ("file:///table", "account_name", "syntheticaccount"),
        (
            "az://container/table",
            "token",
            "synthetic-secret-must-not-leak",
        ),
        ("az://container/table", "skip_signature", "true"),
        ("az://container/table", "unrecognized", "ignored"),
    ] {
        let option = KernelCredentialOptionV1 {
            key_utf8: key.as_ptr(),
            key_len: key.len() as u64,
            value_utf8: value.as_ptr(),
            value_len: value.len() as u64,
        };
        let error = unsafe {
            match kernel_engine_new_with_credential_v1(
                crate::engine::string_slice(uri),
                &option,
                1,
                fixture.context_id,
                allocate_error,
                0,
                0,
            ) {
                ExternResult::Err(error) => Box::from_raw(error.cast::<TestError>()),
                ExternResult::Ok(handle) => {
                    kernel_ffi::free_engine(handle);
                    panic!("invalid configuration must fail");
                }
            }
        };
        assert_eq!(error.kind, KernelError::GenericError);
        assert!(error.message.starts_with("kernel credential host:"));
        assert!(!error.message.contains("synthetic-secret"));
        assert_eq!(fixture.callbacks.releases.load(Ordering::Acquire), 0);
    }
    assert!(fixture.callbacks.replies.lock().unwrap().is_empty());
    assert_eq!(kernel_credential_unregister(fixture.context_id), 0);
    assert_eq!(fixture.callbacks.releases.load(Ordering::Acquire), 1);
}

#[test]
fn provider_constructor_returns_real_stock_handles_and_clones_retain_registration() {
    let fixture = Fixture::new();
    let account = "account_name";
    let value = "syntheticaccount";
    let option = KernelCredentialOptionV1 {
        key_utf8: account.as_ptr(),
        key_len: account.len() as u64,
        value_utf8: value.as_ptr(),
        value_len: value.len() as u64,
    };
    let handle = unsafe {
        match kernel_engine_new_with_credential_v1(
            crate::engine::string_slice("az://container/table"),
            &option,
            1,
            fixture.context_id,
            allocate_error,
            0,
            0,
        ) {
            ExternResult::Ok(handle) => handle,
            ExternResult::Err(_) => panic!("provider constructor should succeed"),
        }
    };
    unsafe {
        let clone = handle.clone_as_arc();
        let engine = clone.engine();
        let _handler = engine.evaluation_handler();
        kernel_ffi::free_engine(handle);
        kernel_credential_unregister(fixture.context_id);
        assert_eq!(fixture.callbacks.releases.load(Ordering::Acquire), 0);
        drop(clone);
        assert_eq!(fixture.callbacks.releases.load(Ordering::Acquire), 0);
        drop(engine);
    }
    assert_eq!(fixture.callbacks.releases.load(Ordering::Acquire), 1);
}

#[test]
fn stock_no_provider_builder_constructs_uses_and_frees_in_same_image() {
    let directory = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../../tests/data/simple_table")
        .canonicalize()
        .unwrap();
    let table_uri = url::Url::from_directory_path(directory)
        .unwrap()
        .to_string();
    unsafe {
        let builder = match kernel_ffi::get_engine_builder(
            crate::engine::string_slice(&table_uri),
            allocate_error,
        ) {
            ExternResult::Ok(builder) => builder,
            ExternResult::Err(_) => panic!("stock builder should succeed"),
        };
        kernel_ffi::set_builder_with_multithreaded_executor(&mut *builder, 2, 0);
        let handle = match kernel_ffi::builder_build(builder) {
            ExternResult::Ok(handle) => handle,
            ExternResult::Err(_) => panic!("stock engine should succeed"),
        };
        let clone = handle.clone_as_arc();
        let _handler = clone.engine().evaluation_handler();
        let snapshot_builder = match kernel_ffi::get_snapshot_builder(
            crate::engine::string_slice(&table_uri),
            ptr::read(&handle),
        ) {
            ExternResult::Ok(builder) => builder,
            ExternResult::Err(_) => panic!("stock snapshot builder should succeed"),
        };
        let snapshot = match kernel_ffi::snapshot_builder_build(snapshot_builder) {
            ExternResult::Ok(snapshot) => snapshot,
            ExternResult::Err(_) => panic!("stock snapshot should read the actual fixture"),
        };
        assert_eq!(kernel_ffi::version(ptr::read(&snapshot)), 4);
        kernel_ffi::free_snapshot(snapshot);
        kernel_ffi::free_engine(handle);
        drop(clone);
    }
}
