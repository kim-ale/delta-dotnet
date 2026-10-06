use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, Weak};
use std::time::Duration;

use http::HeaderMap;
use object_store::client::{HttpError, HttpRequest};
use tokio::sync::oneshot;

use crate::headers;
use crate::protocol::{
    provider_error, ByteSlice, HeaderCallbacks, HeaderError, ABI_VERSION, MAX_BYTES,
};

const MAX_CONTEXTS: usize = 256;
const MAX_PENDING: usize = 1024;
const MAX_CONTEXT_PENDING: usize = 64;
const TIMEOUT: Duration = Duration::from_secs(30);
static NEXT_REQUEST: AtomicU64 = AtomicU64::new(1);
static REGISTRY: OnceLock<Mutex<Registry>> = OnceLock::new();

type HeaderResult = Result<HeaderMap, HttpError>;
type Begin = unsafe extern "C" fn(u64, u64, ByteSlice, ByteSlice) -> u32;
type Cancel = unsafe extern "C" fn(u64, u64);
type Released = unsafe extern "C" fn(u64);

#[derive(Default)]
struct Registry {
    registered: HashMap<u64, Arc<Provider>>,
    live: HashMap<u64, Weak<Provider>>,
    pending: HashMap<(u64, u64), Flight>,
    counts: HashMap<u64, usize>,
}

/// Native callback owner. Store and service Arcs, not management registration alone,
/// determine its lifetime. Debug deliberately omits callback and request data.
pub struct Provider {
    context_id: u64,
    begin: Begin,
    cancel: Cancel,
    released: Released,
    timeout: Duration,
}

struct Flight {
    provider: Arc<Provider>,
    sender: oneshot::Sender<HeaderResult>,
}

struct PendingGuard {
    provider: Arc<Provider>,
    request_id: u64,
    accepted: bool,
}

impl fmt::Debug for Provider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Provider { callbacks: <redacted> }")
    }
}

impl Drop for Provider {
    fn drop(&mut self) {
        // SAFETY: Successful registration requires process-lifetime, non-unwinding callbacks.
        unsafe { (self.released)(self.context_id) };
        {
            let mut registry = lock_registry();
            registry.live.remove(&self.context_id);
        }
    }
}

impl Drop for PendingGuard {
    fn drop(&mut self) {
        let flight = take_flight(self.provider.context_id, self.request_id);
        if flight.is_some() && self.accepted {
            // SAFETY: The guard and flight retain the registered callback owner during cancellation.
            unsafe { (self.provider.cancel)(self.provider.context_id, self.request_id) };
        }
    }
}

impl Provider {
    /// Acquires fresh headers for this original HTTP send, with no caching or singleflight.
    /// Borrowed method/URI strings are valid during begin only. The host must run a
    /// Tokio runtime with time enabled; callbacks must queue work without blocking.
    ///
    /// # Errors
    /// Returns a sanitized, non-retryable Unknown HTTP error on any acquisition failure.
    pub async fn headers(self: &Arc<Self>, request: &HttpRequest) -> HeaderResult {
        let uri = request.uri().to_string();
        if uri.len() > MAX_BYTES {
            return Err(provider_error());
        }
        let request_id = NEXT_REQUEST
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                value.checked_add(1)
            })
            .map_err(|_| provider_error())?;
        let deadline = tokio::time::Instant::now() + self.timeout;
        let (sender, receiver) = oneshot::channel();
        {
            let mut registry = lock_registry();
            let count = registry.counts.get(&self.context_id).copied().unwrap_or(0);
            if registry.pending.len() >= MAX_PENDING || count >= MAX_CONTEXT_PENDING {
                return Err(provider_error());
            }
            registry.pending.insert(
                (self.context_id, request_id),
                Flight {
                    provider: self.clone(),
                    sender,
                },
            );
            registry.counts.insert(self.context_id, count + 1);
        }
        let mut guard = PendingGuard {
            provider: self.clone(),
            request_id,
            accepted: false,
        };
        // SAFETY: Registration establishes callback validity; these counted strings live through begin.
        let status = unsafe {
            (self.begin)(
                self.context_id,
                request_id,
                ByteSlice::borrowed(request.method().as_str().as_bytes()),
                ByteSlice::borrowed(uri.as_bytes()),
            )
        };
        if status != 0 {
            return Err(provider_error());
        }
        guard.accepted = true;
        let result = tokio::time::timeout_at(deadline, receiver)
            .await
            .map_err(|_| provider_error())?
            .map_err(|_| provider_error())?;
        drop(guard);
        result
    }
}

fn lock_registry() -> MutexGuard<'static, Registry> {
    REGISTRY
        .get_or_init(|| Mutex::new(Registry::default()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn take_flight(context_id: u64, request_id: u64) -> Option<Flight> {
    let mut registry = lock_registry();
    let flight = registry.pending.remove(&(context_id, request_id))?;
    if let Some(count) = registry.counts.get_mut(&context_id) {
        *count -= 1;
        if *count == 0 {
            registry.counts.remove(&context_id);
        }
    }
    Some(flight)
}

/// Validates and synchronously copies the descriptor before publishing a management Arc.
/// Returns 0 accepted, 1 invalid, 2 duplicate live context, or 3 capacity exhausted.
/// A failed registration never calls released and never transfers ownership.
///
/// # Safety
/// All callbacks must be valid, non-unwinding, promptly returning, process-lifetime
/// trampolines. The managed context must survive through released and any managed
/// workers that outlive native cancellation. Begin must copy its borrowed strings.
pub unsafe fn register(callbacks: &HeaderCallbacks) -> u32 {
    register_inner(callbacks, TIMEOUT)
}

fn register_inner(callbacks: &HeaderCallbacks, timeout: Duration) -> u32 {
    if callbacks.abi_version != ABI_VERSION
        || callbacks.struct_size as usize != std::mem::size_of::<HeaderCallbacks>()
        || callbacks.context_id == 0
        || timeout.is_zero()
        || timeout > TIMEOUT
    {
        return 1;
    }
    let (Some(begin), Some(cancel), Some(released)) =
        (callbacks.begin, callbacks.cancel, callbacks.released)
    else {
        return 1;
    };
    let mut registry = lock_registry();
    if registry.live.contains_key(&callbacks.context_id) {
        return 2;
    }
    if registry.live.len() >= MAX_CONTEXTS {
        return 3;
    }
    let provider = Arc::new(Provider {
        context_id: callbacks.context_id,
        begin,
        cancel,
        released,
        timeout,
    });
    registry
        .live
        .insert(callbacks.context_id, Arc::downgrade(&provider));
    registry.registered.insert(callbacks.context_id, provider);
    0
}

/// Acquires a store/request owner from management registration.
///
/// # Errors
/// Returns ContextUnavailable after unregister or for an unknown identifier.
pub fn acquire(context_id: u64) -> Result<Arc<Provider>, HeaderError> {
    lock_registry()
        .registered
        .get(&context_id)
        .cloned()
        .ok_or(HeaderError::ContextUnavailable)
}

/// Removes only the management Arc. Existing stores and flights remain usable.
/// Released runs outside the registry lock when the final native Arc retires.
pub fn unregister(context_id: u64) {
    let provider = { lock_registry().registered.remove(&context_id) };
    drop(provider);
}

/// Claims a pending request before inspecting its payload. Returns 0 claimed or
/// 1 ignored. Status zero is success; every other status is a sanitized failure.
/// Malformed known completions terminate the flight. Unknown/late requests never
/// read bytes. This map and entrypoint do not require a Tokio runtime.
///
/// # Safety
/// For a known, successful request, a nonnull pointer with a length in 1..=65536
/// must address readable bytes for the call duration. Data is copied/parsed before
/// returning. Failure/ignored completions need not supply readable bytes.
pub unsafe fn complete(context_id: u64, request_id: u64, status: u32, bytes: ByteSlice) -> u32 {
    let Some(flight) = take_flight(context_id, request_id) else {
        return 1;
    };
    let provider = flight.provider.clone();
    let result = if status != 0 || bytes.data.is_null() || bytes.len == 0 || bytes.len > MAX_BYTES {
        Err(provider_error())
    } else {
        // SAFETY: The known successful completion contract guarantees readable bounded bytes.
        let bytes = unsafe { std::slice::from_raw_parts(bytes.data, bytes.len) };
        headers::parse(bytes).map_err(|_| provider_error())
    };
    let _ = flight.sender.send(result);
    drop(provider);
    0
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use object_store::client::HttpRequestBody;
    use std::future::Future;
    use std::sync::atomic::AtomicUsize;

    static NEXT_CONTEXT: AtomicU64 = AtomicU64::new(1);
    static TEST_STATES: OnceLock<Mutex<HashMap<u64, Arc<TestState>>>> = OnceLock::new();
    static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    pub(crate) async fn serial() -> tokio::sync::MutexGuard<'static, ()> {
        TEST_LOCK.lock().await
    }

    fn serial_sync() -> tokio::sync::MutexGuard<'static, ()> {
        TEST_LOCK.blocking_lock()
    }

    #[derive(Default)]
    pub(crate) struct TestState {
        pub(crate) calls: AtomicUsize,
        pub(crate) cancels: AtomicUsize,
        pub(crate) releases: AtomicUsize,
        pub(crate) requests: Mutex<Vec<(u64, String, String)>>,
        pub(crate) payload: Mutex<Option<Vec<u8>>>,
        pub(crate) reject: bool,
    }

    fn states() -> MutexGuard<'static, HashMap<u64, Arc<TestState>>> {
        TEST_STATES
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
            .unwrap()
    }

    unsafe extern "C" fn begin(
        context: u64,
        request: u64,
        method: ByteSlice,
        uri: ByteSlice,
    ) -> u32 {
        {
            let registry = lock_registry();
            assert!(registry.live.contains_key(&context));
        }
        let state = states().get(&context).unwrap().clone();
        // SAFETY: The broker lends valid strings for this call; the test copies them immediately.
        let method = unsafe { std::slice::from_raw_parts(method.data, method.len) };
        // SAFETY: The broker lends valid strings for this call; the test copies them immediately.
        let uri = unsafe { std::slice::from_raw_parts(uri.data, uri.len) };
        state.calls.fetch_add(1, Ordering::SeqCst);
        state.requests.lock().unwrap().push((
            request,
            String::from_utf8(method.to_vec()).unwrap(),
            String::from_utf8(uri.to_vec()).unwrap(),
        ));
        let payload = state.payload.lock().unwrap().clone();
        if let Some(payload) = payload {
            // SAFETY: The test owns the payload through the synchronous completion call.
            assert_eq!(
                unsafe { complete(context, request, 0, ByteSlice::borrowed(&payload)) },
                0
            );
        }
        u32::from(state.reject)
    }

    unsafe extern "C" fn cancel(context: u64, request: u64) {
        assert!(!lock_registry().pending.contains_key(&(context, request)));
        let state = states().get(&context).unwrap().clone();
        state.cancels.fetch_add(1, Ordering::SeqCst);
        // SAFETY: Late completions must ignore even invalid pointers without dereferencing them.
        assert_eq!(unsafe { complete(context, request, 0, invalid_bytes()) }, 1);
    }

    unsafe extern "C" fn released(context: u64) {
        assert!(lock_registry()
            .live
            .get(&context)
            .unwrap()
            .upgrade()
            .is_none());
        assert!(acquire(context).is_err());
        assert_eq!(register_inner(&descriptor(context), TIMEOUT), 2);
        states()
            .get(&context)
            .unwrap()
            .releases
            .fetch_add(1, Ordering::SeqCst);
    }

    pub(crate) struct Fixture {
        pub(crate) id: u64,
        pub(crate) provider: Option<Arc<Provider>>,
        pub(crate) state: Arc<TestState>,
    }

    impl Fixture {
        pub(crate) fn new(payload: Option<Vec<u8>>) -> Self {
            Self::with_state(
                TestState {
                    payload: Mutex::new(payload),
                    ..Default::default()
                },
                TIMEOUT,
            )
        }

        fn with_state(state: TestState, timeout: Duration) -> Self {
            let id = NEXT_CONTEXT.fetch_add(1, Ordering::SeqCst);
            let state = Arc::new(state);
            states().insert(id, state.clone());
            assert_eq!(register_inner(&descriptor(id), timeout), 0);
            Self {
                id,
                provider: Some(acquire(id).unwrap()),
                state,
            }
        }

        pub(crate) fn provider(&self) -> Arc<Provider> {
            self.provider.as_ref().unwrap().clone()
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            unregister(self.id);
            self.provider.take();
            assert_eq!(self.state.releases.load(Ordering::SeqCst), 1);
            states().remove(&self.id);
        }
    }

    fn descriptor(context_id: u64) -> HeaderCallbacks {
        HeaderCallbacks {
            abi_version: ABI_VERSION,
            struct_size: std::mem::size_of::<HeaderCallbacks>() as u32,
            context_id,
            begin: Some(begin),
            cancel: Some(cancel),
            released: Some(released),
        }
    }

    pub(crate) fn request(uri: &str) -> HttpRequest {
        http::Request::builder()
            .method("GET")
            .uri(uri)
            .body(HttpRequestBody::empty())
            .unwrap()
    }

    fn invalid_bytes() -> ByteSlice {
        ByteSlice {
            data: std::ptr::dangling(),
            len: usize::MAX,
        }
    }

    #[tokio::test]
    async fn immediate_completion_is_reentrant_and_duplicate_is_ignored() {
        let _serial = serial().await;
        let fixture = Fixture::new(Some(br#"{"x-custom":"secret"}"#.to_vec()));
        let request = request("https://storage.example/container/blob?sig=private");
        let headers = fixture.provider().headers(&request).await.unwrap();
        assert!(headers["x-custom"].is_sensitive());
        let records = fixture.state.requests.lock().unwrap();
        assert_eq!(records[0].1, "GET");
        assert_eq!(records[0].2, request.uri().to_string());
        // SAFETY: Duplicate requests must ignore the payload without reading it.
        assert_eq!(
            unsafe { complete(fixture.id, records[0].0, 0, invalid_bytes()) },
            1
        );
        assert_eq!(fixture.state.cancels.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn invalid_and_duplicate_registration_never_release() {
        let _serial = serial_sync();
        let fixture = Fixture::new(None);
        let mut callbacks = descriptor(fixture.id);
        assert_eq!(register_inner(&callbacks, TIMEOUT), 2);
        callbacks.context_id = 0;
        assert_eq!(register_inner(&callbacks, TIMEOUT), 1);
        callbacks.context_id = fixture.id;
        callbacks.abi_version += 1;
        assert_eq!(register_inner(&callbacks, TIMEOUT), 1);
        callbacks = descriptor(fixture.id);
        callbacks.struct_size -= 1;
        assert_eq!(register_inner(&callbacks, TIMEOUT), 1);
        callbacks = descriptor(fixture.id);
        callbacks.begin = None;
        assert_eq!(register_inner(&callbacks, TIMEOUT), 1);
        callbacks = descriptor(fixture.id);
        callbacks.cancel = None;
        assert_eq!(register_inner(&callbacks, TIMEOUT), 1);
        callbacks = descriptor(fixture.id);
        callbacks.released = None;
        assert_eq!(register_inner(&callbacks, TIMEOUT), 1);
        assert_eq!(fixture.state.releases.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn dropped_future_cancels_only_accepted_pending_work() {
        let _serial = serial().await;
        let fixture = Fixture::new(None);
        let provider = fixture.provider();
        let request = request("https://storage.example/container/blob");
        let mut future = Box::pin(provider.headers(&request));
        assert!(std::future::poll_fn(|context| std::task::Poll::Ready(
            future.as_mut().poll(context)
        ))
        .await
        .is_pending());
        drop(future);
        assert_eq!(fixture.state.cancels.load(Ordering::SeqCst), 1);
        assert!(!lock_registry().counts.contains_key(&fixture.id));
    }

    #[tokio::test]
    async fn rejected_begin_never_cancels_even_after_synchronous_completion() {
        let _serial = serial().await;
        for payload in [None, Some(b"{}".to_vec())] {
            let fixture = Fixture::with_state(
                TestState {
                    reject: true,
                    payload: Mutex::new(payload),
                    ..Default::default()
                },
                TIMEOUT,
            );
            assert!(fixture
                .provider()
                .headers(&request("https://storage.example/blob"))
                .await
                .is_err());
            assert_eq!(fixture.state.cancels.load(Ordering::SeqCst), 0);
            assert!(!lock_registry().counts.contains_key(&fixture.id));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn timeout_is_bounded_and_cancels_pending_work() {
        let _serial = serial().await;
        let fixture = Fixture::with_state(TestState::default(), Duration::from_millis(10));
        let start = tokio::time::Instant::now();
        assert!(fixture
            .provider()
            .headers(&request("https://storage.example/blob"))
            .await
            .is_err());
        assert_eq!(start.elapsed(), Duration::from_millis(10));
        assert_eq!(fixture.state.cancels.load(Ordering::SeqCst), 1);
        assert!(TIMEOUT <= Duration::from_secs(30));
    }

    #[tokio::test]
    async fn unregister_keeps_pending_and_store_owners_alive() {
        let _serial = serial().await;
        let mut fixture = Fixture::new(Some(b"{}".to_vec()));
        let store_owner = fixture.provider();
        unregister(fixture.id);
        fixture.provider.take();
        assert!(acquire(fixture.id).is_err());
        assert_eq!(register_inner(&descriptor(fixture.id), TIMEOUT), 2);
        assert!(store_owner
            .headers(&request("https://storage.example/blob"))
            .await
            .is_ok());
        assert_eq!(fixture.state.releases.load(Ordering::SeqCst), 0);
        drop(store_owner);
        assert_eq!(fixture.state.releases.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn malformed_known_completion_terminates_flight_with_sanitized_error() {
        let _serial = serial().await;
        for payload in [
            b"not-json private".to_vec(),
            br#"{"Authorization":"secret"}"#.to_vec(),
        ] {
            let fixture = Fixture::new(Some(payload));
            let error = fixture
                .provider()
                .headers(&request("https://storage.example/blob"))
                .await
                .unwrap_err();
            assert!(error.to_string().contains("header provider failed"));
            assert_eq!(error.kind(), object_store::client::HttpErrorKind::Unknown);
            assert!(!format!("{error:?}").contains("secret"));
            assert_eq!(fixture.state.cancels.load(Ordering::SeqCst), 0);
            assert!(!lock_registry().counts.contains_key(&fixture.id));
        }
        // SAFETY: Unknown requests do not inspect even an invalid payload pointer.
        assert_eq!(
            unsafe { complete(u64::MAX, u64::MAX, 0, invalid_bytes()) },
            1
        );
    }

    #[tokio::test]
    async fn uri_library_bound_rejects_oversized_uri_before_begin() {
        let _serial = serial().await;
        let fixture = Fixture::new(Some(b"{}".to_vec()));
        let uri = format!("https://storage.example/{}", "a".repeat(MAX_BYTES));
        assert!(http::Request::builder()
            .uri(uri)
            .body(HttpRequestBody::empty())
            .is_err());
        assert_eq!(fixture.state.calls.load(Ordering::SeqCst), 0);
    }

    fn pending_future(
        provider: Arc<Provider>,
    ) -> std::pin::Pin<Box<dyn Future<Output = HeaderResult> + Send>> {
        Box::pin(async move {
            provider
                .headers(&request("https://storage.example/blob"))
                .await
        })
    }

    async fn assert_pending(
        future: &mut std::pin::Pin<Box<dyn Future<Output = HeaderResult> + Send>>,
    ) {
        assert!(std::future::poll_fn(|context| std::task::Poll::Ready(
            future.as_mut().poll(context)
        ))
        .await
        .is_pending());
    }

    #[test]
    fn context_limit_counts_unregistered_live_owners_without_failure_release() {
        let _serial = serial_sync();
        let fixtures: Vec<_> = (0..MAX_CONTEXTS).map(|_| Fixture::new(None)).collect();
        for fixture in &fixtures {
            unregister(fixture.id);
        }
        let id = NEXT_CONTEXT.fetch_add(1, Ordering::SeqCst);
        assert_eq!(register_inner(&descriptor(id), TIMEOUT), 3);
        assert_eq!(lock_registry().live.len(), MAX_CONTEXTS);
        assert!(!lock_registry().live.contains_key(&id));
        assert!(fixtures
            .iter()
            .all(|fixture| fixture.state.releases.load(Ordering::SeqCst) == 0));
        drop(fixtures);
        assert!(lock_registry().live.is_empty());
    }

    #[tokio::test]
    async fn per_context_pending_limit_rejects_before_begin_and_reclaims_slots() {
        let _serial = serial().await;
        let fixture = Fixture::new(None);
        let mut futures = Vec::new();
        for _ in 0..MAX_CONTEXT_PENDING {
            let mut future = pending_future(fixture.provider());
            assert_pending(&mut future).await;
            futures.push(future);
        }
        assert!(fixture
            .provider()
            .headers(&request("https://storage.example/blob"))
            .await
            .is_err());
        assert_eq!(
            fixture.state.calls.load(Ordering::SeqCst),
            MAX_CONTEXT_PENDING
        );
        assert_eq!(lock_registry().counts[&fixture.id], MAX_CONTEXT_PENDING);
        drop(futures);
        assert_eq!(
            fixture.state.cancels.load(Ordering::SeqCst),
            MAX_CONTEXT_PENDING
        );
        let mut next = pending_future(fixture.provider());
        assert_pending(&mut next).await;
        drop(next);
        let requests = fixture.state.requests.lock().unwrap();
        assert!(requests.windows(2).all(|pair| pair[0].0 < pair[1].0));
        assert!(!lock_registry().counts.contains_key(&fixture.id));
    }

    #[tokio::test]
    async fn global_pending_limit_is_1024_and_has_no_cleanup_queue() {
        let _serial = serial().await;
        let fixtures: Vec<_> = (0..MAX_PENDING / MAX_CONTEXT_PENDING)
            .map(|_| Fixture::new(None))
            .collect();
        let mut futures = Vec::new();
        for fixture in &fixtures {
            for _ in 0..MAX_CONTEXT_PENDING {
                let mut future = pending_future(fixture.provider());
                assert_pending(&mut future).await;
                futures.push(future);
            }
        }
        let extra = Fixture::new(None);
        assert!(extra
            .provider()
            .headers(&request("https://storage.example/blob"))
            .await
            .is_err());
        assert_eq!(extra.state.calls.load(Ordering::SeqCst), 0);
        assert_eq!(lock_registry().pending.len(), MAX_PENDING);
        drop(futures);
        assert!(lock_registry().pending.is_empty());
        assert!(lock_registry().counts.is_empty());
    }

    #[tokio::test]
    async fn completion_from_thread_without_runtime_survives_unregister_and_copies_bytes() {
        let _serial = serial().await;
        let mut fixture = Fixture::new(None);
        let mut future = pending_future(fixture.provider());
        assert_pending(&mut future).await;
        let request_id = fixture.state.requests.lock().unwrap()[0].0;
        unregister(fixture.id);
        fixture.provider.take();
        assert_eq!(fixture.state.releases.load(Ordering::SeqCst), 0);
        let context = fixture.id;
        assert_eq!(
            std::thread::spawn(move || {
                assert!(tokio::runtime::Handle::try_current().is_err());
                let bytes = br#"{"x-custom":"owned-after-call"}"#.to_vec();
                // SAFETY: This thread owns valid completion bytes until the call returns.
                unsafe { complete(context, request_id, 0, ByteSlice::borrowed(&bytes)) }
            })
            .join()
            .unwrap(),
            0
        );
        assert_eq!(fixture.state.releases.load(Ordering::SeqCst), 0);
        let headers = future.await.unwrap();
        assert_eq!(headers["x-custom"], "owned-after-call");
        assert_eq!(fixture.state.releases.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn completion_holds_its_own_final_arc_and_releases_outside_locks() {
        let _serial = serial_sync();
        let mut fixture = Fixture::new(None);
        let request_id = NEXT_REQUEST.fetch_add(1, Ordering::Relaxed);
        let (sender, mut receiver) = oneshot::channel();
        {
            let mut registry = lock_registry();
            registry.pending.insert(
                (fixture.id, request_id),
                Flight {
                    provider: fixture.provider.take().unwrap(),
                    sender,
                },
            );
            registry.counts.insert(fixture.id, 1);
        }
        unregister(fixture.id);
        assert_eq!(fixture.state.releases.load(Ordering::SeqCst), 0);
        // SAFETY: Empty dictionary bytes remain valid throughout synchronous completion.
        assert_eq!(
            unsafe { complete(fixture.id, request_id, 0, ByteSlice::borrowed(b"{}")) },
            0
        );
        assert!(receiver.try_recv().unwrap().is_ok());
        assert_eq!(fixture.state.releases.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn known_invalid_slices_and_failure_status_terminate_without_reading_pointer() {
        let _serial = serial().await;
        let fixture = Fixture::new(None);
        for (status, bytes) in [
            (
                0,
                ByteSlice {
                    data: std::ptr::null(),
                    len: 2,
                },
            ),
            (
                0,
                ByteSlice {
                    data: std::ptr::dangling(),
                    len: 0,
                },
            ),
            (
                0,
                ByteSlice {
                    data: std::ptr::dangling(),
                    len: MAX_BYTES + 1,
                },
            ),
            (
                1,
                ByteSlice {
                    data: std::ptr::dangling(),
                    len: 16,
                },
            ),
            (u32::MAX, invalid_bytes()),
        ] {
            let mut future = pending_future(fixture.provider());
            assert_pending(&mut future).await;
            let request_id = fixture.state.requests.lock().unwrap().last().unwrap().0;
            // SAFETY: Structurally invalid or failure payloads must not be dereferenced.
            assert_eq!(
                unsafe { complete(fixture.id, request_id, status, bytes) },
                0
            );
            let error = future.await.unwrap_err();
            assert_eq!(error.kind(), object_store::client::HttpErrorKind::Unknown);
            // SAFETY: Every duplicate must ignore its payload before pointer inspection.
            assert_eq!(
                unsafe { complete(fixture.id, request_id, 0, invalid_bytes()) },
                1
            );
        }
        assert_eq!(fixture.state.cancels.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn completion_cancel_races_claim_exactly_once_and_retire_all_flights() {
        let _serial = serial().await;
        let fixture = Fixture::new(None);
        let mut claimed = 0;
        for _ in 0..64 {
            let mut future = pending_future(fixture.provider());
            assert_pending(&mut future).await;
            let request_id = fixture.state.requests.lock().unwrap().last().unwrap().0;
            let context = fixture.id;
            let thread = std::thread::spawn(move || {
                // SAFETY: The thread lends a static valid dictionary; unknown requests ignore it.
                unsafe { complete(context, request_id, 0, ByteSlice::borrowed(b"{}")) }
            });
            drop(future);
            claimed += usize::from(thread.join().unwrap() == 0);
        }
        assert_eq!(claimed + fixture.state.cancels.load(Ordering::SeqCst), 64);
        assert!(lock_registry().pending.is_empty());
        assert!(lock_registry().counts.is_empty());
    }

    #[tokio::test]
    async fn descriptor_is_copied_before_registration_returns() {
        let _serial = serial().await;
        let id = NEXT_CONTEXT.fetch_add(1, Ordering::SeqCst);
        let state = Arc::new(TestState {
            payload: Mutex::new(Some(b"{}".to_vec())),
            ..Default::default()
        });
        states().insert(id, state.clone());
        let mut callbacks = descriptor(id);
        // SAFETY: These test callbacks are static, promptly returning and retain their test state.
        assert_eq!(unsafe { register(&callbacks) }, 0);
        callbacks.begin = None;
        callbacks.cancel = None;
        callbacks.released = None;
        callbacks.context_id = 0;
        assert_eq!(callbacks.context_id, 0);
        let provider = acquire(id).unwrap();
        assert!(provider
            .headers(&request("https://storage.example/blob"))
            .await
            .is_ok());
        unregister(id);
        drop(provider);
        assert_eq!(state.releases.load(Ordering::SeqCst), 1);
        states().remove(&id);
    }
}
