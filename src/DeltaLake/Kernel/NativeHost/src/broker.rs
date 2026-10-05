use std::collections::HashMap;
use std::fmt;
use std::mem::size_of;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use delta_kernel::object_store::azure::AzureCredential;
use delta_kernel::object_store::CredentialProvider;
use futures::FutureExt;
use tokio::runtime::Runtime;
use tokio::sync::{oneshot, watch};

use crate::abi::*;

const MAX_CONTEXTS: usize = 256;
const MAX_WAITERS: usize = 1_024;
const BACKOFF: Duration = Duration::from_secs(1);
static NEXT_ID: AtomicU64 = AtomicU64::new(1);
static CONTEXTS: AtomicUsize = AtomicUsize::new(0);
static WAITERS: AtomicUsize = AtomicUsize::new(0);
static REGISTRY: OnceLock<Mutex<HashMap<u64, Slot>>> = OnceLock::new();
static PENDING: OnceLock<Mutex<HashMap<(u64, u64), Pending>>> = OnceLock::new();
static SUPERVISOR: OnceLock<Result<Runtime, BrokerError>> = OnceLock::new();

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub(crate) enum BrokerError {
    #[error("credential acquisition failed: permanent")]
    Permanent,
    #[error("credential acquisition failed: transient")]
    Transient,
    #[error("credential acquisition canceled")]
    Canceled,
    #[error("credential acquisition timed out")]
    TimedOut,
    #[error("credential result invalid")]
    InvalidResult,
    #[error("credential capacity exhausted")]
    Capacity,
    #[error("credential host failure")]
    Internal,
}

type Outcome = Result<Arc<CachedToken>, BrokerError>;

enum Slot {
    Reserved(Permit),
    Registered(Arc<Registration>),
}

pub(crate) struct Registration {
    pub(crate) descriptor: KernelCredentialRegistrationV1,
    active: AtomicBool,
    release_authorized: AtomicBool,
    _permit: Permit,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    cached: Option<Arc<CachedToken>>,
    flight: Option<Arc<Flight>>,
    retry_after: Option<Instant>,
}

#[derive(Clone)]
pub(crate) struct CachedToken {
    pub(crate) credential: Arc<AzureCredential>,
    expires_unix_ms: i64,
    hard_deadline: Instant,
    refresh_at: Instant,
}

struct Flight {
    generation: u64,
    deadline: Instant,
    result: watch::Sender<Option<Outcome>>,
    cancel: watch::Sender<bool>,
    interest: Mutex<Interest>,
}

struct Interest {
    count: usize,
    retired: bool,
}

struct Waiter {
    flight: Arc<Flight>,
    _permit: Permit,
}

struct Pending {
    registration: Arc<Registration>,
    deadline: Instant,
    sender: oneshot::Sender<Outcome>,
}

struct PendingGuard {
    registration: Arc<Registration>,
    request_id: u64,
    accepted: bool,
}

struct Permit(&'static AtomicUsize);

pub(crate) struct Provider(pub(crate) Arc<Registration>);

impl fmt::Debug for Provider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("KernelAzureBearerProvider(<redacted>)")
    }
}

#[async_trait]
impl CredentialProvider for Provider {
    type Credential = AzureCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<AzureCredential>> {
        self.0
            .token()
            .await
            .map(|cached| cached.credential.clone())
            .map_err(|error| object_store::Error::Generic {
                store: "kernel credential host",
                source: Box::new(error),
            })
    }
}

impl Drop for Registration {
    fn drop(&mut self) {
        if self.release_authorized.load(Ordering::Acquire) {
            if let Some(released) = self.descriptor.released {
                unsafe { released(self.descriptor.context_id) };
            }
        }
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

impl Drop for Waiter {
    fn drop(&mut self) {
        let mut interest = lock(&self.flight.interest);
        interest.count -= 1;
        if interest.count == 0 && !interest.retired {
            interest.retired = true;
            self.flight.cancel.send_replace(true);
        }
    }
}

impl Drop for PendingGuard {
    fn drop(&mut self) {
        self.retire(1);
    }
}

impl PendingGuard {
    fn retire(&self, reason: u32) -> bool {
        let removed =
            lock(pending()).remove(&(self.registration.descriptor.context_id, self.request_id));
        if removed.is_some() && self.accepted {
            if let Some(cancel) = self.registration.descriptor.cancel {
                unsafe {
                    cancel(
                        self.registration.descriptor.context_id,
                        self.request_id,
                        reason,
                    )
                };
            }
        }
        removed.is_some()
    }
}

impl CachedToken {
    fn for_delivery(self: Arc<Self>) -> Outcome {
        if self.usable() {
            Ok(self)
        } else {
            Err(BrokerError::InvalidResult)
        }
    }

    fn usable(&self) -> bool {
        unix_ms().is_ok_and(|now| self.usable_at(now, Instant::now()))
    }

    fn usable_at(&self, unix_ms: i64, instant: Instant) -> bool {
        instant < self.hard_deadline
            && self
                .expires_unix_ms
                .checked_sub(unix_ms)
                .is_some_and(|remaining| remaining > i64::from(MINIMUM_LIFETIME_MS))
    }

    fn new(bytes: &[u8], expiry: i64, generation: u64) -> Result<Self, BrokerError> {
        let now = Instant::now();
        let useful_ms =
            validate_token(bytes, expiry, unix_ms()?).map_err(|_| BrokerError::InvalidResult)?;
        let token = std::str::from_utf8(bytes)
            .map_err(|_| BrokerError::InvalidResult)?
            .to_owned();
        let lead_ms = 120_000.min(useful_ms / 5);
        let jitter_cap = 30_000.min(useful_ms / 10);
        let jitter = generation.wrapping_mul(0x9e3779b97f4a7c15) % (jitter_cap + 1);
        Ok(Self {
            credential: Arc::new(AzureCredential::BearerToken(token)),
            expires_unix_ms: expiry,
            hard_deadline: now
                .checked_add(Duration::from_millis(useful_ms))
                .ok_or(BrokerError::InvalidResult)?,
            refresh_at: now
                .checked_add(Duration::from_millis(useful_ms - lead_ms - jitter))
                .ok_or(BrokerError::InvalidResult)?,
        })
    }
}

impl Registration {
    pub(crate) async fn token(self: &Arc<Self>) -> Outcome {
        let permit = permit(&WAITERS, MAX_WAITERS).map_err(|_| BrokerError::Capacity)?;
        let runtime = supervisor()?;
        let (flight, launch) = loop {
            let mut retiring = {
                let mut state = lock(&self.state);
                if let Some(cached) = &state.cached {
                    if cached.usable()
                        && (Instant::now() < cached.refresh_at
                            || state
                                .retry_after
                                .is_some_and(|until| Instant::now() < until))
                    {
                        return Ok(cached.clone());
                    }
                }
                if state
                    .retry_after
                    .is_some_and(|until| Instant::now() < until)
                {
                    return Err(BrokerError::Transient);
                }
                if let Some(flight) = &state.flight {
                    let mut interest = lock(&flight.interest);
                    if !interest.retired {
                        interest.count += 1;
                        break (flight.clone(), false);
                    }
                    flight.result.subscribe()
                } else {
                    let generation = allocate_id().ok_or(BrokerError::Capacity)?;
                    let (result, _) = watch::channel(None);
                    let (cancel, _) = watch::channel(false);
                    let flight = Arc::new(Flight {
                        generation,
                        deadline: Instant::now()
                            + Duration::from_millis(u64::from(
                                self.descriptor.acquisition_timeout_ms,
                            )),
                        result,
                        cancel,
                        interest: Mutex::new(Interest {
                            count: 1,
                            retired: false,
                        }),
                    });
                    state.flight = Some(flight.clone());
                    break (flight, true);
                }
            };
            while retiring.borrow_and_update().is_none() {
                retiring
                    .changed()
                    .await
                    .map_err(|_| BrokerError::Internal)?;
            }
        };
        let mut receiver = flight.result.subscribe();
        let waiter = Waiter {
            flight,
            _permit: permit,
        };
        if launch {
            let registration = self.clone();
            let flight = waiter.flight.clone();
            runtime.spawn(async move {
                let outcome = std::panic::AssertUnwindSafe(registration.acquire(&flight))
                    .catch_unwind()
                    .await
                    .unwrap_or(Err(BrokerError::Internal));
                registration.finish(&flight, outcome);
            });
        }
        loop {
            if let Some(outcome) = receiver.borrow_and_update().clone() {
                return checked_outcome(
                    outcome.and_then(CachedToken::for_delivery),
                    waiter.flight.deadline,
                );
            }
            receiver
                .changed()
                .await
                .map_err(|_| BrokerError::Internal)?;
        }
    }

    async fn acquire(self: &Arc<Self>, flight: &Arc<Flight>) -> Outcome {
        let deadline = flight.deadline;
        let mut cancellation = flight.cancel.subscribe();
        for attempt in 0..2 {
            if *cancellation.borrow() {
                return Err(BrokerError::Canceled);
            }
            let outcome = self.attempt(deadline, &mut cancellation).await;
            if outcome.as_ref().err() != Some(&BrokerError::Transient) || attempt == 1 {
                return outcome;
            }
            tokio::select! {
                _ = cancellation.changed() => return Err(BrokerError::Canceled),
                _ = tokio::time::sleep_until(deadline.into()) => return Err(BrokerError::TimedOut),
                _ = tokio::time::sleep(BACKOFF) => {}
            }
        }
        Err(BrokerError::Internal)
    }

    async fn attempt(
        self: &Arc<Self>,
        deadline: Instant,
        cancellation: &mut watch::Receiver<bool>,
    ) -> Outcome {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .ok_or(BrokerError::TimedOut)?;
        let timeout_ms = u32::try_from(
            remaining
                .as_millis()
                .saturating_add(u128::from(remaining.subsec_nanos() % 1_000_000 != 0)),
        )
        .map_err(|_| BrokerError::Internal)?
        .min(self.descriptor.acquisition_timeout_ms);
        if timeout_ms == 0 {
            return Err(BrokerError::TimedOut);
        }
        if *cancellation.borrow() {
            return Err(BrokerError::Canceled);
        }
        let request_id = allocate_id().ok_or(BrokerError::Capacity)?;
        let (sender, mut receiver) = oneshot::channel();
        lock(pending()).insert(
            (self.descriptor.context_id, request_id),
            Pending {
                registration: self.clone(),
                deadline,
                sender,
            },
        );
        let mut guard = PendingGuard {
            registration: self.clone(),
            request_id,
            accepted: false,
        };
        let request = KernelCredentialRequestV1 {
            abi_version: ABI_VERSION,
            struct_size: size_of::<KernelCredentialRequestV1>() as u32,
            context_id: self.descriptor.context_id,
            request_id,
            credential_kind: AZURE_BEARER,
            timeout_ms,
            minimum_lifetime_ms: MINIMUM_LIFETIME_MS,
            flags: 0,
            reserved: [0; 2],
        };
        let begin = self.descriptor.begin.ok_or(BrokerError::Internal)?;
        let admission = unsafe { begin(&request) };
        if admission != 0 {
            return Err(match admission {
                1 => BrokerError::Transient,
                2 => BrokerError::Canceled,
                3 => BrokerError::InvalidResult,
                _ => BrokerError::Internal,
            });
        }
        guard.accepted = true;
        let (reason, error) = tokio::select! {
            result = &mut receiver => return checked_outcome(result.unwrap_or(Err(BrokerError::Internal)), deadline),
            _ = cancellation.wait_for(|value| *value) => (1, BrokerError::Canceled),
            _ = tokio::time::sleep_until(deadline.into()) => (2, BrokerError::TimedOut),
        };
        if guard.retire(reason) {
            Err(error)
        } else {
            checked_outcome(
                receiver.await.unwrap_or(Err(BrokerError::Internal)),
                deadline,
            )
        }
    }

    fn finish(&self, flight: &Flight, outcome: Outcome) {
        let mut state = lock(&self.state);
        if state
            .flight
            .as_ref()
            .is_some_and(|current| current.generation == flight.generation)
        {
            let mut interest = lock(&flight.interest);
            let mut outcome = checked_outcome(
                if interest.retired {
                    Err(BrokerError::Canceled)
                } else {
                    outcome
                },
                flight.deadline,
            );
            match &outcome {
                Ok(cached) => {
                    state.cached = Some(cached.clone());
                    state.retry_after = None;
                }
                Err(BrokerError::Transient) => {
                    state.retry_after = Some(Instant::now() + BACKOFF);
                    if let Some(cached) = state.cached.as_ref().filter(|cached| cached.usable()) {
                        outcome = Ok(cached.clone());
                    }
                }
                Err(BrokerError::Canceled | BrokerError::TimedOut) => {}
                Err(_) => {
                    state.cached = None;
                    state.retry_after = None;
                }
            }
            interest.retired = true;
            state.flight = None;
            drop(interest);
            flight.result.send_replace(Some(outcome));
        }
    }

    #[cfg(test)]
    pub(crate) fn invalidate_cache(&self) {
        lock(&self.state).cached = None;
    }

    #[cfg(test)]
    pub(crate) fn waiter_count(&self) -> usize {
        lock(&self.state)
            .flight
            .as_ref()
            .map_or(0, |flight| lock(&flight.interest).count)
    }

    #[cfg(test)]
    pub(crate) fn force_refresh(&self) {
        let mut state = lock(&self.state);
        if let Some(cached) = &mut state.cached {
            Arc::make_mut(cached).refresh_at = Instant::now();
        }
    }
}

pub(crate) fn lock<Type>(value: &Mutex<Type>) -> MutexGuard<'_, Type> {
    value
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn registry() -> &'static Mutex<HashMap<u64, Slot>> {
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

fn pending() -> &'static Mutex<HashMap<(u64, u64), Pending>> {
    PENDING.get_or_init(|| Mutex::new(HashMap::new()))
}

fn checked_outcome(outcome: Outcome, deadline: Instant) -> Outcome {
    if Instant::now() >= deadline {
        Err(BrokerError::TimedOut)
    } else {
        outcome
    }
}

fn permit(counter: &'static AtomicUsize, limit: usize) -> Result<Permit, u32> {
    counter
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
            (count < limit).then_some(count + 1)
        })
        .map(|_| Permit(counter))
        .map_err(|_| CAPACITY_EXHAUSTED)
}

pub(crate) fn allocate_id() -> Option<u64> {
    allocate_checked_id(&NEXT_ID)
}

fn allocate_checked_id(counter: &AtomicU64) -> Option<u64> {
    counter
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            current.checked_add(1)
        })
        .ok()
}

fn supervisor() -> Result<&'static Runtime, BrokerError> {
    SUPERVISOR
        .get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .thread_name("kernel-credential-broker")
                .build()
                .map_err(|_| BrokerError::Internal)
        })
        .as_ref()
        .map_err(|error| *error)
}

pub(crate) fn unix_ms() -> Result<i64, BrokerError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_millis()).ok())
        .ok_or(BrokerError::Internal)
}

pub(crate) fn context_new() -> Result<u64, u32> {
    let permit = permit(&CONTEXTS, MAX_CONTEXTS)?;
    let context_id = allocate_id().ok_or(CAPACITY_EXHAUSTED)?;
    lock(registry()).insert(context_id, Slot::Reserved(permit));
    Ok(context_id)
}

pub(crate) fn register(descriptor: KernelCredentialRegistrationV1) -> Result<(), u32> {
    validate_registration(&descriptor)?;
    let mut contexts = lock(registry());
    let Some(slot) = contexts.get(&descriptor.context_id) else {
        return Err(CLOSED);
    };
    if matches!(slot, Slot::Registered(_)) {
        return Err(DUPLICATE_ID);
    }
    let Some(Slot::Reserved(permit)) = contexts.remove(&descriptor.context_id) else {
        return Err(INTERNAL_FAILURE);
    };
    contexts.insert(
        descriptor.context_id,
        Slot::Registered(Arc::new(Registration {
            descriptor,
            active: AtomicBool::new(false),
            release_authorized: AtomicBool::new(false),
            _permit: permit,
            state: Mutex::new(State::default()),
        })),
    );
    Ok(())
}

pub(crate) fn activate(context_id: u64) -> Result<(), u32> {
    let contexts = lock(registry());
    let Some(Slot::Registered(registration)) = contexts.get(&context_id) else {
        return Err(CLOSED);
    };
    registration
        .release_authorized
        .store(true, Ordering::Release);
    registration.active.store(true, Ordering::Release);
    Ok(())
}

pub(crate) fn attach(context_id: u64) -> Result<Arc<Registration>, u32> {
    let contexts = lock(registry());
    let Some(Slot::Registered(registration)) = contexts.get(&context_id) else {
        return Err(CLOSED);
    };
    if !registration.active.load(Ordering::Acquire) {
        return Err(CLOSED);
    }
    Ok(registration.clone())
}

pub(crate) fn unregister(context_id: u64) -> Result<(), u32> {
    let removed = lock(registry()).remove(&context_id).ok_or(CLOSED)?;
    if let Slot::Registered(registration) = &removed {
        registration
            .release_authorized
            .store(true, Ordering::Release);
    }
    drop(removed);
    Ok(())
}

pub(crate) unsafe fn complete(
    context_id: u64,
    request_id: u64,
    result: *const KernelCredentialResultV1,
) -> u32 {
    let Some(pending) = lock(pending()).remove(&(context_id, request_id)) else {
        return 1;
    };
    if Instant::now() >= pending.deadline {
        let _ = pending.sender.send(Err(BrokerError::TimedOut));
        return 0;
    }
    let outcome = unsafe { copy_result(result, &pending.registration, request_id) };
    let status = if outcome.as_ref().err() == Some(&BrokerError::InvalidResult) {
        2
    } else {
        0
    };
    let _ = pending.sender.send(outcome);
    status
}

unsafe fn copy_result(
    result: *const KernelCredentialResultV1,
    registration: &Registration,
    request_id: u64,
) -> Outcome {
    if result.is_null() {
        return Err(BrokerError::InvalidResult);
    }
    let header = unsafe { result.cast::<Header>().read_unaligned() };
    validate_header::<KernelCredentialResultV1>(header).map_err(|_| BrokerError::InvalidResult)?;
    let result = unsafe { result.read_unaligned() };
    validate_result_descriptor(&result, registration.descriptor.max_token_bytes)
        .map_err(|_| BrokerError::InvalidResult)?;
    if result.status != 0 {
        return Err(match result.status {
            1 => BrokerError::Permanent,
            2 => BrokerError::Transient,
            3 => BrokerError::Canceled,
            4 => BrokerError::TimedOut,
            _ => BrokerError::InvalidResult,
        });
    }
    let bytes = unsafe { std::slice::from_raw_parts(result.token_utf8, result.token_len as usize) };
    CachedToken::new(bytes, result.expires_unix_ms, request_id).map(Arc::new)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn deadline_registration() -> Arc<Registration> {
        unsafe extern "C" fn begin(_request: *const KernelCredentialRequestV1) -> u32 {
            4
        }
        Arc::new(Registration {
            descriptor: KernelCredentialRegistrationV1 {
                abi_version: ABI_VERSION,
                struct_size: size_of::<KernelCredentialRegistrationV1>() as u32,
                context_id: allocate_id().unwrap(),
                credential_kind: AZURE_BEARER,
                acquisition_timeout_ms: DEFAULT_TIMEOUT_MS,
                max_token_bytes: MAX_TOKEN_BYTES,
                flags: 0,
                begin: Some(begin),
                cancel: None,
                released: None,
                reserved: [0; 2],
            },
            active: AtomicBool::new(true),
            release_authorized: AtomicBool::new(false),
            _permit: permit(&CONTEXTS, MAX_CONTEXTS).unwrap(),
            state: Mutex::new(State::default()),
        })
    }

    fn deadline_flight(registration: &Registration, deadline: Instant) -> Arc<Flight> {
        let flight = Arc::new(Flight {
            generation: allocate_id().unwrap(),
            deadline,
            result: watch::channel(None).0,
            cancel: watch::channel(false).0,
            interest: Mutex::new(Interest {
                count: 1,
                retired: false,
            }),
        });
        lock(&registration.state).flight = Some(flight.clone());
        flight
    }

    fn deadline_pending(
        registration: &Arc<Registration>,
        deadline: Instant,
    ) -> (u64, oneshot::Receiver<Outcome>) {
        let request_id = allocate_id().unwrap();
        let (sender, receiver) = oneshot::channel();
        lock(pending()).insert(
            (registration.descriptor.context_id, request_id),
            Pending {
                registration: registration.clone(),
                deadline,
                sender,
            },
        );
        (request_id, receiver)
    }

    fn deadline_result() -> KernelCredentialResultV1 {
        KernelCredentialResultV1 {
            abi_version: ABI_VERSION,
            struct_size: size_of::<KernelCredentialResultV1>() as u32,
            status: 0,
            credential_kind: AZURE_BEARER,
            token_utf8: b"synthetic".as_ptr(),
            token_len: b"synthetic".len() as u64,
            expires_unix_ms: i64::MAX,
            reserved: [0; 2],
        }
    }

    #[tokio::test]
    async fn pending_deadline_expired_completion_does_not_read_descriptor() {
        let registration = deadline_registration();
        let (request_id, receiver) =
            deadline_pending(&registration, Instant::now() - Duration::from_secs(1));
        let unreadable = std::ptr::without_provenance(1);

        assert_eq!(
            unsafe { complete(registration.descriptor.context_id, request_id, unreadable) },
            0
        );
        assert!(matches!(
            receiver.await.unwrap(),
            Err(BrokerError::TimedOut)
        ));
        assert_eq!(
            unsafe { complete(registration.descriptor.context_id, request_id, unreadable) },
            1
        );
    }

    #[tokio::test]
    async fn pending_deadline_expired_completion_does_not_read_token() {
        let registration = deadline_registration();
        let (request_id, receiver) =
            deadline_pending(&registration, Instant::now() - Duration::from_secs(1));
        let result = KernelCredentialResultV1 {
            token_utf8: std::ptr::without_provenance(1),
            ..deadline_result()
        };

        assert_eq!(
            unsafe { complete(registration.descriptor.context_id, request_id, &result) },
            0
        );
        assert!(matches!(
            receiver.await.unwrap(),
            Err(BrokerError::TimedOut)
        ));
    }

    #[tokio::test]
    async fn pending_deadline_expired_in_begin_rejects_valid_completion() {
        unsafe extern "C" fn begin(request: *const KernelCredentialRequestV1) -> u32 {
            let request = unsafe { request.read_unaligned() };
            {
                let mut pending = lock(pending());
                let Some(pending) = pending.get_mut(&(request.context_id, request.request_id))
                else {
                    return 4;
                };
                pending.deadline = Instant::now() - Duration::from_secs(1);
            }
            unsafe { complete(request.context_id, request.request_id, &deadline_result()) }
        }
        let mut registration = deadline_registration();
        Arc::get_mut(&mut registration).unwrap().descriptor.begin = Some(begin);
        let (_cancel, mut cancellation) = watch::channel(false);

        assert!(matches!(
            registration
                .attempt(Instant::now() + Duration::from_secs(60), &mut cancellation)
                .await,
            Err(BrokerError::TimedOut)
        ));
    }

    #[tokio::test]
    async fn pending_deadline_claimed_success_is_rejected_at_late_delivery() {
        let registration = deadline_registration();
        let (request_id, receiver) =
            deadline_pending(&registration, Instant::now() + Duration::from_secs(60));

        assert_eq!(
            unsafe {
                complete(
                    registration.descriptor.context_id,
                    request_id,
                    &deadline_result(),
                )
            },
            0
        );
        let deadline = Instant::now() - Duration::from_secs(1);
        let outcome = receiver.await.unwrap();
        assert!(outcome.is_ok());
        assert!(matches!(
            checked_outcome(outcome, deadline),
            Err(BrokerError::TimedOut)
        ));
    }

    #[tokio::test]
    async fn pending_deadline_claimed_success_cannot_win_timeout_retirement() {
        let registration = deadline_registration();
        let (request_id, receiver) =
            deadline_pending(&registration, Instant::now() + Duration::from_secs(60));
        let claimed = lock(pending())
            .remove(&(registration.descriptor.context_id, request_id))
            .unwrap();
        let guard = PendingGuard {
            registration,
            request_id,
            accepted: true,
        };
        let deadline = Instant::now() - Duration::from_secs(1);

        assert!(!guard.retire(2));
        let cached = Arc::new(CachedToken::new(b"synthetic", i64::MAX, request_id).unwrap());
        assert!(claimed.sender.send(Ok(cached)).is_ok());
        assert!(matches!(
            checked_outcome(receiver.await.unwrap(), deadline),
            Err(BrokerError::TimedOut)
        ));
    }

    #[tokio::test]
    async fn flight_deadline_delayed_waiter_rejects_queued_success() {
        let registration = deadline_registration();
        let flight = deadline_flight(&registration, Instant::now() - Duration::from_secs(1));
        let cached = Arc::new(CachedToken::new(b"synthetic", i64::MAX, 1).unwrap());
        flight.result.send_replace(Some(Ok(cached)));

        assert!(matches!(
            registration.token().await,
            Err(BrokerError::TimedOut)
        ));
        assert!(lock(&registration.state).cached.is_none());
    }

    #[test]
    fn flight_deadline_expired_success_is_not_published_or_cached() {
        let registration = deadline_registration();
        let old = Arc::new(CachedToken::new(b"existing", i64::MAX, 1).unwrap());
        lock(&registration.state).cached = Some(old.clone());
        let flight = deadline_flight(&registration, Instant::now() - Duration::from_secs(1));
        let cached = Arc::new(CachedToken::new(b"late", i64::MAX, 2).unwrap());

        registration.finish(&flight, Ok(cached));

        assert!(matches!(
            *flight.result.borrow(),
            Some(Err(BrokerError::TimedOut))
        ));
        let state = lock(&registration.state);
        assert!(Arc::ptr_eq(state.cached.as_ref().unwrap(), &old));
        assert!(state.flight.is_none());
        assert!(state.retry_after.is_none());
        assert!(lock(&flight.interest).retired);
    }

    #[tokio::test]
    async fn last_waiter_retirement_before_claimed_success_requires_fresh_flight() {
        unsafe extern "C" fn begin(request: *const KernelCredentialRequestV1) -> u32 {
            let request = unsafe { request.read_unaligned() };
            let result = KernelCredentialResultV1 {
                token_utf8: b"B".as_ptr(),
                token_len: 1,
                ..deadline_result()
            };
            unsafe { complete(request.context_id, request.request_id, &result) }
        }
        let mut registration = deadline_registration();
        Arc::get_mut(&mut registration).unwrap().descriptor.begin = Some(begin);
        let mut old = CachedToken::new(b"existing", i64::MAX, 1).unwrap();
        old.refresh_at = Instant::now() - Duration::from_secs(1);
        let old = Arc::new(old);
        let retry_after = Instant::now() - Duration::from_secs(1);
        {
            let mut state = lock(&registration.state);
            state.cached = Some(old.clone());
            state.retry_after = Some(retry_after);
        }
        let deadline = Instant::now() + Duration::from_secs(60);
        let flight = deadline_flight(&registration, deadline);
        let (request_id, receiver) = deadline_pending(&registration, deadline);
        let guard = PendingGuard {
            registration: registration.clone(),
            request_id,
            accepted: true,
        };
        let waiter = Waiter {
            flight: flight.clone(),
            _permit: permit(&WAITERS, MAX_WAITERS).unwrap(),
        };

        drop(waiter);

        {
            let interest = lock(&flight.interest);
            assert_eq!(interest.count, 0);
            assert!(interest.retired);
        }
        assert!(*flight.cancel.borrow());
        let mut next = Box::pin(registration.token());
        assert!(futures::poll!(next.as_mut()).is_pending());
        assert_eq!(registration.waiter_count(), 0);
        assert_eq!(
            unsafe {
                complete(
                    registration.descriptor.context_id,
                    request_id,
                    &deadline_result(),
                )
            },
            0
        );
        assert!(!guard.retire(1));
        let outcome = receiver.await.unwrap();
        assert!(outcome.is_ok());

        registration.finish(&flight, outcome);

        assert!(matches!(
            *flight.result.borrow(),
            Some(Err(BrokerError::Canceled))
        ));
        {
            let state = lock(&registration.state);
            assert!(Arc::ptr_eq(state.cached.as_ref().unwrap(), &old));
            assert_eq!(state.retry_after, Some(retry_after));
            assert!(state.flight.is_none());
        }
        let cached = tokio::time::timeout(Duration::from_secs(5), next)
            .await
            .unwrap()
            .unwrap();
        match cached.credential.as_ref() {
            AzureCredential::BearerToken(token) => assert_eq!(token, "B"),
            _ => panic!("only bearer credentials are supported"),
        }
        assert!(!Arc::ptr_eq(&cached, &old));
    }

    #[test]
    fn last_waiter_retirement_preserves_cache_and_backoff_for_every_outcome() {
        let registration = deadline_registration();
        let old = Arc::new(CachedToken::new(b"existing", i64::MAX, 1).unwrap());
        let retry_after = Instant::now() + Duration::from_secs(60);
        {
            let mut state = lock(&registration.state);
            state.cached = Some(old.clone());
            state.retry_after = Some(retry_after);
        }
        let outcomes = [
            Ok(Arc::new(CachedToken::new(b"late", i64::MAX, 2).unwrap())),
            Err(BrokerError::Permanent),
            Err(BrokerError::Transient),
            Err(BrokerError::Canceled),
            Err(BrokerError::TimedOut),
            Err(BrokerError::InvalidResult),
            Err(BrokerError::Capacity),
            Err(BrokerError::Internal),
        ];
        for (deadline, expected) in [
            (
                Instant::now() + Duration::from_secs(60),
                BrokerError::Canceled,
            ),
            (
                Instant::now() - Duration::from_secs(1),
                BrokerError::TimedOut,
            ),
        ] {
            for outcome in &outcomes {
                let flight = deadline_flight(&registration, deadline);
                drop(Waiter {
                    flight: flight.clone(),
                    _permit: permit(&WAITERS, MAX_WAITERS).unwrap(),
                });

                registration.finish(&flight, outcome.clone());

                assert_eq!(
                    flight.result.borrow().as_ref().unwrap().as_ref().err(),
                    Some(&expected)
                );
                let state = lock(&registration.state);
                assert!(Arc::ptr_eq(state.cached.as_ref().unwrap(), &old));
                assert_eq!(state.retry_after, Some(retry_after));
                assert!(state.flight.is_none());
                assert!(lock(&flight.interest).retired);
            }
        }
    }

    #[test]
    fn flight_deadline_expired_transient_does_not_deliver_old_cache() {
        let registration = deadline_registration();
        let old = Arc::new(CachedToken::new(b"existing", i64::MAX, 1).unwrap());
        lock(&registration.state).cached = Some(old.clone());
        let flight = deadline_flight(&registration, Instant::now() - Duration::from_secs(1));

        registration.finish(&flight, Err(BrokerError::Transient));

        assert!(matches!(
            *flight.result.borrow(),
            Some(Err(BrokerError::TimedOut))
        ));
        let state = lock(&registration.state);
        assert!(Arc::ptr_eq(state.cached.as_ref().unwrap(), &old));
        assert!(state.retry_after.is_none());
    }

    #[test]
    fn flight_deadline_expired_failure_does_not_clear_old_cache() {
        let registration = deadline_registration();
        let old = Arc::new(CachedToken::new(b"existing", i64::MAX, 1).unwrap());
        lock(&registration.state).cached = Some(old.clone());
        let flight = deadline_flight(&registration, Instant::now() - Duration::from_secs(1));

        registration.finish(&flight, Err(BrokerError::Permanent));

        assert!(matches!(
            *flight.result.borrow(),
            Some(Err(BrokerError::TimedOut))
        ));
        assert!(Arc::ptr_eq(
            lock(&registration.state).cached.as_ref().unwrap(),
            &old
        ));
    }

    #[test]
    fn flight_deadline_stale_generation_cannot_modify_current_flight() {
        let registration = deadline_registration();
        let old = Arc::new(CachedToken::new(b"existing", i64::MAX, 1).unwrap());
        lock(&registration.state).cached = Some(old.clone());
        let stale = deadline_flight(&registration, Instant::now() - Duration::from_secs(1));
        let current = deadline_flight(&registration, Instant::now() + Duration::from_secs(60));

        registration.finish(&stale, Err(BrokerError::Permanent));

        let state = lock(&registration.state);
        assert!(Arc::ptr_eq(state.cached.as_ref().unwrap(), &old));
        assert!(Arc::ptr_eq(state.flight.as_ref().unwrap(), &current));
        assert!(stale.result.borrow().is_none());
        assert!(current.result.borrow().is_none());
        assert!(!lock(&current.interest).retired);
    }

    #[test]
    fn flight_deadline_unexpired_transient_can_deliver_old_cache() {
        let registration = deadline_registration();
        let old = Arc::new(CachedToken::new(b"existing", i64::MAX, 1).unwrap());
        lock(&registration.state).cached = Some(old.clone());
        let flight = deadline_flight(&registration, Instant::now() + Duration::from_secs(60));

        registration.finish(&flight, Err(BrokerError::Transient));

        let outcome = flight.result.borrow().clone().unwrap().unwrap();
        assert!(Arc::ptr_eq(&outcome, &old));
        assert!(lock(&registration.state).retry_after.is_some());
    }

    #[tokio::test]
    async fn exhausted_original_budget_never_reaches_begin() {
        static BEGINS: AtomicUsize = AtomicUsize::new(0);
        unsafe extern "C" fn begin(_request: *const KernelCredentialRequestV1) -> u32 {
            BEGINS.fetch_add(1, Ordering::AcqRel);
            4
        }
        unsafe extern "C" fn cancel(_context: u64, _request: u64, _reason: u32) {}
        unsafe extern "C" fn released(_context: u64) {}
        let context_id = context_new().unwrap();
        register(KernelCredentialRegistrationV1 {
            abi_version: 1,
            struct_size: 72,
            context_id,
            credential_kind: 1,
            acquisition_timeout_ms: DEFAULT_TIMEOUT_MS,
            max_token_bytes: MAX_TOKEN_BYTES,
            flags: 0,
            begin: Some(begin),
            cancel: Some(cancel),
            released: Some(released),
            reserved: [0; 2],
        })
        .unwrap();
        activate(context_id).unwrap();
        let registration = attach(context_id).unwrap();
        let flight = Arc::new(Flight {
            generation: allocate_id().unwrap(),
            deadline: Instant::now() - Duration::from_secs(1),
            result: watch::channel(None).0,
            cancel: watch::channel(false).0,
            interest: Mutex::new(Interest {
                count: 1,
                retired: false,
            }),
        });
        assert!(matches!(
            registration.acquire(&flight).await,
            Err(BrokerError::TimedOut)
        ));
        assert_eq!(BEGINS.load(Ordering::Acquire), 0);
        unregister(context_id).unwrap();
    }

    #[test]
    fn wall_clock_rollback_does_not_extend_monotonic_hard_expiry() {
        let now = unix_ms().unwrap();
        let cached = CachedToken::new(b"synthetic", now + 1_000_000, 1).unwrap();
        assert!(cached.usable_at(now, Instant::now()));
        assert!(!cached.usable_at(now - 3_600_000, cached.hard_deadline));
        assert!(!cached.usable_at(now + 910_000, Instant::now()));
        assert!(cached.refresh_at < cached.hard_deadline);
    }

    #[test]
    fn delayed_waiter_cannot_receive_a_token_past_its_hard_deadline() {
        let mut cached = CachedToken::new(b"synthetic", i64::MAX, 3).unwrap();
        cached.hard_deadline = Instant::now();
        assert!(matches!(
            Arc::new(cached).for_delivery(),
            Err(BrokerError::InvalidResult)
        ));
    }

    #[test]
    fn cache_lifetime_is_capped_at_one_day() {
        let before = Instant::now();
        let cached = CachedToken::new(b"synthetic-secret", i64::MAX, 2).unwrap();
        assert!(cached.hard_deadline.duration_since(before) <= Duration::from_secs(86_401));
        let fixture_counter = AtomicU64::new(u64::MAX);
        assert_eq!(allocate_checked_id(&fixture_counter), None);
        assert_eq!(fixture_counter.load(Ordering::Acquire), u64::MAX);
    }

    #[test]
    fn bounded_admission_retains_slots_until_owned_permits_drop() {
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let first = permit(&COUNTER, 2).unwrap();
        let second = permit(&COUNTER, 2).unwrap();
        assert!(matches!(permit(&COUNTER, 2), Err(CAPACITY_EXHAUSTED)));
        drop(first);
        let replacement = permit(&COUNTER, 2).unwrap();
        assert_eq!(COUNTER.load(Ordering::Acquire), 2);
        drop(second);
        drop(replacement);
        assert_eq!(COUNTER.load(Ordering::Acquire), 0);
        assert_eq!((MAX_CONTEXTS, MAX_WAITERS), (256, 1_024));
    }
}
