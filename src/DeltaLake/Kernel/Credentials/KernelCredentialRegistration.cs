using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using DeltaLake.Credentials;
using DeltaLake.Kernel.Callbacks.Errors;
using DeltaLake.Kernel.Interop;

namespace DeltaLake.Kernel.Credentials
{
    [SuppressMessage("Design", "CA1031", Justification = "Registration's reverse-P/Invoke callbacks and request admission must contain all managed exceptions; no managed exception may cross the native credential ABI.")]
    internal sealed unsafe class KernelCredentialRegistration : IDisposable
    {
        private const uint MinimumLifetimeMs = 90000;
        private const int MaxOptionCount = 256;
        private static readonly ConcurrentDictionary<ulong, KernelCredentialRegistration> Registrations = new ConcurrentDictionary<ulong, KernelCredentialRegistration>();
        private static readonly KernelCredentialBegin BeginCallback = Begin;
        private static readonly KernelCredentialCancel CancelCallback = Cancel;
        private static readonly KernelCredentialReleased ReleasedCallback = NativeReleased;
        private static readonly AllocateErrorFn AllocateError = AllocateErrorCallbacks.AllocateError;
        private static readonly IntPtr AllocateErrorPointer = Marshal.GetFunctionPointerForDelegate(AllocateError);
        private static readonly UTF8Encoding StrictUtf8 = new UTF8Encoding(false, true);
        private readonly KernelAzureBearerCredentialOptions _options;
        private readonly object _attachGate = new object();
        private readonly object _requestGate = new object();
        private readonly ConcurrentDictionary<ulong, KernelCredentialRequest> _requests = new ConcurrentDictionary<ulong, KernelCredentialRequest>();
        private readonly TaskCompletionSource<bool> _released = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        private GCHandle _root;
        private bool _mapped;
        private bool _disposed;
        private int _nativeReleased;

        private KernelCredentialRegistration(ulong contextId, KernelAzureBearerCredentialOptions options)
        {
            ContextId = contextId;
            _options = options;
        }

        internal ulong ContextId { get; }

        internal Task Released => _released.Task;

        internal int PendingRequestCount => _requests.Count;

        internal static int RegistrationCount => Registrations.Count;

        public void Dispose()
        {
            lock (_attachGate)
            {
                if (_disposed)
                {
                    return;
                }

                _disposed = true;
            }

            try
            {
                var status = KernelCredentialInterop.kernel_credential_unregister(ContextId);
                if (status != 0 && status != 2)
                {
                    throw ControlFailure();
                }
            }
            catch (Exception error) when (IsUnavailable(error))
            {
                throw Unsupported();
            }
        }

        internal static void EnsureSupported()
        {
            if (IntPtr.Size != 8 || !BitConverter.IsLittleEndian)
            {
                throw Unsupported();
            }

            try
            {
                if (KernelCredentialInterop.kernel_credential_abi_version() != KernelCredentialInterop.AbiVersion)
                {
                    throw Unsupported();
                }
            }
            catch (Exception error) when (IsUnavailable(error))
            {
                throw Unsupported();
            }
        }

        [SuppressMessage("Maintainability", "CA1510", Justification = "Explicit null guards preserve net472 compatibility; ArgumentNullException.ThrowIfNull is unavailable on that target.")]
        internal static KernelCredentialRegistration Create(KernelAzureBearerCredentialOptions options)
        {
            if (options == null)
            {
                throw new ArgumentNullException(nameof(options));
            }

            EnsureSupported();
            ulong contextId = 0;
            KernelCredentialRegistration? registration = null;
            var published = false;
            var succeeded = false;
            try
            {
                if (KernelCredentialInterop.kernel_credential_context_new(out contextId) != 0 || contextId == 0)
                {
                    throw ControlFailure();
                }

                registration = new KernelCredentialRegistration(contextId, options);
                registration._root = GCHandle.Alloc(registration, GCHandleType.Normal);
                if (!Registrations.TryAdd(contextId, registration))
                {
                    throw ControlFailure();
                }

                registration._mapped = true;
                var descriptor = new KernelCredentialRegistrationV1
                {
                    AbiVersion = KernelCredentialInterop.AbiVersion,
                    StructSize = (uint)sizeof(KernelCredentialRegistrationV1),
                    ContextId = contextId,
                    CredentialKind = KernelCredentialInterop.AzureBearer,
                    AcquisitionTimeoutMs = checked((uint)(options.AcquisitionTimeout.Ticks / TimeSpan.TicksPerMillisecond)),
                    MaxTokenBytes = checked((uint)options.MaxTokenBytes),
                    Begin = Marshal.GetFunctionPointerForDelegate(BeginCallback),
                    Cancel = Marshal.GetFunctionPointerForDelegate(CancelCallback),
                    Released = Marshal.GetFunctionPointerForDelegate(ReleasedCallback),
                };
                if (KernelCredentialInterop.kernel_credential_register(&descriptor) != 0)
                {
                    throw ControlFailure();
                }

                published = true;
                if (KernelCredentialInterop.kernel_credential_activate(contextId) != 0)
                {
                    throw ControlFailure();
                }

                succeeded = true;
                return registration;
            }
            catch (Exception error) when (IsUnavailable(error))
            {
                throw Unsupported();
            }
            finally
            {
                if (!succeeded && contextId != 0)
                {
                    if (published)
                    {
                        registration!.Dispose();
                    }
                    else
                    {
                        try
                        {
                            _ = KernelCredentialInterop.kernel_credential_unregister(contextId);
                        }
                        catch (Exception error) when (IsUnavailable(error))
                        {
                        }
                        finally
                        {
                            registration?.ReleaseManagedRoot();
                        }
                    }
                }
            }
        }

        [SuppressMessage("Maintainability", "CA1510", Justification = "Explicit null guards preserve net472 compatibility; ArgumentNullException.ThrowIfNull is unavailable on that target.")]
        internal SharedExternEngine* CreateEngine(string tableLocation, IReadOnlyCollection<KeyValuePair<string, string>> storageOptions)
        {
            CheckAttachable();
            if (storageOptions == null)
            {
                throw new ArgumentNullException(nameof(storageOptions));
            }

            var count = storageOptions.Count;
            if (count < 0 || count > MaxOptionCount)
            {
                throw InputFailure();
            }

            using var buffers = new Utf8Buffers(count * 2 + 1);
            var tableUri = new KernelStringSlice
            {
                ptr = (sbyte*)buffers.Pin(tableLocation, 65536, false, false, out var tableLength),
                len = tableLength,
            };
            var nativeOptions = new KernelCredentialOptionV1[count];
            var index = 0;
            foreach (var option in storageOptions)
            {
                if (index >= count)
                {
                    throw InputFailure();
                }

                var key = buffers.Pin(option.Key, 1024, false, true, out var keyLength);
                var value = buffers.Pin(option.Value, 65536, true, true, out var valueLength);
                nativeOptions[index++] = new KernelCredentialOptionV1
                {
                    KeyUtf8 = key,
                    KeyLen = keyLength,
                    ValueUtf8 = value,
                    ValueLen = valueLength,
                };
            }

            if (index != count)
            {
                throw InputFailure();
            }

            CheckAttachable();
            try
            {
                fixed (KernelCredentialOptionV1* optionsPointer = nativeOptions)
                {
                    var result = KernelCredentialInterop.kernel_engine_new_with_credential_v1(
                        tableUri, optionsPointer, (ulong)count, ContextId, AllocateErrorPointer, 2, 0);
                    if (result.tag == ExternResultHandleSharedExternEngine_Tag.ErrHandleSharedExternEngine
                        && result.Anonymous.Anonymous2.err != null)
                    {
                        throw KernelException.FromEngineError(result.Anonymous.Anonymous2.err, null);
                    }

                    if (result.tag != ExternResultHandleSharedExternEngine_Tag.OkHandleSharedExternEngine
                        || result.Anonymous.Anonymous1.ok == null)
                    {
                        throw new InvalidOperationException("Kernel credential engine construction failed.");
                    }

                    return result.Anonymous.Anonymous1.ok;
                }
            }
            catch (Exception error) when (IsUnavailable(error))
            {
                throw Unsupported();
            }
        }

        internal void CompleteRequest(ulong requestId, uint status, AzureBearerToken? token)
        {
            byte[]? bytes = null;
            try
            {
                var result = new KernelCredentialResultV1
                {
                    AbiVersion = KernelCredentialInterop.AbiVersion,
                    StructSize = (uint)sizeof(KernelCredentialResultV1),
                    Status = status,
                    CredentialKind = KernelCredentialInterop.AzureBearer,
                };
                if (status == 0)
                {
                    KernelCredentialBootstrap.ValidateToken(token!, _options.MaxTokenBytes);
                    bytes = StrictUtf8.GetBytes(token!.Token);
                    result.TokenLen = checked((ulong)bytes.Length);
                    result.ExpiresUnixMs = token.ExpiresOn.ToUnixTimeMilliseconds();
                }

                fixed (byte* tokenPointer = bytes)
                {
                    result.TokenUtf8 = tokenPointer;
                    var completion = KernelCredentialInterop.kernel_credential_complete(ContextId, requestId, &result);
                    if (completion > 2) throw ControlFailure();
                }
            }
            finally
            {
                if (bytes != null)
                {
                    Array.Clear(bytes, 0, bytes.Length);
                }
            }
        }

        internal void RetireRequest(ulong requestId, KernelCredentialRequest request)
            => ((ICollection<KeyValuePair<ulong, KernelCredentialRequest>>)_requests)
                .Remove(new KeyValuePair<ulong, KernelCredentialRequest>(requestId, request));

        private static uint Begin(KernelCredentialRequestV1* pointer)
        {
            try
            {
                if (pointer == null || pointer->AbiVersion != KernelCredentialInterop.AbiVersion
                    || pointer->StructSize != sizeof(KernelCredentialRequestV1))
                {
                    return 3;
                }

                var request = *pointer;
                if (request.ContextId == 0 || request.RequestId == 0
                    || request.CredentialKind != KernelCredentialInterop.AzureBearer
                    || request.TimeoutMs == 0 || request.TimeoutMs > 120000
                    || request.MinimumLifetimeMs != MinimumLifetimeMs || request.Flags != 0
                    || request.Reserved0 != 0 || request.Reserved1 != 0)
                {
                    return 3;
                }

                if (!Registrations.TryGetValue(request.ContextId, out var registration))
                {
                    return 2;
                }

                return registration.BeginRequest(request);
            }
            catch (Exception)
            {
                return 4;
            }
        }

        private uint BeginRequest(KernelCredentialRequestV1 descriptor)
        {
            lock (_requestGate)
            {
                if (Volatile.Read(ref _nativeReleased) != 0)
                {
                    return 2;
                }

                if (descriptor.TimeoutMs > _options.AcquisitionTimeout.TotalMilliseconds)
                {
                    return 3;
                }

                if (_requests.Count >= 64)
                {
                    return 1;
                }

                var request = new KernelCredentialRequest(this, descriptor.RequestId, descriptor.TimeoutMs, _options);
                if (!_requests.TryAdd(descriptor.RequestId, request))
                {
                    request.DriverCompleted();
                    return 3;
                }

                try
                {
                    if (request.Queue())
                    {
                        return 0;
                    }
                }
                catch (Exception)
                {
                    request.DriverCompleted();
                    return 4;
                }

                request.DriverCompleted();
                return 1;
            }
        }

        private static void Cancel(ulong contextId, ulong requestId, uint reason)
        {
            try
            {
                if (reason >= 1 && reason <= 3 && Registrations.TryGetValue(contextId, out var registration)
                    && registration._requests.TryGetValue(requestId, out var request))
                {
                    request.QueueCancellation(reason);
                }
            }
            catch (Exception)
            {
            }
        }

        private static void NativeReleased(ulong contextId)
        {
            try
            {
                if (Registrations.TryGetValue(contextId, out var registration))
                {
                    registration.ReleaseManagedRoot();
                }
            }
            catch (Exception)
            {
            }
        }

        private void ReleaseManagedRoot()
        {
            lock (_requestGate)
            {
                if (Interlocked.Exchange(ref _nativeReleased, 1) != 0)
                {
                    return;
                }
            }

            if (_mapped)
            {
                Registrations.TryRemove(ContextId, out _);
            }

            if (_root.IsAllocated)
            {
                _root.Free();
            }

            _released.TrySetResult(true);
        }

        [SuppressMessage("Maintainability", "CA1513", Justification = "Explicit disposal guards preserve net472 compatibility; ObjectDisposedException.ThrowIf is unavailable on that target.")]
        private void CheckAttachable()
        {
            lock (_attachGate)
            {
                if (_disposed || Volatile.Read(ref _nativeReleased) != 0)
                {
                    throw new ObjectDisposedException(nameof(KernelCredentialRegistration));
                }
            }
        }

        private static bool IsUnavailable(Exception error)
            => error is DllNotFoundException || error is EntryPointNotFoundException || error is BadImageFormatException;

        private static NotSupportedException Unsupported()
            => new NotSupportedException("The loaded kernel library does not support credential ABI v1.");

        private static InvalidOperationException ControlFailure()
            => new InvalidOperationException("Kernel credential registration failed.");

        private static ArgumentException InputFailure()
            => new ArgumentException("Kernel credential engine input exceeds its limits or is invalid.");

        private sealed class Utf8Buffers : IDisposable
        {
            private readonly byte[]?[] _bytes;
            private readonly GCHandle[] _pins;
            private int _next;
            private int _optionBytes;

            internal Utf8Buffers(int capacity)
            {
                _bytes = new byte[capacity][];
                _pins = new GCHandle[capacity];
            }

            public void Dispose()
            {
                for (var index = 0; index < _bytes.Length; index++)
                {
                    if (_pins[index].IsAllocated)
                    {
                        _pins[index].Free();
                    }

                    var bytes = _bytes[index];
                    if (bytes != null)
                    {
                        Array.Clear(bytes, 0, bytes.Length);
                    }
                }
            }

            [SuppressMessage("Maintainability", "CA2249", Justification = "Character searches use IndexOf for net472 compatibility; string.Contains(char) is unavailable on that target.")]
            internal byte* Pin(string text, int maximum, bool allowEmpty, bool option, out ulong length)
            {
                if (text == null || (!allowEmpty && text.Length == 0) || text.Length > maximum || text.IndexOf('\0') >= 0)
                {
                    throw InputFailure();
                }

                int count;
                try
                {
                    count = StrictUtf8.GetByteCount(text);
                }
                catch (EncoderFallbackException)
                {
                    throw InputFailure();
                }

                if (count > maximum || (option && count > 1048576 - _optionBytes))
                {
                    throw InputFailure();
                }

                if (option)
                {
                    _optionBytes += count;
                }

                var index = _next++;
                var bytes = StrictUtf8.GetBytes(text);
                _bytes[index] = bytes;
                _pins[index] = GCHandle.Alloc(bytes, GCHandleType.Pinned);
                length = (ulong)bytes.Length;
                return (byte*)_pins[index].AddrOfPinnedObject();
            }
        }
    }

    [SuppressMessage("Design", "CA1001", Justification = "RetireIfFinished disposes the cancellation source only after the native request driver, provider execution and cancellation callbacks finish; exposing IDisposable could retire an in-flight request prematurely.")]
    internal sealed class KernelCredentialRequest
    {
        private static readonly WaitCallback RunCallback = state => ((KernelCredentialRequest)state!).StartDriver();
        private static readonly WaitCallback CancelCallback = state => ((KernelCredentialRequest)state!).Cancel();
        private readonly KernelCredentialRegistration _owner;
        private readonly ulong _requestId;
        private readonly uint _timeoutMs;
        private readonly KernelAzureBearerCredentialOptions _originalOptions;
        private readonly KernelAzureBearerCredentialOptions _requestOptions;
        private readonly object _gate = new object();
        private readonly CancellationTokenSource _cancellation = new CancellationTokenSource();
        private bool _driverCompleted;
        private bool _actualRunning;
        private bool _canceling;
        private bool _retired;
        private int _cancelReason;

        internal KernelCredentialRequest(KernelCredentialRegistration owner, ulong requestId, uint timeoutMs, KernelAzureBearerCredentialOptions options)
        {
            _owner = owner;
            _requestId = requestId;
            _timeoutMs = timeoutMs;
            _originalOptions = options;
            _requestOptions = new KernelAzureBearerCredentialOptions(
                new DelegateAzureBearerTokenProvider(RunProviderAsync), options.Scopes, options.AcquisitionTimeout, options.MaxTokenBytes);
        }

        internal bool Queue() => ThreadPool.UnsafeQueueUserWorkItem(RunCallback, this);

        internal static uint GetFailureStatus(Exception error)
            => error is KernelCredentialCapacityException ? 2u : 1u;

        internal void QueueCancellation(uint reason)
        {
            if (reason == 2)
            {
                Interlocked.Exchange(ref _cancelReason, 2);
            }
            else
            {
                Interlocked.CompareExchange(ref _cancelReason, (int)reason, 0);
            }

            ThreadPool.UnsafeQueueUserWorkItem(CancelCallback, this);
        }

        internal void DriverCompleted()
        {
            lock (_gate)
            {
                _driverCompleted = true;
                RetireIfFinished();
            }
        }

        private void StartDriver()
        {
            var driver = RunAsync();
            _ = driver.ContinueWith(task => { _ = task.Exception; }, CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
        }

        [SuppressMessage("Design", "CA1031", Justification = "User credential providers may throw arbitrary exceptions; the request driver must translate all failures to native status codes and complete managed retirement.")]
        private async Task RunAsync()
        {
            try
            {
                _cancellation.CancelAfter((int)_timeoutMs);
                var token = await KernelCredentialBootstrap.AcquireAsync(_requestOptions, _cancellation.Token).ConfigureAwait(false);
                _owner.CompleteRequest(_requestId, 0, token);
            }
            catch (TimeoutException)
            {
                CompleteFailure(4);
            }
            catch (OperationCanceledException)
            {
                var reason = Volatile.Read(ref _cancelReason);
                CompleteFailure(reason == 1 || reason == 3 ? 3u : 4u);
            }
            catch (Exception error)
            {
                CompleteFailure(GetFailureStatus(error));
            }
            finally
            {
                DriverCompleted();
            }
        }

        private async Task<AzureBearerToken> RunProviderAsync(AzureTokenRequestContext context, CancellationToken cancellationToken)
        {
            lock (_gate)
            {
                if (_driverCompleted)
                {
                    throw new OperationCanceledException(cancellationToken);
                }

                _actualRunning = true;
            }

            try
            {
                var actual = _originalOptions.Provider.GetTokenAsync(context, cancellationToken);
                if (actual == null)
                {
                    throw new InvalidOperationException("Credential acquisition failed.");
                }

                return await actual.ConfigureAwait(false);
            }
            finally
            {
                lock (_gate)
                {
                    _actualRunning = false;
                    RetireIfFinished();
                }
            }
        }

        [SuppressMessage("Design", "CA1031", Justification = "Failure reporting is best-effort during native request retirement; reporting exceptions must not prevent driver completion and resource retirement.")]
        private void CompleteFailure(uint status)
        {
            try
            {
                _owner.CompleteRequest(_requestId, status, null);
            }
            catch (Exception)
            {
            }
        }

        [SuppressMessage("Design", "CA1031", Justification = "Cancellation invokes arbitrary user provider callbacks; all callback exceptions must be contained so request retirement still completes.")]
        private void Cancel()
        {
            lock (_gate)
            {
                if (_retired || _canceling)
                {
                    return;
                }

                _canceling = true;
            }

            try
            {
                _cancellation.Cancel();
            }
            catch (Exception)
            {
            }
            finally
            {
                lock (_gate)
                {
                    _canceling = false;
                    RetireIfFinished();
                }
            }
        }

        private void RetireIfFinished()
        {
            if (_driverCompleted && !_actualRunning && !_canceling && !_retired)
            {
                _retired = true;
                _cancellation.Dispose();
                _owner.RetireRequest(_requestId, this);
            }
        }
    }
}