using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using DeltaLake.Table;

namespace DeltaLake.Http
{
    internal static class HeaderDispatcher
    {
        internal const int MaximumBytes = 64 * 1024;
        private static readonly UTF8Encoding StrictUtf8 = new UTF8Encoding(false, true);
        private static readonly ConcurrentDictionary<(NativeHeaderModule, ulong), Registration> Registrations = new();
        private static readonly ConcurrentDictionary<(NativeHeaderModule, ulong, ulong), Request> Workers = new();
        private static readonly SemaphoreSlim QueueSlots = new(256, 256);
        private static readonly SemaphoreSlim WorkerSlots = new(64, 64);
        private static readonly HeaderBegin BridgeBegin = (context, request, method, uri) => Begin(NativeHeaderModule.Bridge, context, request, method, uri);
        private static readonly HeaderCancel BridgeCancel = (context, request) => Cancel(NativeHeaderModule.Bridge, context, request);
        private static readonly HeaderReleased BridgeReleased = context => Released(NativeHeaderModule.Bridge, context);
        private static readonly HeaderBegin KernelBegin = (context, request, method, uri) => Begin(NativeHeaderModule.Kernel, context, request, method, uri);
        private static readonly HeaderCancel KernelCancel = (context, request) => Cancel(NativeHeaderModule.Kernel, context, request);
        private static readonly HeaderReleased KernelReleased = context => Released(NativeHeaderModule.Kernel, context);
        private static readonly WaitCallback RunWorker = state => StartWorker((Request)state!);
        private static readonly WaitCallback RunCancellation = state => ((Request)state!).CancelWorker();
        private static readonly Action<Task> ObserveFault = task => GC.KeepAlive(task.Exception);
        private static long _nextContext;

        internal static NativeHeaderRegistration Register(NativeHeaderModule module, IStorageRequestHeaderProvider provider)
        {
            return module == NativeHeaderModule.Bridge
                ? Register(module, provider, callbacks => NativeMethods.bridge_headers_register(in callbacks),
                    NativeMethods.bridge_headers_unregister, (context, request, status, bytes) => NativeMethods.bridge_headers_complete(context, request, status, bytes))
                : Register(module, provider, callbacks => NativeMethods.kernel_headers_register(in callbacks),
                    NativeMethods.kernel_headers_unregister, (context, request, status, bytes) => NativeMethods.kernel_headers_complete(context, request, status, bytes));
        }

        internal static NativeHeaderRegistration Register(NativeHeaderModule module, IStorageRequestHeaderProvider provider,
            Func<HeaderCallbacks, uint> register, Action<ulong> unregister,
            Action<ulong, ulong, uint, ByteSlice> complete)
        {
            var context = NextContext();
            var state = new Registration(module, context, provider, complete);
            var callbacks = new HeaderCallbacks
            {
                AbiVersion = 1,
                StructSize = (uint)Marshal.SizeOf<HeaderCallbacks>(),
                Context = context,
                Begin = Marshal.GetFunctionPointerForDelegate(module == NativeHeaderModule.Bridge ? BridgeBegin : KernelBegin),
                Cancel = Marshal.GetFunctionPointerForDelegate(module == NativeHeaderModule.Bridge ? BridgeCancel : KernelCancel),
                Released = Marshal.GetFunctionPointerForDelegate(module == NativeHeaderModule.Bridge ? BridgeReleased : KernelReleased)
            };
            if (!Registrations.TryAdd((module, context), state)) throw new InvalidOperationException("Header context unavailable.");
            try
            {
                if (register(callbacks) != 0) throw new InvalidOperationException("Header registration failed.");
                state.Owned = true;
                return new NativeHeaderRegistration(context, unregister);
            }
            catch
            {
                Registrations.TryRemove((module, context), out _);
                throw;
            }
        }

        internal static uint Begin(NativeHeaderModule module, ulong context, ulong requestId, ByteSlice method, ByteSlice uri)
        {
            Request? request = null;
            var queued = false;
            var added = false;
            try
            {
                if (!Registrations.TryGetValue((module, context), out var registration) || !registration.Owned || requestId == 0) return 1;
                var requestContext = new StorageRequestContext(Copy(method), new Uri(Copy(uri), UriKind.Absolute));
                if (!QueueSlots.Wait(0)) return 1;
                queued = true;
                request = new Request(registration, requestId, requestContext);
                if (!Workers.TryAdd(request.Key, request)) return 1;
                added = true;
                if (!ThreadPool.UnsafeQueueUserWorkItem(RunWorker, request)) return 1;
                queued = false;
                return 0;
            }
            catch { return 1; }
            finally
            {
                if (queued)
                {
                    QueueSlots.Release();
                    if (request != null)
                    {
                        if (added) Workers.TryRemove(request.Key, out _);
                        request.Finish();
                    }
                }
            }
        }

        private static ulong NextContext()
        {
            while (true)
            {
                var previous = Volatile.Read(ref _nextContext);
                if (previous == long.MaxValue) throw new InvalidOperationException("Header context identifiers exhausted.");
                var next = previous + 1;
                if (Interlocked.CompareExchange(ref _nextContext, next, previous) == previous) return (ulong)next;
            }
        }

        private static string Copy(ByteSlice bytes)
        {
            var length = bytes.Length.ToUInt64();
            if (length == 0 || length > MaximumBytes || bytes.Data == IntPtr.Zero) throw new InvalidOperationException("Invalid header request.");
            var copy = new byte[(int)length];
            try
            {
                Marshal.Copy(bytes.Data, copy, 0, copy.Length);
                return StrictUtf8.GetString(copy);
            }
            finally { Array.Clear(copy, 0, copy.Length); }
        }

        internal static void Cancel(NativeHeaderModule module, ulong context, ulong request)
        {
            try
            {
                if (Workers.TryGetValue((module, context, request), out var worker)) worker.QueueCancellation();
            }
            catch { }
        }

        internal static void Released(NativeHeaderModule module, ulong context)
        {
            try { Registrations.TryRemove((module, context), out _); }
            catch { }
        }

        private static void StartWorker(Request request)
        {
            QueueSlots.Release();
            request.Task = ExecuteAsync(request);
            _ = request.Task.ContinueWith(ObserveFault, CancellationToken.None,
                TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
        }

        private static async Task ExecuteAsync(Request request)
        {
            var globalSlot = false;
            var localSlot = false;
            byte[]? bytes = null;
            try
            {
                globalSlot = TryAcquire(WorkerSlots);
                if (!globalSlot) return;
                localSlot = TryAcquire(request.Registration.Slots);
                if (!localSlot) return;
                request.Token.ThrowIfCancellationRequested();
                var task = request.Registration.Provider.GetHeadersAsync(request.Context, request.Token);
                if (task == null) return;
                var headers = await task.ConfigureAwait(false);
                request.Token.ThrowIfCancellationRequested();
                bytes = Serialize(headers);
            }
            catch { }
            finally
            {
                try { Submit(request, bytes); }
                catch { }
                finally
                {
                    if (bytes != null) Array.Clear(bytes, 0, bytes.Length);
                    if (localSlot) request.Registration.Slots.Release();
                    if (globalSlot) WorkerSlots.Release();
                    Workers.TryRemove(request.Key, out _);
                    request.Finish();
                }
            }
        }

        private static void Submit(Request request, byte[]? bytes)
        {
            if (bytes == null)
            {
                request.Registration.Complete(request.Registration.Context, request.Id, 1, default);
                return;
            }
            var pin = GCHandle.Alloc(bytes, GCHandleType.Pinned);
            try
            {
                request.Registration.Complete(request.Registration.Context, request.Id, 0,
                    new ByteSlice { Data = pin.AddrOfPinnedObject(), Length = new UIntPtr((uint)bytes.Length) });
            }
            finally { pin.Free(); }
        }

        private static bool TryAcquire(SemaphoreSlim slots) => slots.Wait(0);

        internal static byte[] Serialize(IReadOnlyDictionary<string, string> headers)
        {
            if (headers == null || headers.Count > 32) throw new InvalidOperationException("Invalid custom headers.");
            using var stream = new MemoryStream();
            try
            {
                using var writer = new Utf8JsonWriter(stream);
                var names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                writer.WriteStartObject();
                foreach (var header in headers)
                {
                    if (names.Count >= 32 || !ValidName(header.Key) || !names.Add(header.Key) ||
                        header.Value == null || StrictUtf8.GetByteCount(header.Value) > 8192) throw new InvalidOperationException("Invalid custom headers.");
                    foreach (var character in header.Value)
                    {
                        if ((character < 32 && character != '\t') || character == 127) throw new InvalidOperationException("Invalid custom headers.");
                    }
                    writer.WriteString(header.Key, header.Value);
                    writer.Flush();
                    if (stream.Length > MaximumBytes) throw new InvalidOperationException("Invalid custom headers.");
                }
                writer.WriteEndObject();
                writer.Flush();
                if (stream.Length > MaximumBytes) throw new InvalidOperationException("Invalid custom headers.");
                return stream.ToArray();
            }
            finally
            {
                var buffer = stream.GetBuffer();
                Array.Clear(buffer, 0, buffer.Length);
            }
        }

        private static bool ValidName(string name)
        {
            if (string.IsNullOrEmpty(name) || name.Length > 128) return false;
            foreach (var character in name)
            {
                if (!(character >= 'a' && character <= 'z') && !(character >= 'A' && character <= 'Z') &&
                    !(character >= '0' && character <= '9') && "!#$%&'*+-.^_`|~".IndexOf(character) < 0) return false;
            }
            return !name.Equals("authorization", StringComparison.OrdinalIgnoreCase) &&
                !name.Equals("host", StringComparison.OrdinalIgnoreCase) &&
                !name.Equals("cookie", StringComparison.OrdinalIgnoreCase) &&
                !name.Equals("content-length", StringComparison.OrdinalIgnoreCase) &&
                !name.Equals("transfer-encoding", StringComparison.OrdinalIgnoreCase) &&
                !name.Equals("connection", StringComparison.OrdinalIgnoreCase) &&
                !name.Equals("upgrade", StringComparison.OrdinalIgnoreCase) &&
                !name.Equals("x-ms-date", StringComparison.OrdinalIgnoreCase) &&
                !name.Equals("x-ms-version", StringComparison.OrdinalIgnoreCase) &&
                !name.StartsWith("proxy-", StringComparison.OrdinalIgnoreCase);
        }

        private sealed class Registration
        {
            internal readonly NativeHeaderModule Module;
            internal readonly ulong Context;
            internal readonly IStorageRequestHeaderProvider Provider;
            internal readonly Action<ulong, ulong, uint, ByteSlice> Complete;
            internal readonly SemaphoreSlim Slots = new(16, 16);
            internal volatile bool Owned;

            internal Registration(NativeHeaderModule module, ulong context, IStorageRequestHeaderProvider provider,
                Action<ulong, ulong, uint, ByteSlice> complete)
            {
                Module = module;
                Context = context;
                Provider = provider;
                Complete = complete;
            }
        }

        private sealed class Request
        {
            internal readonly Registration Registration;
            internal readonly ulong Id;
            internal readonly StorageRequestContext Context;
            private readonly CancellationTokenSource _source = new();
            private readonly object _gate = new();
            private bool _cancelQueued;
            private bool _finished;
            private int _cancelLeases;
            internal Task? Task;

            internal Request(Registration registration, ulong id, StorageRequestContext context)
            {
                Registration = registration;
                Id = id;
                Context = context;
                Token = _source.Token;
            }

            internal CancellationToken Token { get; }
            internal (NativeHeaderModule, ulong, ulong) Key => (Registration.Module, Registration.Context, Id);

            internal void QueueCancellation()
            {
                lock (_gate)
                {
                    if (_finished || _cancelQueued) return;
                    _cancelQueued = true;
                    _cancelLeases++;
                }
                try
                {
                    if (ThreadPool.UnsafeQueueUserWorkItem(RunCancellation, this)) return;
                }
                catch { }
                EndCancellation();
            }

            internal void CancelWorker()
            {
                try { _source.Cancel(); }
                catch { }
                finally { EndCancellation(); }
            }

            private void EndCancellation()
            {
                lock (_gate)
                {
                    _cancelLeases--;
                    if (_finished && _cancelLeases == 0) _source.Dispose();
                }
            }

            internal void Finish()
            {
                lock (_gate)
                {
                    _finished = true;
                    if (_cancelLeases == 0) _source.Dispose();
                }
            }
        }
    }
}