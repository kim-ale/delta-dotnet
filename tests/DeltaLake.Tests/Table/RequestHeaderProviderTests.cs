using System.Collections.Concurrent;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;
using DeltaLake.Http;
using DeltaLake.Table;

namespace DeltaLake.Tests.Table;

[CollectionDefinition("Request header dispatch", DisableParallelization = true)]
public class RequestHeaderDispatchCollection { }

[Collection("Request header dispatch")]
public class RequestHeaderProviderTests
{
    private static TaskCompletionSource<TValue> Signal<TValue>() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    private static async Task<TValue> WithinDeadline<TValue>(Task<TValue> task)
    {
        Assert.Same(task, await Task.WhenAny(task, Task.Delay(TimeSpan.FromSeconds(15))));
        return await task;
    }

    private static async Task WithinDeadline(Task task)
    {
        Assert.Same(task, await Task.WhenAny(task, Task.Delay(TimeSpan.FromSeconds(15))));
        await task;
    }

    [Fact]
    public void GivenPublicContract_WhenInspected_ContextIsImmutableAndTyped()
    {
        var method = typeof(IStorageRequestHeaderProvider).GetMethod("GetHeadersAsync")!;
        Assert.Equal(typeof(Task<IReadOnlyDictionary<string, string>>), method.ReturnType);
        Assert.Equal(new[] { typeof(StorageRequestContext), typeof(CancellationToken) },
            method.GetParameters().Select(parameter => parameter.ParameterType));
        Assert.Empty(typeof(StorageRequestContext).GetConstructors());
        Assert.All(typeof(StorageRequestContext).GetProperties(), property => Assert.Null(property.SetMethod));
        var context = new StorageRequestContext("GET", new Uri("https://example.test/container/object"));
        Assert.Equal("GET", context.Method);
        Assert.Equal(typeof(Uri), context.RequestUri.GetType());
    }

    [Fact]
    public void GivenNoProvider_WhenCaptured_OriginalOptionsAndStringsArePreserved()
    {
        var options = new TableOptions { TableLocation = "memory:///table", StorageOptions = { ["arbitrary"] = "unchanged" } };
        Assert.Null(options.RequestHeaderProvider);
        Assert.Same(options, StorageOptionsSnapshot.Capture(options));
        Assert.Equal("unchanged", options.StorageOptions["arbitrary"]);
        var create = new TableCreateOptions("memory:///table", new Apache.Arrow.Schema.Builder().Build());
        Assert.Same(create, StorageOptionsSnapshot.Capture(create));
    }

    [Theory]
    [InlineData("file:///table")]
    [InlineData("memory:///table")]
    [InlineData("s3://bucket/table")]
    [InlineData("https://account.blob.core.windows.net/container/table")]
    [InlineData("relative/table")]
    public void GivenUnsupportedScheme_WhenCaptured_RejectsWithoutCallback(string location)
    {
        var provider = new Provider((_, _) => throw new InvalidOperationException("Provider must not execute."));
        Assert.Throws<NotSupportedException>(() => StorageOptionsSnapshot.Capture(
            new TableOptions { TableLocation = location, RequestHeaderProvider = provider }));
        Assert.Throws<NotSupportedException>(() => StorageOptionsSnapshot.Capture(
            new TableCreateOptions(location, new Apache.Arrow.Schema.Builder().Build()) { RequestHeaderProvider = provider }));
        Assert.Equal(0, provider.Calls);
    }

    [Theory]
    [InlineData("az")]
    [InlineData("adl")]
    [InlineData("azure")]
    [InlineData("abfs")]
    [InlineData("abfss")]
    public void GivenSupportedScheme_WhenCaptured_SnapshotIsStableAndShared(string scheme)
    {
        var provider = new Provider((_, _) => Task.FromResult<IReadOnlyDictionary<string, string>>(new Dictionary<string, string>()));
        var options = new TableOptions
        {
            TableLocation = scheme + "://container/table", RequestHeaderProvider = provider,
            Version = 42, WithoutFiles = true, LogBufferSize = 8, StorageOptions = { ["key"] = "original" }
        };
        var snapshot = StorageOptionsSnapshot.Capture(options);
        options.StorageOptions["key"] = "mutated";
        options.Version = 99;
        options.WithoutFiles = false;
        options.LogBufferSize = 64;
        Assert.Equal("original", snapshot.StorageOptions["key"]);
        Assert.Equal(42UL, snapshot.Version);
        Assert.True(snapshot.WithoutFiles);
        Assert.Equal(8U, snapshot.LogBufferSize);
        Assert.Same(provider, snapshot.RequestHeaderProvider);
        Assert.Same(snapshot, StorageOptionsSnapshot.Capture(snapshot));
    }

    [Fact]
    public void GivenCreateOptions_WhenCaptured_AllMutableCollectionsAreCopied()
    {
        var provider = new Provider((_, _) => Task.FromResult<IReadOnlyDictionary<string, string>>(new Dictionary<string, string>()));
        var original = new TableCreateOptions("az://container/table", new Apache.Arrow.Schema.Builder().Build())
        {
            RequestHeaderProvider = provider, Name = "original", Description = "original", SaveMode = SaveMode.Append,
            StorageOptions = { ["storage"] = "original" }, PartitionBy = { "partition" },
            Configuration = new Dictionary<string, string> { ["config"] = "original" },
            CustomMetadata = new Dictionary<string, string> { ["metadata"] = "original" }
        };
        var snapshot = StorageOptionsSnapshot.Capture(original);
        original.Name = "mutated";
        original.Description = "mutated";
        original.SaveMode = SaveMode.Overwrite;
        original.StorageOptions.Clear();
        original.PartitionBy.Clear();
        original.Configuration.Clear();
        original.CustomMetadata.Clear();
        Assert.Equal("original", snapshot.Name);
        Assert.Equal("original", snapshot.Description);
        Assert.Equal(SaveMode.Append, snapshot.SaveMode);
        Assert.Single(snapshot.StorageOptions);
        Assert.Single(snapshot.PartitionBy);
        Assert.Single(snapshot.Configuration!);
        Assert.Single(snapshot.CustomMetadata!);
        Assert.Same(snapshot, StorageOptionsSnapshot.Capture(snapshot));
    }

    [Fact]
    public void GivenHostileProviderFormatting_WhenPrintingRecords_ProviderIsRedacted()
    {
        var provider = new HostileProvider();
        TableStorageOptions[] options =
        {
            new TableStorageOptions { RequestHeaderProvider = provider },
            new TableOptions { RequestHeaderProvider = provider },
            new TableCreateOptions("az://container/table", new Apache.Arrow.Schema.Builder().Build()) { RequestHeaderProvider = provider }
        };
        foreach (var option in options)
        {
            Assert.Contains("RequestHeaderProvider = <configured>", option.ToString());
            Assert.DoesNotContain("provider-secret", option.ToString());
        }
        Assert.Equal(0, provider.FormattingCalls);
    }

    [Fact]
    public void GivenCallbackDescriptor_WhenMarshaled_LayoutMatchesVersionOne()
    {
        Assert.Equal(0, Marshal.OffsetOf<HeaderCallbacks>(nameof(HeaderCallbacks.AbiVersion)).ToInt32());
        Assert.Equal(4, Marshal.OffsetOf<HeaderCallbacks>(nameof(HeaderCallbacks.StructSize)).ToInt32());
        Assert.Equal(8, Marshal.OffsetOf<HeaderCallbacks>(nameof(HeaderCallbacks.Context)).ToInt32());
        Assert.Equal(16, Marshal.OffsetOf<HeaderCallbacks>(nameof(HeaderCallbacks.Begin)).ToInt32());
        Assert.Equal(16 + IntPtr.Size, Marshal.OffsetOf<HeaderCallbacks>(nameof(HeaderCallbacks.Cancel)).ToInt32());
        Assert.Equal(16 + 2 * IntPtr.Size, Marshal.OffsetOf<HeaderCallbacks>(nameof(HeaderCallbacks.Released)).ToInt32());
        Assert.Equal(2 * IntPtr.Size, Marshal.SizeOf<ByteSlice>());
        if (IntPtr.Size == 8) Assert.Equal(40, Marshal.SizeOf<HeaderCallbacks>());
        Assert.All(typeof(NativeMethods).GetMethods(BindingFlags.NonPublic | BindingFlags.Static), method =>
        {
            var import = method.GetCustomAttribute<DllImportAttribute>();
            Assert.NotNull(import);
            Assert.Equal(CallingConvention.Cdecl, import!.CallingConvention);
            Assert.True(import.ExactSpelling);
        });
    }

    [Fact]
    public void GivenKernelBooleanResult_WhenMarshaled_UnionUsesBlittableByteLayout()
    {
        Assert.Equal(IntPtr.Size == 8 ? 16 : 8,
            Marshal.SizeOf<DeltaLake.Kernel.Interop.BooleanResultMethods.BooleanResult>());
        Assert.Equal(IntPtr.Size,
            Marshal.OffsetOf<DeltaLake.Kernel.Interop.BooleanResultMethods.BooleanResult>("Value").ToInt32());
        Assert.Equal(IntPtr.Size, Marshal.SizeOf<DeltaLake.Kernel.Interop.BooleanResultMethods.Payload>());
    }

    [Theory]
    [InlineData("Authorization")]
    [InlineData("HOST")]
    [InlineData("Cookie")]
    [InlineData("Content-Length")]
    [InlineData("Transfer-Encoding")]
    [InlineData("Connection")]
    [InlineData("Upgrade")]
    [InlineData("Proxy-Anything")]
    [InlineData("X-MS-Date")]
    [InlineData("x-ms-version")]
    [InlineData("bad name")]
    [InlineData("")]
    public void GivenControlledOrInvalidName_WhenSerialized_RejectsSanitized(string name)
    {
        var error = Assert.Throws<InvalidOperationException>(() => HeaderDispatcher.Serialize(
            new Dictionary<string, string> { [name] = "header-secret" }));
        Assert.DoesNotContain("header-secret", error.ToString());
    }

    [Theory]
    [InlineData("bad\r\ninjected:yes")]
    [InlineData("bad\0value")]
    [InlineData("bad\u007fvalue")]
    public void GivenInvalidValue_WhenSerialized_Rejects(string value)
    {
        Assert.Throws<InvalidOperationException>(() => HeaderDispatcher.Serialize(new Dictionary<string, string> { ["x-test"] = value }));
    }

    [Fact]
    public void GivenHeaderBounds_WhenSerialized_EnforcesEveryLimitAndDuplicates()
    {
        var dictionary = new Dictionary<string, string> { [new string('a', 128)] = new string('b', 8192) };
        Assert.NotEmpty(HeaderDispatcher.Serialize(dictionary));
        Assert.Throws<InvalidOperationException>(() => HeaderDispatcher.Serialize(new Dictionary<string, string> { [new string('a', 129)] = "value" }));
        Assert.Throws<InvalidOperationException>(() => HeaderDispatcher.Serialize(new Dictionary<string, string> { ["x"] = new string('b', 8193) }));
        Assert.Throws<InvalidOperationException>(() => HeaderDispatcher.Serialize(new Dictionary<string, string> { ["X"] = "first", ["x"] = "second" }));
        Assert.Throws<InvalidOperationException>(() => HeaderDispatcher.Serialize(new Dictionary<string, string> { ["x"] = null! }));
        Assert.Throws<InvalidOperationException>(() => HeaderDispatcher.Serialize(null!));
        Assert.Throws<EncoderFallbackException>(() => HeaderDispatcher.Serialize(new Dictionary<string, string> { ["x"] = "\ud800" }));
        var entries = Enumerable.Range(0, 32).ToDictionary(index => "x-" + index, _ => "value");
        Assert.NotEmpty(HeaderDispatcher.Serialize(entries));
        entries.Add("overflow", "value");
        Assert.Throws<InvalidOperationException>(() => HeaderDispatcher.Serialize(entries));
        var oversized = Enumerable.Range(0, 9).ToDictionary(index => "x-" + index, _ => new string('a', 8192));
        Assert.Throws<InvalidOperationException>(() => HeaderDispatcher.Serialize(oversized));
        Assert.Equal("{}", Encoding.UTF8.GetString(HeaderDispatcher.Serialize(new Dictionary<string, string>())));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task GivenModule_WhenInvoked_QueuesFreshRequestsWithoutExecutionContext(bool kernel)
    {
        var module = kernel ? NativeHeaderModule.Kernel : NativeHeaderModule.Bridge;
        var ambient = new AsyncLocal<string>();
        var contexts = new ConcurrentQueue<StorageRequestContext>();
        var provider = new Provider((context, _) =>
        {
            Assert.Null(ambient.Value);
            contexts.Enqueue(context);
            return Task.FromResult<IReadOnlyDictionary<string, string>>(new Dictionary<string, string> { ["x-fresh"] = contexts.Count.ToString() });
        });
        using var fixture = new Fixture(module, provider);
        ambient.Value = "caller-context";
        Assert.Equal(0U, fixture.Begin(1));
        var first = await WithinDeadline(fixture.Completion(1));
        Assert.Equal(0U, fixture.Begin(2));
        var second = await WithinDeadline(fixture.Completion(2));
        Assert.Equal(0U, first.Status);
        Assert.Equal(0U, second.Status);
        Assert.NotEqual(first.Json, second.Json);
        Assert.All(contexts, context =>
        {
            Assert.Equal("GET", context.Method);
            Assert.Equal("https://account.test/container/object", context.RequestUri.AbsoluteUri);
        });
        Assert.Equal(1U, HeaderDispatcher.Begin(module == NativeHeaderModule.Bridge ? NativeHeaderModule.Kernel : NativeHeaderModule.Bridge,
            fixture.Context, 3, default, default));
        Assert.Equal(2, provider.Calls);
        Assert.False(provider.Disposed);
    }

    [Theory]
    [InlineData("throw")]
    [InlineData("null-task")]
    [InlineData("fault")]
    [InlineData("unrelated-cancel")]
    [InlineData("null-dictionary")]
    [InlineData("forbidden")]
    public async Task GivenProviderFailure_WhenDispatched_CompletesNumericFailureOnly(string mode)
    {
        var provider = new Provider((_, _) => mode switch
        {
            "throw" => throw new InvalidOperationException("exception-secret"),
            "null-task" => null!,
            "fault" => Task.FromException<IReadOnlyDictionary<string, string>>(new InvalidOperationException("exception-secret")),
            "unrelated-cancel" => Task.FromCanceled<IReadOnlyDictionary<string, string>>(new CancellationToken(true)),
            "null-dictionary" => Task.FromResult<IReadOnlyDictionary<string, string>>(null!),
            _ => Task.FromResult<IReadOnlyDictionary<string, string>>(new Dictionary<string, string> { ["Authorization"] = "header-secret" })
        });
        using var fixture = new Fixture(NativeHeaderModule.Bridge, provider);
        Assert.Equal(0U, fixture.Begin(1));
        var completion = await WithinDeadline(fixture.Completion(1));
        Assert.Equal(1U, completion.Status);
        Assert.Null(completion.Json);
    }

    [Fact]
    public void GivenFailedNativeRegistration_WhenRejected_RollsBackWithoutReleaseObligation()
    {
        HeaderCallbacks callbacks = default;
        var unregisters = 0;
        var provider = new HostileProvider();
        Assert.Throws<InvalidOperationException>(() => HeaderDispatcher.Register(NativeHeaderModule.Bridge, provider,
            descriptor => { callbacks = descriptor; return 3; }, _ => unregisters++, (_, _, _, _) => { }));
        Assert.NotEqual(0UL, callbacks.Context);
        Assert.Equal(1U, HeaderDispatcher.Begin(NativeHeaderModule.Bridge, callbacks.Context, 1, default, default));
        Assert.Equal(0, unregisters);
    }

    [Fact]
    public async Task GivenRegistration_WhenManagementRetires_NativeOwnerCanStillInvoke()
    {
        var provider = new Provider((_, _) => Task.FromResult<IReadOnlyDictionary<string, string>>(new Dictionary<string, string>()));
        using var fixture = new Fixture(NativeHeaderModule.Kernel, provider, releaseOnUnregister: false);
        fixture.Unregister();
        Assert.Equal(0U, fixture.Begin(1));
        Assert.Equal(0U, (await WithinDeadline(fixture.Completion(1))).Status);
        fixture.Release();
        Assert.Equal(1U, fixture.Begin(2));
        Assert.False(provider.Disposed);
    }

    [Fact]
    public async Task GivenDuplicateRequest_WhenRejected_OriginalWorkerIsNotRemoved()
    {
        var result = Signal<IReadOnlyDictionary<string, string>>();
        var started = Signal<bool>();
        using var fixture = new Fixture(NativeHeaderModule.Bridge, new Provider((_, token) =>
        {
            token.Register(() => result.TrySetCanceled());
            started.TrySetResult(true);
            return result.Task;
        }));
        Assert.Equal(0U, fixture.Begin(1));
        await WithinDeadline(started.Task);
        Assert.Equal(1U, fixture.Begin(1));
        fixture.Cancel(1);
        var completion = await WithinDeadline(fixture.Completion(1));
        Assert.Equal(1U, completion.Status);
    }

    [Fact]
    public async Task GivenThrowingCancellationCallback_WhenCanceled_WorkerSurvivesNativeRelease()
    {
        var started = Signal<bool>();
        var canceled = Signal<bool>();
        var result = Signal<IReadOnlyDictionary<string, string>>();
        var provider = new Provider((_, token) =>
        {
            token.Register(() => { canceled.TrySetResult(true); throw new InvalidOperationException("cancel-secret"); });
            started.TrySetResult(true);
            return result.Task;
        });
        using var fixture = new Fixture(NativeHeaderModule.Bridge, provider);
        Assert.Equal(0U, fixture.Begin(1));
        await WithinDeadline(started.Task);
        fixture.Cancel(1);
        fixture.Release();
        await WithinDeadline(canceled.Task);
        result.TrySetResult(new Dictionary<string, string>());
        Assert.Equal(1U, (await WithinDeadline(fixture.Completion(1))).Status);
        Assert.False(provider.Disposed);
    }

    [Fact]
    public async Task GivenUncooperativeProvider_WhenNativeReleases_LocalWorkerSlotsRemainBounded()
    {
        var result = Signal<IReadOnlyDictionary<string, string>>();
        using var started = new SemaphoreSlim(0);
        var provider = new Provider((_, _) => { started.Release(); return result.Task; });
        using var fixture = new Fixture(NativeHeaderModule.Bridge, provider);
        try
        {
            for (ulong request = 1; request <= 16; request++)
            {
                Assert.Equal(0U, fixture.Begin(request));
                await WithinDeadline(started.WaitAsync());
            }
            Assert.Equal(0U, fixture.Begin(17));
            Assert.Equal(1U, (await WithinDeadline(fixture.Completion(17))).Status);
            fixture.Release();
            for (ulong request = 1; request <= 16; request++) fixture.Cancel(request);
            Assert.Equal(16, provider.Calls);
        }
        finally
        {
            result.TrySetResult(new Dictionary<string, string>());
            await WithinDeadline(Task.WhenAll(Enumerable.Range(1, 16).Select(request => fixture.Completion((ulong)request))));
        }
    }

    [Fact]
    public async Task GivenGlobalCapacity_WhenExceeded_RejectsWithoutCallingAdditionalProvider()
    {
        var result = Signal<IReadOnlyDictionary<string, string>>();
        using var started = new SemaphoreSlim(0);
        var provider = new Provider((_, _) => { started.Release(); return result.Task; });
        var fixtures = Enumerable.Range(0, 5).Select(_ => new Fixture(NativeHeaderModule.Kernel, provider)).ToArray();
        try
        {
            foreach (var fixture in fixtures.Take(4))
            {
                for (ulong request = 1; request <= 16; request++)
                {
                    Assert.Equal(0U, fixture.Begin(request));
                    await WithinDeadline(started.WaitAsync());
                }
            }
            Assert.Equal(0U, fixtures[4].Begin(1));
            Assert.Equal(1U, (await WithinDeadline(fixtures[4].Completion(1))).Status);
            Assert.Equal(64, provider.Calls);
        }
        finally
        {
            result.TrySetResult(new Dictionary<string, string>());
            await WithinDeadline(Task.WhenAll(fixtures.Take(4).SelectMany(fixture =>
                Enumerable.Range(1, 16).Select(request => fixture.Completion((ulong)request)))));
            foreach (var fixture in fixtures) fixture.Dispose();
        }
    }

    [Fact]
    public async Task GivenCancellationCompletionRaces_WhenNativeReleases_AllWorkersRetire()
    {
        for (var iteration = 0; iteration < 64; iteration++)
        {
            var result = Signal<IReadOnlyDictionary<string, string>>();
            var started = Signal<bool>();
            using var fixture = new Fixture(iteration % 2 == 0 ? NativeHeaderModule.Bridge : NativeHeaderModule.Kernel,
                new Provider((_, token) =>
                {
                    token.Register(() => { throw new InvalidOperationException("cancel-secret"); });
                    started.TrySetResult(true);
                    return result.Task;
                }));
            Assert.Equal(0U, fixture.Begin(1));
            await WithinDeadline(started.Task);
            await Task.WhenAll(Task.Run(() => fixture.Cancel(1)), Task.Run(() =>
            {
                fixture.Release();
                result.TrySetResult(new Dictionary<string, string> { ["x-test"] = "value" });
            }));
            var completion = await WithinDeadline(fixture.Completion(1));
            Assert.True(completion.Status == 0 || completion.Status == 1);
            Assert.Equal(1U, fixture.Begin(2));
        }
    }

    [Fact]
    public void GivenMalformedBorrowedStrings_WhenBegun_RejectsBeforeCallingProvider()
    {
        var provider = new HostileProvider();
        using var fixture = new Fixture(NativeHeaderModule.Bridge, provider);
        Assert.Equal(1U, fixture.Begin(1, new byte[] { 255 }, Encoding.UTF8.GetBytes("https://account.test/object")));
        Assert.Equal(1U, fixture.Begin(2, Encoding.UTF8.GetBytes("GET"), new byte[HeaderDispatcher.MaximumBytes + 1]));
        Assert.Equal(1U, HeaderDispatcher.Begin(NativeHeaderModule.Bridge, fixture.Context, 3, default, default));
        Assert.Equal(1U, fixture.Begin(0));
    }

    [Fact]
    public void GivenSafeHandleLease_WhenOwnerDisposed_ReleaseWaitsForLeaseAndIsPaired()
    {
        var owner = new TestHandle();
        var scope = new DeltaLake.Bridge.Scope(owner);
        owner.Dispose();
        Assert.Equal(0, owner.Releases);
        scope.Dispose();
        scope.Dispose();
        Assert.Equal(1, owner.Releases);
        Assert.Throws<ObjectDisposedException>(() => new DeltaLake.Bridge.Scope(owner));
    }

    [Fact]
    public void GivenMultipleOwners_WhenLeaseAcquisitionFails_PreviousLeaseIsRolledBack()
    {
        var first = new TestHandle();
        var closed = new TestHandle();
        closed.Dispose();
        Assert.Throws<ObjectDisposedException>(() => new DeltaLake.Bridge.Scope(first, closed));
        first.Dispose();
        Assert.Equal(1, first.Releases);
        Assert.Equal(1, closed.Releases);
    }

    private sealed class TestHandle : SafeHandle
    {
        internal int Releases;
        internal TestHandle() : base(IntPtr.Zero, true) => SetHandle(new IntPtr(1));
        public override bool IsInvalid => handle == IntPtr.Zero;
        protected override bool ReleaseHandle() { Interlocked.Increment(ref Releases); return true; }
    }

    private sealed class Provider : IStorageRequestHeaderProvider, IDisposable
    {
        private readonly Func<StorageRequestContext, CancellationToken, Task<IReadOnlyDictionary<string, string>>> _get;
        internal int Calls;
        internal bool Disposed;
        internal Provider(Func<StorageRequestContext, CancellationToken, Task<IReadOnlyDictionary<string, string>>> get) => _get = get;
        public Task<IReadOnlyDictionary<string, string>> GetHeadersAsync(StorageRequestContext request, CancellationToken token)
        {
            Interlocked.Increment(ref Calls);
            return _get(request, token);
        }
        public void Dispose() => Disposed = true;
    }

    private sealed class HostileProvider : IStorageRequestHeaderProvider
    {
        internal int FormattingCalls;
        public Task<IReadOnlyDictionary<string, string>> GetHeadersAsync(StorageRequestContext request, CancellationToken token) =>
            throw new InvalidOperationException("Provider must not execute.");
        public override string ToString()
        {
            FormattingCalls++;
            throw new InvalidOperationException("provider-secret");
        }
    }

    private sealed class CompletionResult
    {
        internal uint Status { get; init; }
        internal string? Json { get; init; }
    }

    private sealed class Fixture : IDisposable
    {
        private readonly NativeHeaderModule _module;
        private readonly HeaderCallbacks _callbacks;
        private readonly NativeHeaderRegistration _registration;
        private readonly ConcurrentDictionary<ulong, TaskCompletionSource<CompletionResult>> _completed = new();
        private int _released;

        internal Fixture(NativeHeaderModule module, IStorageRequestHeaderProvider provider, bool releaseOnUnregister = true)
        {
            _module = module;
            HeaderCallbacks callbacks = default;
            _registration = HeaderDispatcher.Register(module, provider,
                descriptor => { callbacks = descriptor; return 0; },
                _ => { if (releaseOnUnregister) Release(); },
                (_, request, status, bytes) =>
                {
                    string? json = null;
                    if (status == 0)
                    {
                        var copy = new byte[checked((int)bytes.Length.ToUInt64())];
                        Marshal.Copy(bytes.Data, copy, 0, copy.Length);
                        json = Encoding.UTF8.GetString(copy);
                        using var document = JsonDocument.Parse(json);
                        Assert.Equal(JsonValueKind.Object, document.RootElement.ValueKind);
                    }
                    else
                    {
                        Assert.Equal(IntPtr.Zero, bytes.Data);
                        Assert.Equal(UIntPtr.Zero, bytes.Length);
                    }
                    _completed.GetOrAdd(request, _ => Signal<CompletionResult>()).TrySetResult(new CompletionResult { Status = status, Json = json });
                });
            _callbacks = callbacks;
        }

        internal ulong Context => _callbacks.Context;
        internal Task<CompletionResult> Completion(ulong request) => _completed.GetOrAdd(request, _ => Signal<CompletionResult>()).Task;

        internal uint Begin(ulong request) => Begin(request, Encoding.UTF8.GetBytes("GET"), Encoding.UTF8.GetBytes("https://account.test/container/object"));

        internal uint Begin(ulong request, byte[] method, byte[] uri)
        {
            var methodPin = GCHandle.Alloc(method, GCHandleType.Pinned);
            var uriPin = GCHandle.Alloc(uri, GCHandleType.Pinned);
            try
            {
                return Marshal.GetDelegateForFunctionPointer<HeaderBegin>(_callbacks.Begin)(Context, request,
                    new ByteSlice { Data = methodPin.AddrOfPinnedObject(), Length = new UIntPtr((uint)method.Length) },
                    new ByteSlice { Data = uriPin.AddrOfPinnedObject(), Length = new UIntPtr((uint)uri.Length) });
            }
            finally { methodPin.Free(); uriPin.Free(); }
        }

        internal void Cancel(ulong request) => Marshal.GetDelegateForFunctionPointer<HeaderCancel>(_callbacks.Cancel)(Context, request);
        internal void Unregister() => _registration.Dispose();
        internal void Release()
        {
            if (Interlocked.Exchange(ref _released, 1) == 0)
                Marshal.GetDelegateForFunctionPointer<HeaderReleased>(_callbacks.Released)(Context);
        }
        public void Dispose() { Unregister(); Release(); }
    }
}