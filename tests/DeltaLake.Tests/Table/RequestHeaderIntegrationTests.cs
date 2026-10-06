using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Xml.Linq;
using DeltaLake.Http;
using DeltaLake.Interfaces;
using DeltaLake.Table;

namespace DeltaLake.Tests.Table;

[Collection("Request header dispatch")]
public class RequestHeaderIntegrationTests : IAsyncLifetime
{
    private const string ActorHeader = "x-integration-actor";
    private const string StorageToken = "integration-native-storage-token";
    private const string TokenHeader = "x-integration-token";
    private readonly List<BlobServer> _servers = new();

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        foreach (var server in _servers)
            await WithinDeadline(server.DisposeAsync());
    }

    private BlobServer StartServer(BlobStore? store = null)
    {
        var server = new BlobServer(store ?? new BlobStore());
        _servers.Add(server);
        return server;
    }

    private static TaskCompletionSource<TValue> Signal<TValue>() =>
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    private static async Task<TValue> WithinDeadline<TValue>(Task<TValue> task)
    {
        Assert.Same(task, await Task.WhenAny(task, Task.Delay(TimeSpan.FromSeconds(45))));
        return await task;
    }

    private static async Task WithinDeadline(Task task)
    {
        Assert.Same(task, await Task.WhenAny(task, Task.Delay(TimeSpan.FromSeconds(45))));
        await task;
    }

    private static Dictionary<string, string> StorageOptions(BlobServer server) => new()
    {
        ["account_name"] = "account",
        ["bearer_token"] = StorageToken,
        ["endpoint"] = server.Endpoint.AbsoluteUri.TrimEnd('/'),
        ["allow_http"] = "true",
        ["max_retries"] = "2"
    };

    private static HashSet<(NativeHeaderModule Module, ulong Context)> Registrations()
    {
        var registrations = typeof(HeaderDispatcher)
            .GetField("Registrations", BindingFlags.Static | BindingFlags.NonPublic)!.GetValue(null)!;
        var keys = (IEnumerable<(NativeHeaderModule, ulong)>)registrations.GetType()
            .GetProperty("Keys")!.GetValue(registrations)!;
        return new HashSet<(NativeHeaderModule, ulong)>(keys);
    }

    private static void AssertBothModules(HashSet<(NativeHeaderModule Module, ulong Context)> baseline)
    {
        var live = Registrations().Except(baseline).ToArray();
        Assert.Contains(live, registration => registration.Module == NativeHeaderModule.Bridge);
        Assert.Contains(live, registration => registration.Module == NativeHeaderModule.Kernel);
    }

    private static HashSet<(NativeHeaderModule Module, ulong Context, ulong Request)> Workers()
    {
        var workers = typeof(HeaderDispatcher)
            .GetField("Workers", BindingFlags.Static | BindingFlags.NonPublic)!.GetValue(null)!;
        var keys = (IEnumerable<(NativeHeaderModule, ulong, ulong)>)workers.GetType()
            .GetProperty("Keys")!.GetValue(workers)!;
        return new HashSet<(NativeHeaderModule, ulong, ulong)>(keys);
    }

    private static NativeHeaderModule RequestModule(StorageRequestContext context)
    {
        var workers = typeof(HeaderDispatcher)
            .GetField("Workers", BindingFlags.Static | BindingFlags.NonPublic)!.GetValue(null)!;
        var values = (System.Collections.IEnumerable)workers.GetType().GetProperty("Values")!.GetValue(workers)!;
        foreach (var worker in values)
        {
            var workerContext = worker.GetType().GetField("Context", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(worker);
            if (!ReferenceEquals(workerContext, context)) continue;
            var registration = worker.GetType().GetField("Registration", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(worker)!;
            return (NativeHeaderModule)registration.GetType().GetField("Module", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(registration)!;
        }
        throw new InvalidOperationException("No native header worker owns this provider request.");
    }

    private static async Task WaitForRetirementAsync(HashSet<(NativeHeaderModule Module, ulong Context)> registrations,
        HashSet<(NativeHeaderModule Module, ulong Context, ulong Request)> workers)
    {
        var deadline = Stopwatch.StartNew();
        while (Registrations().Except(registrations).Any() || Workers().Except(workers).Any())
        {
            Assert.True(deadline.Elapsed < TimeSpan.FromSeconds(45), "Native provider owners or workers did not retire.");
            await Task.Delay(10);
        }
    }

    private static async Task SeedSimpleTableAsync(BlobStore store)
    {
        var directory = TableIdentifier.SimpleTable.TablePath();
        var paths = Directory.GetFiles(directory, "*.parquet")
            .Concat(new[] { Path.Combine(directory, "_delta_log", "00000000000000000000.json") });
        foreach (var path in paths)
        {
            using var file = File.OpenRead(path);
            using var bytes = new MemoryStream();
            await file.CopyToAsync(bytes);
            var relative = path.Substring(directory.TrimEnd(Path.DirectorySeparatorChar).Length + 1).Replace('\\', '/');
            store.Blobs["table/" + relative] = bytes.ToArray();
        }
    }

    private static TableOptions LoadOptions(BlobServer server, IStorageRequestHeaderProvider provider) => new()
    {
        TableLocation = "az://container/table",
        RequestHeaderProvider = provider,
        StorageOptions = StorageOptions(server)
    };

    private static async Task<long> QueryRowsAsync(ITable table, CancellationToken token)
    {
        long rows = 0;
        await foreach (var batch in table.QueryAsync(new SelectQuery("SELECT * FROM source") { TableAlias = "source" }, token))
        {
            using (batch) rows += batch.Length;
        }
        return rows;
    }

    private static async Task<(int Status, IReadOnlyDictionary<string, string> Headers, byte[] Body)> SendFixtureRequestAsync(
        BlobServer server, string method, string path, byte[]? body = null, string? range = null)
    {
        using var client = new TcpClient();
        await client.ConnectAsync(server.Endpoint.Host, server.Endpoint.Port);
        using var stream = client.GetStream();
        body ??= Array.Empty<byte>();
        var uri = new Uri(server.Endpoint, path);
        var request = new StringBuilder(method).Append(' ').Append(uri.PathAndQuery).Append(" HTTP/1.1\r\nHost: ")
            .Append(uri.Authority).Append("\r\nConnection: close\r\nContent-Length: ")
            .Append(body.Length.ToString(CultureInfo.InvariantCulture)).Append("\r\n");
        if (range != null) request.Append("Range: ").Append(range).Append("\r\n");
        request.Append("\r\n");
        var bytes = Encoding.ASCII.GetBytes(request.ToString());
        await stream.WriteAsync(bytes, 0, bytes.Length);
        await stream.WriteAsync(body, 0, body.Length);
        var status = int.Parse((await BlobServer.ReadLineAsync(stream)).Split(' ')[1], CultureInfo.InvariantCulture);
        var headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        for (var line = await BlobServer.ReadLineAsync(stream); line.Length != 0; line = await BlobServer.ReadLineAsync(stream))
        {
            var separator = line.IndexOf(':');
            headers[line.Substring(0, separator)] = line.Substring(separator + 1).Trim();
        }
        var length = method == "HEAD" ? 0 : int.Parse(headers["Content-Length"], CultureInfo.InvariantCulture);
        return (status, headers, await BlobServer.ReadBytesAsync(stream, length));
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static (Task<ITable> Load, WeakReference Provider) StartWeakLoad(DeltaEngine engine, BlobServer server)
    {
        var provider = new TrackingProvider();
        return (engine.LoadTableAsync(LoadOptions(server, provider), CancellationToken.None), new WeakReference(provider));
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void ForceCollection()
    {
        GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, true);
        GC.WaitForPendingFinalizers();
        GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, true);
    }

    private static void AssertInstrumented(BlobServer server, TrackingProvider provider)
    {
        server.AssertHealthy();
        var requests = server.Requests.ToArray();
        Assert.NotEmpty(requests);
        Assert.Equal(requests.Length, requests.Select(request => request.Headers[TokenHeader]).Distinct().Count());
        var calls = provider.Calls.ToDictionary(call => call.Value, StringComparer.Ordinal);
        foreach (var request in requests)
        {
            Assert.True(request.Headers.TryGetValue(TokenHeader, out var value), "A storage request lacked provider headers.");
            Assert.Equal("loopback", request.Headers[ActorHeader]);
            Assert.True(request.Headers.TryGetValue("Authorization", out var authorization) &&
                authorization == "Bearer " + StorageToken, "Native storage Authorization was changed.");
            Assert.True(calls.TryGetValue(value!, out var call), "A storage request had an unknown provider token.");
            Assert.Equal(request.Method, call!.Context.Method);
            Assert.Equal(request.Uri, call.Context.RequestUri);
            Assert.StartsWith("/base/container", request.Uri.AbsolutePath);
        }
        Assert.Equal(requests.Length, calls.Count);
        Assert.Contains(provider.Calls, call => call.Module == NativeHeaderModule.Bridge);
        Assert.Contains(provider.Calls, call => call.Module == NativeHeaderModule.Kernel);
    }

    [Fact]
    public async Task GivenAsyncProviderAndRetry_WhenCreatingAndCheckpointing_BothImagesUseFreshHeaders()
    {
        var baseline = Registrations();
        var server = StartServer();
        server.FailuresRemaining = 1;
        var provider = new TrackingProvider();
        using var batch = TableHelpers.BuildBasicRecordBatch(0);
        using var engine = new DeltaEngine(EngineOptions.Default);
        using var table = await WithinDeadline(engine.CreateTableAsync(new TableCreateOptions("az://container/table", batch.Schema)
        {
            RequestHeaderProvider = provider,
            StorageOptions = StorageOptions(server)
        }, CancellationToken.None));

        AssertBothModules(baseline);
        Assert.True(server.Store.Blobs.ContainsKey("table/_delta_log/00000000000000000000.json"));
        Assert.Equal(0L, await WithinDeadline(QueryRowsAsync(table, CancellationToken.None)));
        var beforeCheckpoint = server.Requests.Count;
        await WithinDeadline(table.CheckpointAsync(CancellationToken.None));
        Assert.Equal(0UL, table.Version());
        Assert.Contains(server.Requests.Skip(beforeCheckpoint), request => request.Method == "PUT" &&
            request.Uri.AbsolutePath.EndsWith(".checkpoint.parquet", StringComparison.Ordinal));
        Assert.True(server.Store.Blobs.ContainsKey("table/_delta_log/_last_checkpoint"));
        AssertInstrumented(server, provider);
        var failed = Assert.Single(server.Requests, request => request.Status == 500);
        Assert.Contains(server.Requests, request => request.Sequence > failed.Sequence &&
            request.Method == failed.Method && request.Uri == failed.Uri && request.Status != 500 &&
            request.Headers[TokenHeader] != failed.Headers[TokenHeader]);
        Assert.False(provider.Disposed);
    }

    [Fact]
    public async Task GivenMutableOptions_WhenLoadingQueryingReadingAndCommitting_OneHandleRetainsOriginalAzureOptions()
    {
        var baseline = Registrations();
        var server = StartServer();
        var mutatedEndpoint = StartServer();
        await WithinDeadline(SeedSimpleTableAsync(server.Store));
        var entered = Signal<bool>();
        var gate = Signal<bool>();
        var provider = new TrackingProvider
        {
            BeforeHeaders = async (_, _) =>
            {
                entered.TrySetResult(true);
                await gate.Task;
            }
        };
        var options = LoadOptions(server, provider);
        using var engine = new DeltaEngine(EngineOptions.Default);
        var loading = engine.LoadTableAsync(options, CancellationToken.None);
        try
        {
            await WithinDeadline(entered.Task);
            Assert.Empty(server.Requests);
            options.StorageOptions["endpoint"] = mutatedEndpoint.Endpoint.AbsoluteUri.TrimEnd('/');
            options.StorageOptions["bearer_token"] = "mutated-storage-token";
            options.StorageOptions["account_name"] = "mutated-account";
            options.Version = 99;
            options.WithoutFiles = true;
        }
        finally { gate.TrySetResult(true); }
        using var table = await WithinDeadline(loading);
        AssertBothModules(baseline);
        Assert.Equal(0UL, table.Version());

        var beforeQuery = server.Requests.Count;
        var queriedRows = await WithinDeadline(QueryRowsAsync(table, CancellationToken.None));
        Assert.True(queriedRows > 0);
        Assert.Contains(server.Requests.Skip(beforeQuery), request => request.Method == "GET" &&
            request.Uri.AbsolutePath.EndsWith(".parquet", StringComparison.Ordinal));
        var beforeRead = server.Requests.Count;
        using (var materialized = await WithinDeadline(table.ReadAsArrowTableAsync(CancellationToken.None)))
            Assert.Equal(queriedRows, materialized.Table.RowCount);
        Assert.Contains(server.Requests.Skip(beforeRead), request => request.Method == "GET" && request.Status == 206 &&
            request.Uri.AbsolutePath.EndsWith(".parquet", StringComparison.Ordinal));

        var existing = server.Store.Blobs.First(blob => blob.Key.EndsWith(".parquet", StringComparison.Ordinal) && blob.Value.Length > 262);
        var version = await WithinDeadline(table.CreateWriteTransactionAsync(new[]
        {
            new AddAction
            {
                Path = existing.Key.Substring("table/".Length), Size = existing.Value.Length,
                ModificationTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), DataChange = true
            }
        }, CancellationToken.None));
        Assert.Equal(1L, version);
        Assert.Equal(1UL, table.Version());
        await WithinDeadline(table.CheckpointAsync(CancellationToken.None));
        Assert.Contains(server.Requests, request => request.Method == "PUT" && request.Body.Length > 0 &&
            request.Uri.AbsolutePath.EndsWith("00000000000000000001.json", StringComparison.Ordinal));
        Assert.True(server.Store.Blobs.ContainsKey("table/_delta_log/00000000000000000001.checkpoint.parquet"));
        AssertInstrumented(server, provider);
        Assert.Empty(mutatedEndpoint.Requests);
        mutatedEndpoint.AssertHealthy();
    }

    [Fact]
    public async Task GivenAcceptedCrossOrigin307_WhenReadingAndCheckpointing_RedirectedHopsReuseOriginalHeaders()
    {
        var baseline = Registrations();
        var store = new BlobStore();
        var origin = StartServer(store);
        var target = StartServer(store);
        origin.RedirectOrigin = new Uri(target.Endpoint.GetLeftPart(UriPartial.Authority) + "/");
        origin.RedirectReadsOnly = true;
        Assert.NotEqual(origin.Endpoint.Port, target.Endpoint.Port);
        var provider = new TrackingProvider();
        using var batch = TableHelpers.BuildBasicRecordBatch(0);
        using var engine = new DeltaEngine(EngineOptions.Default);
        using var table = await WithinDeadline(engine.CreateTableAsync(new TableCreateOptions("az://container/table", batch.Schema)
        {
            RequestHeaderProvider = provider,
            StorageOptions = StorageOptions(origin)
        }, CancellationToken.None));
        AssertBothModules(baseline);
        await WithinDeadline(table.CheckpointAsync(CancellationToken.None));
        Assert.Equal(0UL, table.Version());
        AssertInstrumented(origin, provider);
        target.AssertHealthy();
        var redirectedRequests = origin.Requests.Where(request => request.Status == 307).ToArray();
        Assert.NotEmpty(redirectedRequests);
        Assert.Equal(redirectedRequests.Length, target.Requests.Count);
        Assert.All(provider.Calls, call => Assert.Equal(origin.Endpoint.Port, call.Context.RequestUri.Port));
        foreach (var request in redirectedRequests)
        {
            Assert.Equal(307, request.Status);
            var redirected = Assert.Single(target.Requests, candidate => candidate.Headers[TokenHeader] == request.Headers[TokenHeader]);
            Assert.Equal(request.Method, redirected.Method);
            Assert.Equal(request.Uri.PathAndQuery, redirected.Uri.PathAndQuery);
            Assert.Equal(request.Headers[ActorHeader], redirected.Headers[ActorHeader]);
            Assert.Equal(request.Body, redirected.Body);
            Assert.False(redirected.Headers.ContainsKey("Authorization"), "Stock redirect stripping of native Authorization changed.");
        }
        Assert.True(store.Blobs.ContainsKey("table/_delta_log/00000000000000000000.json"));
        Assert.True(store.Blobs.ContainsKey("table/_delta_log/00000000000000000000.checkpoint.parquet"));
    }

    [Fact]
    public async Task GivenStreamedPutRedirect_WhenCreating_StockClientDoesNotReplayBody()
    {
        var origin = StartServer();
        var target = StartServer(origin.Store);
        origin.RedirectOrigin = new Uri(target.Endpoint.GetLeftPart(UriPartial.Authority) + "/");
        using var batch = TableHelpers.BuildBasicRecordBatch(0);
        using var engine = new DeltaEngine(EngineOptions.Default);
        var error = await Assert.ThrowsAsync<DeltaLake.Errors.DeltaRuntimeException>(() =>
            WithinDeadline(engine.CreateTableAsync(new TableCreateOptions("az://container/table", batch.Schema)
            {
                RequestHeaderProvider = new TrackingProvider(),
                StorageOptions = StorageOptions(origin)
            }, CancellationToken.None)));
        Assert.Contains("307", error.Message);
        Assert.Contains(origin.Requests, request => request.Method == "PUT" && request.Status == 307);
        Assert.DoesNotContain(target.Requests, request => request.Method == "PUT");
        origin.AssertHealthy();
        target.AssertHealthy();
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task GivenFaultingOrInvalidProvider_WhenBridgeOrKernelRequestsHeaders_FailsSanitizedWithoutSending(bool kernel, bool invalidHeader)
    {
        const string sensitiveDetail = "provider-sensitive-detail";
        var baseline = Registrations();
        var server = StartServer();
        await WithinDeadline(SeedSimpleTableAsync(server.Store));
        var provider = new TrackingProvider();
        using var engine = new DeltaEngine(EngineOptions.Default);
        ITable? table = null;
        try
        {
            if (kernel)
            {
                table = await WithinDeadline(engine.LoadTableAsync(LoadOptions(server, provider), CancellationToken.None));
                AssertBothModules(baseline);
            }
            var before = server.Requests.Count;
            var callbacksBefore = provider.Calls.Count;
            if (invalidHeader) provider.HeadersOverride = new Dictionary<string, string> { ["Authorization"] = sensitiveDetail };
            else provider.BeforeHeaders = (_, _) => Task.FromException(new InvalidOperationException(sensitiveDetail));
            var error = await WithinDeadline(Record.ExceptionAsync(async () =>
            {
                if (kernel)
                {
                    using var read = await table!.ReadAsArrowTableAsync(CancellationToken.None);
                }
                else
                {
                    using var rejected = await engine.LoadTableAsync(LoadOptions(server, provider), CancellationToken.None);
                }
            }));
            Assert.NotNull(error);
            Assert.True(provider.Calls.Count > callbacksBefore, "The native operation never reached the real provider.");
            Assert.Contains(provider.Calls.Skip(callbacksBefore), call => call.Module ==
                (kernel ? NativeHeaderModule.Kernel : NativeHeaderModule.Bridge));
            Assert.False(error!.ToString().Contains(sensitiveDetail), "Provider details escaped into the public exception.");
            Assert.False(error.ToString().Contains(StorageToken), "Storage credentials escaped into the public exception.");
            Assert.Equal(before, server.Requests.Count);
            Assert.False(provider.Disposed);
            server.AssertHealthy();
        }
        finally { table?.Dispose(); }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task GivenPendingProvider_WhenDisposingTableAndEngineThenCancelling_LateCompletionRetiresSafely(bool honorsCancellation)
    {
        var registrations = Registrations();
        var workers = Workers();
        var server = StartServer();
        await WithinDeadline(SeedSimpleTableAsync(server.Store));
        var provider = new TrackingProvider();
        using var engine = new DeltaEngine(EngineOptions.Default);
        using var table = await WithinDeadline(engine.LoadTableAsync(LoadOptions(server, provider), CancellationToken.None));
        AssertBothModules(registrations);
        var entered = Signal<bool>();
        var gate = Signal<bool>();
        var cancelled = Signal<bool>();
        provider.BeforeHeaders = async (_, token) =>
        {
            using var registration = token.Register(() => cancelled.TrySetResult(true));
            entered.TrySetResult(true);
            if (honorsCancellation)
            {
                await Task.WhenAny(gate.Task, cancelled.Task);
                token.ThrowIfCancellationRequested();
            }
            else await gate.Task;
        };
        using var cancellation = new CancellationTokenSource();
        var before = server.Requests.Count;
        var query = QueryRowsAsync(table, cancellation.Token);
        try
        {
            await WithinDeadline(entered.Task);
            Assert.False(query.IsCompleted);
            Assert.Equal(before, server.Requests.Count);
            table.Dispose();
            engine.Dispose();
            await Task.Run(() => cancellation.Cancel());
            await WithinDeadline(Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await query));
            await WithinDeadline(cancelled.Task);
            if (!honorsCancellation) Assert.NotEmpty(Workers().Except(workers));
        }
        finally { gate.TrySetResult(true); }
        await WaitForRetirementAsync(registrations, workers);
        Assert.Equal(before, server.Requests.Count);
        Assert.False(provider.Disposed);
        server.AssertHealthy();
    }

    [Fact]
    public async Task GivenOnlyAWeakCallerReference_WhenCollectingGarbage_LiveHybridHandleRootsProvider()
    {
        var baseline = Registrations();
        var workers = Workers();
        var server = StartServer();
        await WithinDeadline(SeedSimpleTableAsync(server.Store));
        using var engine = new DeltaEngine(EngineOptions.Default);
        var weakLoad = StartWeakLoad(engine, server);
        using var table = await WithinDeadline(weakLoad.Load);
        AssertBothModules(baseline);
        ForceCollection();
        Assert.True(weakLoad.Provider.IsAlive, "A live hybrid handle lost its caller-owned provider.");
        await WithinDeadline(table.CheckpointAsync(CancellationToken.None));
        Assert.Equal(0UL, table.Version());
        AssertInstrumented(server, (TrackingProvider)weakLoad.Provider.Target!);
        Assert.False(((TrackingProvider)weakLoad.Provider.Target!).Disposed);
        table.Dispose();
        engine.Dispose();
        await WaitForRetirementAsync(baseline, workers);
    }

    [Fact]
    public async Task GivenLoopbackFixture_WhenUsingAzureBlobRest_ListsRangesAndCommitsBlocks()
    {
        var server = StartServer();
        const string blob = "container/table/data.parquet";
        var put = await WithinDeadline(SendFixtureRequestAsync(server, "PUT", blob, new byte[] { 0, 1, 2, 3, 4, 5 }));
        Assert.Equal(201, put.Status);
        var head = await WithinDeadline(SendFixtureRequestAsync(server, "HEAD", blob));
        Assert.Equal("6", head.Headers["Content-Length"]);
        Assert.True(head.Headers.ContainsKey("ETag"));
        Assert.True(head.Headers.ContainsKey("Last-Modified"));
        Assert.Empty(head.Body);
        var range = await WithinDeadline(SendFixtureRequestAsync(server, "GET", blob, range: "bytes=2-4"));
        Assert.Equal(206, range.Status);
        Assert.Equal(new byte[] { 2, 3, 4 }, range.Body);
        Assert.Equal("bytes 2-4/6", range.Headers["Content-Range"]);
        var listing = await WithinDeadline(SendFixtureRequestAsync(server, "GET", "container?comp=list&prefix=table%2F"));
        var xml = XElement.Parse(Encoding.UTF8.GetString(listing.Body));
        Assert.Equal("table/data.parquet", Assert.Single(xml.Descendants("Name")).Value);
        foreach (var blockId in new[] { "first", "second" })
        {
            var block = await WithinDeadline(SendFixtureRequestAsync(server, "PUT", blob + "?comp=block&blockid=" + blockId,
                Encoding.UTF8.GetBytes(blockId)));
            Assert.Equal(201, block.Status);
        }
        var commit = await WithinDeadline(SendFixtureRequestAsync(server, "PUT", blob + "?comp=blocklist",
            Encoding.UTF8.GetBytes("<BlockList><Latest>first</Latest><Latest>second</Latest></BlockList>")));
        Assert.Equal(201, commit.Status);
        var committed = await WithinDeadline(SendFixtureRequestAsync(server, "GET", blob));
        Assert.Equal("firstsecond", Encoding.UTF8.GetString(committed.Body));
        var deletion = await WithinDeadline(SendFixtureRequestAsync(server, "DELETE", blob));
        Assert.Equal(202, deletion.Status);
        var missing = await WithinDeadline(SendFixtureRequestAsync(server, "GET", blob));
        Assert.Equal(404, missing.Status);
        Assert.Contains(server.Requests, request => request.Method == "PUT" && request.Body.Length == 6);
        server.AssertHealthy();
    }

    private sealed class ProviderCall
    {
        internal ProviderCall(StorageRequestContext context, string value, NativeHeaderModule module)
        {
            Context = context;
            Module = module;
            Value = value;
        }

        internal StorageRequestContext Context { get; }
        internal NativeHeaderModule Module { get; }
        internal string Value { get; }
    }

    private sealed class TrackingProvider : IStorageRequestHeaderProvider, IDisposable
    {
        private int _sequence;

        internal ConcurrentQueue<ProviderCall> Calls { get; } = new();
        internal bool Disposed { get; private set; }
        internal Func<StorageRequestContext, CancellationToken, Task>? BeforeHeaders { get; set; }
        internal IReadOnlyDictionary<string, string>? HeadersOverride { get; set; }

        public async Task<IReadOnlyDictionary<string, string>> GetHeadersAsync(StorageRequestContext request, CancellationToken token)
        {
            var value = "request-" + Interlocked.Increment(ref _sequence).ToString(CultureInfo.InvariantCulture);
            Calls.Enqueue(new ProviderCall(request, value, RequestModule(request)));
            await Task.Yield();
            if (BeforeHeaders != null) await BeforeHeaders(request, token);
            return HeadersOverride ?? new Dictionary<string, string> { [TokenHeader] = value, [ActorHeader] = "loopback" };
        }

        public void Dispose() => Disposed = true;
    }

    private sealed class CapturedRequest
    {
        internal CapturedRequest(string method, Uri uri, Dictionary<string, string> headers, byte[] body, int sequence)
        {
            Method = method;
            Uri = uri;
            Headers = headers;
            Body = body;
            Sequence = sequence;
        }

        internal byte[] Body { get; }
        internal IReadOnlyDictionary<string, string> Headers { get; }
        internal string Method { get; }
        internal int Sequence { get; }
        internal int Status { get; set; }
        internal Uri Uri { get; }
        public override string ToString() => Method + " " + Uri.AbsolutePath;
    }

    private sealed class BlobStore
    {
        internal ConcurrentDictionary<string, byte[]> Blobs { get; } = new(StringComparer.Ordinal);
        internal ConcurrentDictionary<(string Path, string Id), byte[]> Blocks { get; } = new();
    }

    private sealed class BlobServer
    {
        private const string LastModified = "Mon, 05 Oct 2026 00:00:00 GMT";
        private readonly Task _accept;
        private readonly ConcurrentDictionary<TcpClient, Task> _clients = new();
        private readonly ConcurrentQueue<Exception> _errors = new();
        private readonly TcpListener _listener;
        private int _sequence;
        private bool _stopping;

        internal BlobServer(BlobStore store)
        {
            Store = store;
            _listener = new TcpListener(IPAddress.Loopback, 0);
            _listener.Start();
            var port = ((IPEndPoint)_listener.LocalEndpoint).Port;
            Endpoint = new Uri("http://127.0.0.1:" + port.ToString(CultureInfo.InvariantCulture) + "/base/");
            _accept = AcceptAsync();
        }

        internal Uri Endpoint { get; }
        internal int FailuresRemaining;
        internal Uri? RedirectOrigin { get; set; }
        internal bool RedirectReadsOnly { get; set; }
        internal ConcurrentQueue<CapturedRequest> Requests { get; } = new();
        internal BlobStore Store { get; }

        internal void AssertHealthy() => Assert.True(_errors.IsEmpty, "The loopback HTTP fixture failed.");

        internal async Task DisposeAsync()
        {
            Volatile.Write(ref _stopping, true);
            _listener.Stop();
            await _accept;
            foreach (var client in _clients.Keys) client.Dispose();
            await Task.WhenAll(_clients.Values);
            AssertHealthy();
        }

        private async Task AcceptAsync()
        {
            try
            {
                while (!Volatile.Read(ref _stopping))
                {
                    var client = await _listener.AcceptTcpClientAsync();
                    client.NoDelay = true;
                    _clients.TryAdd(client, ServeAsync(client));
                }
            }
            catch (SocketException) when (Volatile.Read(ref _stopping)) { }
            catch (ObjectDisposedException) when (Volatile.Read(ref _stopping)) { }
        }

        private async Task ServeAsync(TcpClient client)
        {
            try
            {
                using (client)
                using (var stream = client.GetStream())
                {
                    var requestLine = await ReadLineAsync(stream);
                    if (requestLine.Length == 0) return;
                    var parts = requestLine.Split(' ');
                    if (parts.Length != 3) throw new InvalidDataException("Invalid request line.");
                    var headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                    for (var line = await ReadLineAsync(stream); line.Length != 0; line = await ReadLineAsync(stream))
                    {
                        var separator = line.IndexOf(':');
                        if (separator < 1) throw new InvalidDataException("Invalid HTTP header.");
                        headers[line.Substring(0, separator)] = line.Substring(separator + 1).Trim();
                    }
                    if (headers.TryGetValue("Expect", out var expect) && expect.Equals("100-continue", StringComparison.OrdinalIgnoreCase))
                        await WriteAsync(stream, Encoding.ASCII.GetBytes("HTTP/1.1 100 Continue\r\n\r\n"));
                    var body = await ReadBodyAsync(stream, headers);
                    var request = new CapturedRequest(parts[0], new Uri(Endpoint, parts[1]), headers, body,
                        Interlocked.Increment(ref _sequence));
                    Requests.Enqueue(request);
                    await RespondAsync(stream, request);
                }
            }
            catch (Exception error) when (Volatile.Read(ref _stopping) &&
                (error is IOException || error is ObjectDisposedException || error is SocketException)) { }
            catch (Exception error) { _errors.Enqueue(error); }
        }

        private async Task RespondAsync(NetworkStream stream, CapturedRequest request)
        {
            if (RedirectOrigin != null && (!RedirectReadsOnly || request.Method == "GET" || request.Method == "HEAD"))
            {
                await RespondAsync(stream, request, 307, Array.Empty<byte>(),
                    new Dictionary<string, string> { ["Location"] = new Uri(RedirectOrigin, request.Uri.PathAndQuery).AbsoluteUri });
                return;
            }
            if (Interlocked.CompareExchange(ref FailuresRemaining, 0, 1) == 1)
            {
                await ErrorAsync(stream, request, 500, "InternalError");
                return;
            }
            var query = ParseQuery(request.Uri);
            var path = Uri.UnescapeDataString(request.Uri.AbsolutePath);
            var key = path.StartsWith("/base/container/", StringComparison.Ordinal) ? path.Substring("/base/container/".Length) : "";
            if (query.TryGetValue("comp", out var component) && component == "list")
            {
                query.TryGetValue("prefix", out var prefix);
                var entries = Store.Blobs.OrderBy(blob => blob.Key, StringComparer.Ordinal)
                    .Where(blob => blob.Key.StartsWith(prefix ?? "", StringComparison.Ordinal))
                    .Select(blob => new XElement("Blob", new XElement("Name", blob.Key),
                        new XElement("Properties", new XElement("Last-Modified", LastModified),
                            new XElement("Etag", "\"integration\""), new XElement("Content-Length", blob.Value.Length),
                            new XElement("Content-Type", "application/octet-stream"), new XElement("BlobType", "BlockBlob"))));
                var listing = new XElement("EnumerationResults", new XElement("Blobs", entries), new XElement("NextMarker"));
                await RespondAsync(stream, request, 200, Encoding.UTF8.GetBytes(listing.ToString(SaveOptions.DisableFormatting)));
                return;
            }
            if (request.Method == "PUT")
            {
                if (component == "block")
                {
                    Store.Blocks[(key, query["blockid"])] = request.Body;
                }
                else
                {
                    var bytes = request.Body;
                    if (component == "blocklist")
                    {
                        var blockList = XElement.Parse(Encoding.UTF8.GetString(bytes));
                        using var combined = new MemoryStream();
                        foreach (var block in blockList.Elements())
                        {
                            var blockBytes = Store.Blocks[(key, block.Value)];
                            await combined.WriteAsync(blockBytes, 0, blockBytes.Length);
                        }
                        bytes = combined.ToArray();
                    }
                    if (request.Headers.TryGetValue("If-None-Match", out var condition) && condition == "*")
                    {
                        if (!Store.Blobs.TryAdd(key, bytes))
                        {
                            await ErrorAsync(stream, request, 412, "BlobAlreadyExists");
                            return;
                        }
                    }
                    else Store.Blobs[key] = bytes;
                }
                await RespondAsync(stream, request, 201, Array.Empty<byte>());
                return;
            }
            if ((request.Method == "GET" || request.Method == "HEAD") && Store.Blobs.TryGetValue(key, out var content))
            {
                var responseHeaders = new Dictionary<string, string>();
                if (request.Method == "HEAD")
                {
                    responseHeaders["Content-Length"] = content.Length.ToString(CultureInfo.InvariantCulture);
                    await RespondAsync(stream, request, 200, Array.Empty<byte>(), responseHeaders);
                    return;
                }
                if (request.Headers.TryGetValue("Range", out var range) || request.Headers.TryGetValue("x-ms-range", out range))
                {
                    var bounds = range.Substring("bytes=".Length).Split('-');
                    var start = bounds[0].Length == 0 ? Math.Max(0, content.Length - int.Parse(bounds[1], CultureInfo.InvariantCulture)) :
                        int.Parse(bounds[0], CultureInfo.InvariantCulture);
                    var end = bounds[0].Length == 0 || bounds[1].Length == 0 ? content.Length - 1 :
                        Math.Min(content.Length - 1, int.Parse(bounds[1], CultureInfo.InvariantCulture));
                    if (start > end || start >= content.Length)
                    {
                        await ErrorAsync(stream, request, 416, "InvalidRange");
                        return;
                    }
                    responseHeaders["Content-Range"] = string.Format(CultureInfo.InvariantCulture, "bytes {0}-{1}/{2}", start, end, content.Length);
                    var slice = new byte[end - start + 1];
                    Array.Copy(content, start, slice, 0, slice.Length);
                    await RespondAsync(stream, request, 206, slice, responseHeaders);
                }
                else await RespondAsync(stream, request, 200, content);
                return;
            }
            if (request.Method == "DELETE" && Store.Blobs.TryRemove(key, out _))
            {
                await RespondAsync(stream, request, 202, Array.Empty<byte>());
                return;
            }
            await ErrorAsync(stream, request, 404, "BlobNotFound");
        }

        private static Dictionary<string, string> ParseQuery(Uri uri)
        {
            var query = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (var entry in uri.Query.TrimStart('?').Split('&'))
            {
                if (entry.Length == 0) continue;
                var separator = entry.IndexOf('=');
                var name = separator < 0 ? entry : entry.Substring(0, separator);
                var value = separator < 0 ? "" : entry.Substring(separator + 1);
                query[Uri.UnescapeDataString(name)] = Uri.UnescapeDataString(value.Replace("+", " "));
            }
            return query;
        }

        private static Task ErrorAsync(NetworkStream stream, CapturedRequest request, int status, string code) =>
            RespondAsync(stream, request, status, Encoding.UTF8.GetBytes(new XElement("Error", new XElement("Code", code)).ToString()),
                new Dictionary<string, string> { ["x-ms-error-code"] = code });

        private static async Task RespondAsync(NetworkStream stream, CapturedRequest request, int status, byte[] body,
            Dictionary<string, string>? headers = null)
        {
            request.Status = status;
            headers ??= new Dictionary<string, string>();
            if (!headers.ContainsKey("Content-Length")) headers["Content-Length"] = body.Length.ToString(CultureInfo.InvariantCulture);
            headers["ETag"] = "\"integration\"";
            headers["Last-Modified"] = LastModified;
            headers["Accept-Ranges"] = "bytes";
            headers["Content-Type"] = "application/octet-stream";
            headers["x-ms-blob-type"] = "BlockBlob";
            headers["Connection"] = "close";
            var response = new StringBuilder("HTTP/1.1 ").Append(status).Append(" Response\r\n");
            foreach (var header in headers) response.Append(header.Key).Append(": ").Append(header.Value).Append("\r\n");
            response.Append("\r\n");
            await WriteAsync(stream, Encoding.ASCII.GetBytes(response.ToString()));
            if (request.Method != "HEAD") await WriteAsync(stream, body);
        }

        private static Task WriteAsync(NetworkStream stream, byte[] bytes) => stream.WriteAsync(bytes, 0, bytes.Length);

        internal static async Task<string> ReadLineAsync(NetworkStream stream)
        {
            using var line = new MemoryStream();
            var buffer = new byte[1];
            while (await stream.ReadAsync(buffer, 0, 1) != 0)
            {
                if (buffer[0] == '\n') return Encoding.ASCII.GetString(line.ToArray()).TrimEnd('\r');
                line.WriteByte(buffer[0]);
                if (line.Length > 65536) throw new InvalidDataException("HTTP line exceeded fixture limit.");
            }
            if (line.Length != 0) throw new EndOfStreamException("Incomplete HTTP line.");
            return "";
        }

        private static async Task<byte[]> ReadBodyAsync(NetworkStream stream, Dictionary<string, string> headers)
        {
            using var body = new MemoryStream();
            if (headers.TryGetValue("Transfer-Encoding", out var transfer) && transfer.Equals("chunked", StringComparison.OrdinalIgnoreCase))
            {
                while (true)
                {
                    var sizeLine = await ReadLineAsync(stream);
                    var size = int.Parse(sizeLine.Split(';')[0], NumberStyles.HexNumber, CultureInfo.InvariantCulture);
                    if (size == 0)
                    {
                        while ((await ReadLineAsync(stream)).Length != 0) { }
                        break;
                    }
                    var chunk = await ReadBytesAsync(stream, size);
                    await body.WriteAsync(chunk, 0, chunk.Length);
                    if ((await ReadLineAsync(stream)).Length != 0) throw new InvalidDataException("Invalid chunk terminator.");
                }
            }
            else if (headers.TryGetValue("Content-Length", out var length))
            {
                var bytes = await ReadBytesAsync(stream, int.Parse(length, CultureInfo.InvariantCulture));
                await body.WriteAsync(bytes, 0, bytes.Length);
            }
            return body.ToArray();
        }

        internal static async Task<byte[]> ReadBytesAsync(NetworkStream stream, int length)
        {
            if (length < 0 || length > 16 * 1024 * 1024) throw new InvalidDataException("HTTP body exceeded fixture limit.");
            var body = new byte[length];
            var offset = 0;
            while (offset < body.Length)
            {
                var count = await stream.ReadAsync(body, offset, body.Length - offset);
                if (count == 0) throw new EndOfStreamException("Incomplete HTTP body.");
                offset += count;
            }
            return body;
        }
    }
}