using System.Collections.Concurrent;
using System.Reflection;
using System.Runtime.InteropServices;
using Apache.Arrow;
using DeltaLake.Credentials;
using DeltaLake.Interfaces;
using DeltaLake.Kernel.Callbacks.Errors;
using DeltaLake.Kernel.Credentials;
using DeltaLake.Kernel.State;
using DeltaLake.Table;

namespace DeltaLake.Tests.Table;

[Collection("KernelCredentialContract")]
public class KernelCredentialDisposalTests
{
    [Fact]
    public async Task GivenRetainedTableHandle_WhenTableIsDisposed_NewKernelAdmissionsAreRejected()
    {
        var server = new KernelAzureTestServer();
        var provider = new GatedProvider(int.MaxValue);
        try
        {
            await SeedAsync(server);
            using IEngine engine = new DeltaEngine(EngineOptions.Default);
            using ITable table = await WithinDeadlineAsync(engine.LoadTableAsync(Options(server, provider), CancellationToken.None));
            var handle = (SafeHandle)typeof(DeltaTable).GetField("table", BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(table)!;
            using (var retainedHandle = new SafeHandleLease(handle))
            {
                table.Dispose();
                Assert.Throws<ObjectDisposedException>(() => table.Version());
                Assert.Throws<ObjectDisposedException>(() => table.Location());
                await AssertReadDisposedAsync(table);
            }
            Assert.False(provider.Disposed);
        }
        finally
        {
            await WithinDeadlineAsync(server.DisposeAsync());
        }
    }

    [Fact]
    public async Task GivenStartedCdcIterator_WhenTableIsDisposed_RemainingChangesAndCredentialContextStayAlive()
    {
        var server = new KernelAzureTestServer();
        var provider = new GatedProvider(int.MaxValue);
        try
        {
            SeedCdc(server);
            Assert.Empty(server.Requests);
            var existingContexts = new HashSet<ulong>(ActiveRegistrations().Keys);
            using IEngine engine = new DeltaEngine(EngineOptions.Default);
            using ITable table = await WithinDeadlineAsync(engine.LoadTableAsync(Options(server, provider), CancellationToken.None));
            var registration = NewRegistration(existingContexts);
            var changes = new List<(string Kind, string? Name, long? Version)>();
            var iterator = table.QueryTableChangesAsync(new TableChangesOptions(0) { EndVersion = 2 }, CancellationToken.None)
                .GetAsyncEnumerator(CancellationToken.None);
            var rowsAfterDisposal = 0;
            try
            {
                Assert.True(await WithinDeadlineAsync(iterator.MoveNextAsync().AsTask()));
                AppendChanges(iterator.Current, changes);
                Assert.Equal(2, provider.Calls);

                table.Dispose();
                Assert.False(registration.Released.IsCompleted);
                Assert.Equal(existingContexts.Count + 1, KernelCredentialRegistration.RegistrationCount);
                Assert.False(provider.Disposed);
                Assert.Throws<ObjectDisposedException>(() => table.Version());
                await AssertReadDisposedAsync(table);

                while (await WithinDeadlineAsync(iterator.MoveNextAsync().AsTask()))
                {
                    rowsAfterDisposal += iterator.Current.Length;
                    AppendChanges(iterator.Current, changes);
                    Assert.InRange(changes.Count, 1, 3);
                }
            }
            finally
            {
                await WithinDeadlineAsync(iterator.DisposeAsync().AsTask());
            }

            Assert.True(rowsAfterDisposal > 0);
            Assert.Equal(3, changes.Count);
            Assert.Contains(("insert", "Mario", (long?)1), changes);
            Assert.Contains(("update_preimage", "Mario", (long?)2), changes);
            Assert.Contains(("update_postimage", "Mino", (long?)2), changes);
            Assert.Equal(2, provider.Calls);
            Assert.Contains(server.Requests, request => request.Method == "GET"
                && request.Path.Contains("/_change_data/")
                && request.Authorization == "Bearer B.synthetic==");
            await AssertRegistrationReleasedAsync(registration, existingContexts.Count);
            engine.Dispose();
            Assert.False(provider.Disposed);
        }
        finally
        {
            await WithinDeadlineAsync(server.DisposeAsync());
        }
    }

    [Fact]
    public async Task GivenPendingBootstrap_WhenEngineIsDisposed_CreateThrowsWithoutRemoteFiles()
    {
        var server = new KernelAzureTestServer();
        var provider = new GatedProvider(1);
        try
        {
            Assert.Empty(server.Requests);
            Assert.Empty(Directory.GetFiles(server.Root.FullName, "*", SearchOption.AllDirectories));
            using IEngine engine = new DeltaEngine(EngineOptions.Default);
            using var batch = TableHelpers.BuildBasicRecordBatch(1);
            var options = Options(server, provider);
            var creating = engine.CreateTableAsync(new TableCreateOptions(options.TableLocation, batch.Schema)
            {
                StorageOptions = options.StorageOptions,
                KernelAzureBearerCredential = options.KernelAzureBearerCredential,
            }, CancellationToken.None);
            await WithinDeadlineAsync(provider.Started.Task);
            Assert.False(creating.IsCompleted);

            engine.Dispose();
            Assert.False(creating.IsCompleted);
            Assert.False(provider.Disposed);
            provider.Result.SetResult(Token("A.synthetic=="));

            await WithinDeadlineAsync(Assert.ThrowsAsync<ObjectDisposedException>(async () =>
            {
                using var table = await creating;
            }));
            Assert.Equal(1, provider.Calls);
            Assert.Empty(server.Requests);
            Assert.Empty(Directory.GetFiles(server.Root.FullName, "*", SearchOption.AllDirectories));
            Assert.False(provider.Disposed);
        }
        finally
        {
            provider.Result.TrySetResult(Token("A.synthetic=="));
            await WithinDeadlineAsync(server.DisposeAsync());
        }
    }

    [Fact]
    public async Task GivenPendingBootstrap_WhenEngineIsDisposed_LoadThrowsBeforeRemoteRequests()
    {
        var server = new KernelAzureTestServer();
        var provider = new GatedProvider(1);
        try
        {
            await SeedAsync(server);
            Assert.Empty(server.Requests);
            using IEngine engine = new DeltaEngine(EngineOptions.Default);
            var loading = engine.LoadTableAsync(Options(server, provider), CancellationToken.None);
            await WithinDeadlineAsync(provider.Started.Task);
            Assert.False(loading.IsCompleted);

            engine.Dispose();
            Assert.False(loading.IsCompleted);
            Assert.False(provider.Disposed);
            provider.Result.SetResult(Token("A.synthetic=="));

            await WithinDeadlineAsync(Assert.ThrowsAsync<ObjectDisposedException>(async () =>
            {
                using var table = await loading;
            }));
            Assert.Equal(1, provider.Calls);
            Assert.Empty(server.Requests);
            Assert.False(provider.Disposed);
        }
        finally
        {
            provider.Result.TrySetResult(Token("A.synthetic=="));
            await WithinDeadlineAsync(server.DisposeAsync());
        }
    }

    [Fact]
    public async Task GivenPendingKernelFault_WhenTableIsDisposed_NativeReleaseDrainsRegistrationWithoutDisposingProvider()
    {
        var server = new KernelAzureTestServer();
        var provider = new GatedProvider(2);
        try
        {
            await SeedAsync(server);
            var existingContexts = new HashSet<ulong>(ActiveRegistrations().Keys);
            using IEngine engine = new DeltaEngine(EngineOptions.Default);
            using ITable table = await WithinDeadlineAsync(engine.LoadTableAsync(Options(server, provider), CancellationToken.None));
            var registration = NewRegistration(existingContexts);
            var beforeRead = server.Requests.Length;
            var reading = table.ReadAsArrowTableAsync(CancellationToken.None);
            await WithinDeadlineAsync(provider.Started.Task);
            Assert.False(reading.IsCompleted);
            Assert.Equal(1, registration.PendingRequestCount);

            table.Dispose();
            Assert.False(reading.IsCompleted);
            Assert.False(registration.Released.IsCompleted);
            Assert.Equal(existingContexts.Count + 1, KernelCredentialRegistration.RegistrationCount);
            Assert.False(provider.Disposed);
            provider.Result.SetException(new InvalidOperationException("B.synthetic== provider-sensitive-detail"));

            var error = await WithinDeadlineAsync(Assert.ThrowsAsync<KernelException>(async () =>
            {
                using var arrow = await reading;
            }));
            Assert.DoesNotContain("B.synthetic==", error.ToString());
            Assert.DoesNotContain("provider-sensitive-detail", error.ToString());
            Assert.Null(error.InnerException);
            Assert.Equal(beforeRead, server.Requests.Length);
            Assert.Equal(2, provider.Calls);
            await AssertRegistrationReleasedAsync(registration, existingContexts.Count);
            Assert.Throws<ObjectDisposedException>(() => table.Version());
            await AssertReadDisposedAsync(table);
            engine.Dispose();
            Assert.False(provider.Disposed);
        }
        finally
        {
            provider.Result.TrySetResult(Token("B.synthetic=="));
            await WithinDeadlineAsync(server.DisposeAsync());
        }
    }

    [Fact]
    public async Task GivenPendingKernelRead_WhenTableIsDisposed_AdmittedReadReturnsRowsAndNewOperationsThrow()
    {
        var server = new KernelAzureTestServer();
        var provider = new GatedProvider(2);
        try
        {
            await SeedAsync(server);
            using IEngine engine = new DeltaEngine(EngineOptions.Default);
            using ITable table = await WithinDeadlineAsync(engine.LoadTableAsync(Options(server, provider), CancellationToken.None));
            Assert.Equal(1, provider.Calls);
            var beforeRead = server.Requests.Length;
            var reading = table.ReadAsArrowTableAsync(CancellationToken.None);
            await WithinDeadlineAsync(provider.Started.Task);
            Assert.False(reading.IsCompleted);
            Assert.Equal(beforeRead, server.Requests.Length);

            table.Dispose();
            Assert.False(reading.IsCompleted);
            Assert.False(provider.Disposed);
            Assert.Throws<ObjectDisposedException>(() => table.Version());
            await AssertReadDisposedAsync(table);
            provider.Result.SetResult(Token("B.synthetic=="));

            using var arrow = await WithinDeadlineAsync(reading);
            Assert.Equal(3, arrow.Table.RowCount);
            var values = (Int32Array)arrow.Table.Column(0).Data.ArrowArray(0);
            Assert.Equal(new int?[] { 0, 1, 2 }, Enumerable.Range(0, 3).Select(values.GetValue));
            Assert.Equal(2, provider.Calls);
            Assert.Contains(server.Requests.Skip(beforeRead), request => request.Method == "GET"
                && request.Path.EndsWith(".parquet", StringComparison.Ordinal)
                && request.Authorization == "Bearer B.synthetic==");
            Assert.Throws<ObjectDisposedException>(() => table.Version());
            await AssertReadDisposedAsync(table);
            arrow.Dispose();
            engine.Dispose();
            Assert.False(provider.Disposed);
        }
        finally
        {
            provider.Result.TrySetResult(Token("B.synthetic=="));
            await WithinDeadlineAsync(server.DisposeAsync());
        }
    }

    private static Task AssertReadDisposedAsync(ITable table)
        => WithinDeadlineAsync(Assert.ThrowsAsync<ObjectDisposedException>(async () =>
        {
            using var arrow = await table.ReadAsArrowTableAsync(CancellationToken.None);
        }));

    private static ConcurrentDictionary<ulong, KernelCredentialRegistration> ActiveRegistrations()
        => (ConcurrentDictionary<ulong, KernelCredentialRegistration>)typeof(KernelCredentialRegistration)
            .GetField("Registrations", BindingFlags.NonPublic | BindingFlags.Static)!.GetValue(null)!;

    private static KernelCredentialRegistration NewRegistration(HashSet<ulong> existingContexts)
        => Assert.Single(ActiveRegistrations().Values, registration => !existingContexts.Contains(registration.ContextId));

    private static async Task AssertRegistrationReleasedAsync(KernelCredentialRegistration registration, int expectedCount)
    {
        await WithinDeadlineAsync(registration.Released);
        Assert.Equal(expectedCount, KernelCredentialRegistration.RegistrationCount);
        Assert.True(await WithinDeadlineAsync(Task.Run(() => SpinWait.SpinUntil(
            () => registration.PendingRequestCount == 0, TimeSpan.FromSeconds(5)))));
    }

    private static void AppendChanges(RecordBatch batch, List<(string Kind, string? Name, long? Version)> changes)
    {
        using (batch)
        {
            var kinds = (StringArray)batch.Column("_change_type");
            var names = (StringArray)batch.Column("name");
            var versions = batch.Column("_commit_version");
            for (var row = 0; row < batch.Length; row++)
            {
                long? version = versions switch
                {
                    Int64Array values => values.GetValue(row),
                    Int32Array values => values.GetValue(row),
                    _ => throw new InvalidDataException("Unexpected CDC commit-version column type."),
                };
                changes.Add((kinds.GetString(row), names.GetString(row), version));
            }
        }
    }

    private static void SeedCdc(KernelAzureTestServer server)
    {
        var source = TableIdentifier.SimpleTableWithCdc.TablePath();
        foreach (var file in Directory.GetFiles(source, "*", SearchOption.AllDirectories))
        {
            var destination = Path.Combine(server.Root.FullName,
                KernelAzureTestServer.RelativePath(Path.GetFullPath(source), Path.GetFullPath(file))
                    .Replace('/', Path.DirectorySeparatorChar));
            Directory.CreateDirectory(Path.GetDirectoryName(destination)!);
            File.Copy(file, destination);
        }
    }

    private static async Task SeedAsync(KernelAzureTestServer server)
    {
        var seed = await WithinDeadlineAsync(TableHelpers.SetupTable(DirectoryHelpers.ToFileUri(server.Root.FullName), 3));
        seed.table.Dispose();
        seed.engine.Dispose();
    }

    private static TableOptions Options(KernelAzureTestServer server, IAzureBearerTokenProvider provider)
    {
        return new TableOptions
        {
            TableLocation = "az://fixture/table",
            StorageOptions = new Dictionary<string, string>
            {
                ["account_name"] = "fixture",
                ["endpoint"] = server.Endpoint.AbsoluteUri.TrimEnd('/'),
                ["allow_http"] = "true",
            },
            KernelAzureBearerCredential = new KernelAzureBearerCredentialOptions(provider),
        };
    }

    private static AzureBearerToken Token(string value)
        => new AzureBearerToken(value, DateTimeOffset.UtcNow.AddMinutes(36));

    private static async Task WithinDeadlineAsync(Task task)
    {
        Assert.Same(task, await Task.WhenAny(task, Task.Delay(TimeSpan.FromSeconds(10))));
        await task;
    }

    private static async Task<TResult> WithinDeadlineAsync<TResult>(Task<TResult> task)
    {
        await WithinDeadlineAsync((Task)task);
        return await task;
    }

    private sealed class GatedProvider : IAzureBearerTokenProvider, IDisposable
    {
        private readonly int delayedCall;
        private int calls;
        private int disposed;

        internal GatedProvider(int delayedCall)
        {
            this.delayedCall = delayedCall;
        }

        internal int Calls => Volatile.Read(ref calls);

        internal bool Disposed => Volatile.Read(ref disposed) != 0;

        internal TaskCompletionSource<AzureBearerToken> Result { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal TaskCompletionSource<bool> Started { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task<AzureBearerToken> GetTokenAsync(AzureTokenRequestContext context, CancellationToken cancellationToken)
        {
            Assert.Equal(TimeSpan.FromSeconds(90), context.MinimumLifetime);
            Assert.Equal(new[] { "https://storage.azure.com/.default" }, context.Scopes);
            cancellationToken.ThrowIfCancellationRequested();
            var call = Interlocked.Increment(ref calls);
            if (call == delayedCall)
            {
                Started.TrySetResult(true);
                return Result.Task;
            }

            return Task.FromResult(Token(call == 1 ? "A.synthetic==" : "B.synthetic=="));
        }

        public void Dispose()
        {
            Interlocked.Exchange(ref disposed, 1);
        }
    }
}