using System.Net.Sockets;
using System.Text;
using Apache.Arrow;
using DeltaLake.Credentials;
using DeltaLake.Interfaces;
using DeltaLake.Kernel.Callbacks.Errors;
using DeltaLake.Table;

namespace DeltaLake.Tests.Table;

public class KernelCredentialIntegrationTests
{
    private const string IPAddressLoopback = "127.0.0.1";

    [Fact]
    public async Task GivenLoopbackFixture_WhenBlobRequestsArrive_UploadRangeListAndDeleteRoundTrip()
    {
        var server = new KernelAzureTestServer();
        try
        {
            var upload = await SendFixtureRequestAsync(server, "PUT", "fixture/table/blob?comp=block&blockid=YmxvY2s%3D", "parquet");
            Assert.Equal(201, upload.Status);
            var commit = await SendFixtureRequestAsync(server, "PUT", "fixture/table/blob?comp=blocklist", "<BlockList><Latest>YmxvY2s=</Latest></BlockList>");
            Assert.Equal(201, commit.Status);
            var range = await SendFixtureRequestAsync(server, "GET", "fixture/table/blob", headers: "Range: bytes=1-3\r\n");
            Assert.Equal(206, range.Status);
            Assert.Equal("arq", range.Body);
            Assert.Contains("Content-Range: bytes 1-3/7", range.Headers);
            var head = await SendFixtureRequestAsync(server, "HEAD", "fixture/table/blob");
            Assert.Equal(200, head.Status);
            Assert.Contains("Content-Length: 7", head.Headers);
            Assert.Empty(head.Body);
            var listing = await SendFixtureRequestAsync(server, "GET", "fixture?restype=container&comp=list&prefix=table/");
            Assert.Contains("<Name>table/blob</Name>", listing.Body);
            var copy = await SendFixtureRequestAsync(server, "PUT", "fixture/table/copied", headers: $"x-ms-copy-source: {new Uri(server.Endpoint, "fixture/table/blob")}\r\n");
            Assert.Equal(201, copy.Status);
            var conflict = await SendFixtureRequestAsync(server, "PUT", "fixture/table/copied", "duplicate", "If-None-Match: *\r\n");
            Assert.Equal(412, conflict.Status);
            var chunked = await SendFixtureRequestAsync(server, "PUT", "fixture/table/chunked", "7\r\nchunked\r\n0\r\n\r\n", "Transfer-Encoding: chunked\r\n");
            Assert.Equal(201, chunked.Status);
            var retrieved = await SendFixtureRequestAsync(server, "GET", "fixture/table/chunked");
            Assert.Equal("chunked", retrieved.Body);
            var deleted = await SendFixtureRequestAsync(server, "DELETE", "fixture/table/blob");
            Assert.Equal(202, deleted.Status);
            Assert.False(File.Exists(Path.Combine(server.Root.FullName, "blob")));
            Assert.Equal(10, server.Requests.Length);
            Assert.All(server.Requests, request => Assert.Equal("Bearer A.synthetic==", request.Authorization));
        }
        finally
        {
            await server.DisposeAsync();
        }
    }

    private static async Task<(int Status, string Headers, string Body)> SendFixtureRequestAsync(KernelAzureTestServer server, string method, string path, string body = "", string headers = "")
    {
        using var client = new TcpClient();
        await client.ConnectAsync(IPAddressLoopback, server.Endpoint.Port);
        using var stream = client.GetStream();
        var length = headers.Contains("Transfer-Encoding") ? string.Empty : $"Content-Length: {Encoding.ASCII.GetByteCount(body)}\r\n";
        var request = Encoding.ASCII.GetBytes($"{method} /{path} HTTP/1.1\r\nHost: 127.0.0.1\r\nAuthorization: Bearer A.synthetic==\r\nConnection: close\r\n{length}{headers}\r\n{body}");
        await stream.WriteAsync(request, 0, request.Length);
        using var response = new MemoryStream();
        await stream.CopyToAsync(response);
        var text = Encoding.UTF8.GetString(response.ToArray());
        var separator = text.IndexOf("\r\n\r\n", StringComparison.Ordinal);
        return (int.Parse(text.Split(' ')[1]), text.Substring(0, separator), text.Substring(separator + 4));
    }

    [Fact]
    public async Task GivenBootstrapA_WhenKernelReads_ParquetUsesBOnTheSameTable()
    {
        var server = new KernelAzureTestServer();
        try
        {
            var seed = await TableHelpers.SetupTable(DirectoryHelpers.ToFileUri(server.Root.FullName), 3);
            seed.table.Dispose();
            seed.engine.Dispose();
            using IEngine engine = new DeltaEngine(EngineOptions.Default);
            var provider = new RecordingProvider();
            using ITable table = await engine.LoadTableAsync(Options(server, provider), CancellationToken.None);
            Assert.Equal(1, provider.Calls);
            using var arrow = await table.ReadAsArrowTableAsync(CancellationToken.None);
            Assert.Equal(3, arrow.Table.RowCount);
            Assert.Equal(2, provider.Calls);
            Assert.Contains(server.Requests, request => request.Authorization == "Bearer A.synthetic==" && request.Path.Contains("_delta_log"));
            Assert.Contains(server.Requests, request => request.Authorization == "Bearer B.synthetic==" && request.Path.EndsWith(".json", StringComparison.Ordinal));
            Assert.Contains(server.Requests, request => request.Authorization == "Bearer B.synthetic==" && request.Path.EndsWith(".parquet", StringComparison.Ordinal));
            Assert.False(provider.Disposed);
        }
        finally
        {
            await server.DisposeAsync();
        }
    }

    [Fact]
    public async Task GivenKernelA_WhenUsefulLifetimeEnds_SameHandleUsesBForCommitLookupReadsAndCheckpoint()
    {
        var server = new KernelAzureTestServer();
        try
        {
            await SeedAsync(server);
            using IEngine engine = new DeltaEngine(EngineOptions.Default);
            var provider = new RecordingProvider(call => new AzureBearerToken(
                call <= 2 ? "A.synthetic==" : "B.synthetic==",
                call == 2 ? DateTimeOffset.UtcNow.AddSeconds(98) : DateTimeOffset.UtcNow.AddMinutes(36)));
            using ITable table = await engine.LoadTableAsync(Options(server, provider), CancellationToken.None);
            using (var initial = await table.ReadAsArrowTableAsync(CancellationToken.None))
            {
                Assert.Equal(3, initial.Table.RowCount);
            }

            Assert.Equal(2, provider.Calls);
            Assert.Contains(server.Requests, request => request.Authorization == "Bearer A.synthetic==" && request.Path.EndsWith(".parquet", StringComparison.Ordinal));
            var beforeRenewal = server.Requests.Length;
            await Task.Delay(TimeSpan.FromSeconds(9));
            Assert.Equal(beforeRenewal, server.Requests.Length);
            Assert.Equal(2, provider.Calls);

            var realFile = Directory.GetFiles(server.Root.FullName, "*.parquet", SearchOption.AllDirectories).Single();
            var action = new AddAction
            {
                Path = KernelAzureTestServer.RelativePath(server.Root.FullName, realFile),
                Size = new FileInfo(realFile).Length,
                ModificationTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                DataChange = true,
            };
            var committed = await table.CreateWriteTransactionAsync(new[] { action },
                new CommitOptions { AppId = "synthetic-app", TransactionVersion = 7 }, CancellationToken.None);
            Assert.Equal(2, committed);
            Assert.Equal(7, await table.GetLatestTransactionVersionAsync("synthetic-app", CancellationToken.None));
            using (var frame = await table.ReadAsDataFrameAsync(CancellationToken.None))
            {
                Assert.Equal(3, frame.Frame.Rows.Count);
                Assert.Equal(0, frame.Frame[0, 0]);
                Assert.Equal("2", frame.Frame[2, 1]);
            }

            using (var arrow = await table.ReadAsArrowTableAsync(CancellationToken.None))
            {
                Assert.Equal(3, arrow.Table.RowCount);
            }

            await table.CheckpointAsync(CancellationToken.None);
            Assert.True(File.Exists(Path.Combine(server.Root.FullName, "_delta_log", "_last_checkpoint")));
            Assert.NotEmpty(Directory.GetFiles(Path.Combine(server.Root.FullName, "_delta_log"), "*.checkpoint.parquet"));
            Assert.Equal(3, provider.Calls);
            var renewed = server.Requests.Skip(beforeRenewal).ToArray();
            Assert.All(renewed, request => Assert.Equal("Bearer B.synthetic==", request.Authorization));
            Assert.Contains(renewed, request => request.Method == "PUT" && request.Path.EndsWith("00000000000000000002.json", StringComparison.Ordinal));
            Assert.Contains(renewed, request => request.Method == "GET" && request.Path.EndsWith(".parquet", StringComparison.Ordinal));
            Assert.Contains(renewed, request => request.Method == "PUT" && request.Path.EndsWith("_last_checkpoint", StringComparison.Ordinal));

            var beforeBridge = server.Requests.Length;
            Assert.NotEmpty(await table.HistoryAsync(null, CancellationToken.None));
            Assert.Equal(3, provider.Calls);
            var bridgeRequests = server.Requests.Skip(beforeBridge).ToArray();
            Assert.NotEmpty(bridgeRequests);
            Assert.All(bridgeRequests, request => Assert.Equal("Bearer A.synthetic==", request.Authorization));
            table.Dispose();
            Assert.False(provider.Disposed);
        }
        finally
        {
            await server.DisposeAsync();
        }
    }

    [Fact]
    public async Task GivenCallerMutatesOptionsDuringBootstrap_WhenLoaded_BothStoresUseFrozenEndpoint()
    {
        var server = new KernelAzureTestServer();
        try
        {
            await SeedAsync(server);
            using IEngine engine = new DeltaEngine(EngineOptions.Default);
            TableOptions? options = null;
            var provider = new RecordingProvider(beforeReturn: call =>
            {
                if (call == 1)
                {
                    options!.StorageOptions["endpoint"] = "http://127.0.0.1:1";
                    options.StorageOptions["account_name"] = "mutated";
                    options.StorageOptions["allow_http"] = "false";
                    options.StorageOptions["bearer_token"] = "C.synthetic==";
                    options.WithoutFiles = true;
                    options.Version = 999;
                }
            });
            options = Options(server, provider);
            using ITable table = await engine.LoadTableAsync(options, CancellationToken.None);
            using var arrow = await table.ReadAsArrowTableAsync(CancellationToken.None);
            Assert.Equal(3, arrow.Table.RowCount);
            Assert.Equal(1UL, table.Version());
            Assert.Equal(2, provider.Calls);
            Assert.Equal("http://127.0.0.1:1", options.StorageOptions["endpoint"]);
            Assert.DoesNotContain(server.Requests, request => request.Authorization == "Bearer C.synthetic==");
            Assert.Contains(server.Requests, request => request.Authorization == "Bearer A.synthetic==");
            Assert.Contains(server.Requests, request => request.Authorization == "Bearer B.synthetic==");
        }
        finally
        {
            await server.DisposeAsync();
        }
    }

    [Fact]
    public async Task GivenNoProvider_WhenStaticAzureTableIsLoaded_StockReadsRemainCompatible()
    {
        var server = new KernelAzureTestServer();
        try
        {
            await SeedAsync(server);
            using IEngine engine = new DeltaEngine(EngineOptions.Default);
            var options = Options(server, null);
            options.StorageOptions["bearer_token"] = "A.synthetic==";
            using ITable table = await engine.LoadTableAsync(options, CancellationToken.None);
            using var arrow = await table.ReadAsArrowTableAsync(CancellationToken.None);
            using var frame = await table.ReadAsDataFrameAsync(CancellationToken.None);
            Assert.Equal(3, arrow.Table.RowCount);
            Assert.Equal(3, frame.Frame.Rows.Count);
            Assert.Equal(1UL, table.Version());
            Assert.All(server.Requests, request => Assert.Equal("Bearer A.synthetic==", request.Authorization));
        }
        finally
        {
            await server.DisposeAsync();
        }
    }

    [Fact]
    public async Task GivenEmptyAzureRoot_WhenCreatedAndInserted_BridgeWritesAAndKernelReadsB()
    {
        var server = new KernelAzureTestServer();
        try
        {
            using IEngine engine = new DeltaEngine(EngineOptions.Default);
            var provider = new RecordingProvider();
            var options = Options(server, provider);
            using var batch = TableHelpers.BuildBasicRecordBatch(2);
            using ITable table = await engine.CreateTableAsync(new TableCreateOptions(options.TableLocation, batch.Schema)
            {
                StorageOptions = options.StorageOptions,
                KernelAzureBearerCredential = options.KernelAzureBearerCredential,
            }, CancellationToken.None);
            await table.InsertAsync(new[] { batch }, batch.Schema, new InsertOptions { SaveMode = SaveMode.Append }, CancellationToken.None);
            using var arrow = await table.ReadAsArrowTableAsync(CancellationToken.None);
            Assert.Equal(2, arrow.Table.RowCount);
            Assert.Equal(2, provider.Calls);
            Assert.Contains(server.Requests, request => request.Method == "PUT" && request.Path.EndsWith("00000000000000000000.json", StringComparison.Ordinal) && request.Authorization == "Bearer A.synthetic==");
            Assert.Contains(server.Requests, request => request.Method == "PUT" && request.Path.EndsWith(".parquet", StringComparison.Ordinal) && request.Authorization == "Bearer A.synthetic==");
            Assert.Contains(server.Requests, request => request.Method == "GET" && request.Path.EndsWith(".parquet", StringComparison.Ordinal) && request.Authorization == "Bearer B.synthetic==");
            table.Dispose();
            Assert.False(provider.Disposed);
        }
        finally
        {
            await server.DisposeAsync();
        }
    }

    [Fact]
    public async Task GivenCdfFixture_WhenChangesAreRead_KernelUsesBForChangeDataParquet()
    {
        var server = new KernelAzureTestServer();
        try
        {
            var source = TableIdentifier.SimpleTableWithCdc.TablePath();
            foreach (var file in Directory.GetFiles(source, "*", SearchOption.AllDirectories))
            {
                var destination = Path.Combine(server.Root.FullName, KernelAzureTestServer.RelativePath(Path.GetFullPath(source), Path.GetFullPath(file)).Replace('/', Path.DirectorySeparatorChar));
                Directory.CreateDirectory(Path.GetDirectoryName(destination)!);
                File.Copy(file, destination);
            }

            using IEngine engine = new DeltaEngine(EngineOptions.Default);
            var provider = new RecordingProvider();
            using ITable table = await engine.LoadTableAsync(Options(server, provider), CancellationToken.None);
            var changes = new List<(string Kind, string? Name, long? Version)>();
            await foreach (var batch in table.QueryTableChangesAsync(new TableChangesOptions(0) { EndVersion = 2 }, CancellationToken.None))
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

            Assert.Equal(3, changes.Count);
            Assert.Contains(("insert", "Mario", (long?)1), changes);
            Assert.Contains(("update_preimage", "Mario", (long?)2), changes);
            Assert.Contains(("update_postimage", "Mino", (long?)2), changes);
            Assert.Equal(2, provider.Calls);
            Assert.Contains(server.Requests, request => request.Method == "GET" && request.Path.Contains("/_change_data/") && request.Authorization == "Bearer B.synthetic==");
        }
        finally
        {
            await server.DisposeAsync();
        }
    }

    [Fact]
    public async Task GivenReturnedArrowAndFrame_WhenTableIsDisposedAndCollected_DataAndCallerProviderRemainAlive()
    {
        var server = new KernelAzureTestServer();
        try
        {
            await SeedAsync(server);
            using IEngine engine = new DeltaEngine(EngineOptions.Default);
            var provider = new RecordingProvider();
            using ITable table = await engine.LoadTableAsync(Options(server, provider), CancellationToken.None);
            using var arrow = await table.ReadAsArrowTableAsync(CancellationToken.None);
            using var frame = await table.ReadAsDataFrameAsync(CancellationToken.None);
            table.Dispose();
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            Assert.Equal(3, arrow.Table.RowCount);
            var values = (Int32Array)arrow.Table.Column(0).Data.ArrowArray(0);
            Assert.Equal(2, values.GetValue(2));
            Assert.Equal("2", frame.Frame[2, 1]);
            Assert.False(provider.Disposed);
            arrow.Dispose();
            frame.Dispose();
            Assert.False(provider.Disposed);
        }
        finally
        {
            await server.DisposeAsync();
        }
    }

    [Fact]
    public async Task GivenKernelProviderFault_WhenReadIsAttempted_ErrorIsSanitizedAndBridgeRemainsCallable()
    {
        var server = new KernelAzureTestServer();
        try
        {
            await SeedAsync(server);
            using IEngine engine = new DeltaEngine(EngineOptions.Default);
            var provider = new RecordingProvider(call => call == 1
                ? new AzureBearerToken("A.synthetic==", DateTimeOffset.UtcNow.AddMinutes(36))
                : throw new InvalidOperationException("B.synthetic== provider-sensitive-detail"));
            using ITable table = await engine.LoadTableAsync(Options(server, provider), CancellationToken.None);
            var beforeFailure = server.Requests.Length;
            var error = await Assert.ThrowsAsync<KernelException>(async () =>
            {
                using var arrow = await table.ReadAsArrowTableAsync(CancellationToken.None);
            });
            Assert.DoesNotContain("B.synthetic==", error.ToString());
            Assert.DoesNotContain("provider-sensitive-detail", error.ToString());
            Assert.Null(error.InnerException);
            Assert.Equal(beforeFailure, server.Requests.Length);
            Assert.Equal(2, provider.Calls);
            Assert.NotEmpty(await table.HistoryAsync(null, CancellationToken.None));
            Assert.Equal(2, provider.Calls);
            Assert.All(server.Requests, request => Assert.Equal("Bearer A.synthetic==", request.Authorization));
            table.Dispose();
            Assert.False(provider.Disposed);
        }
        finally
        {
            await server.DisposeAsync();
        }
    }

    private static async Task SeedAsync(KernelAzureTestServer server)
    {
        var seed = await TableHelpers.SetupTable(DirectoryHelpers.ToFileUri(server.Root.FullName), 3);
        seed.table.Dispose();
        seed.engine.Dispose();
    }

    private static TableOptions Options(KernelAzureTestServer server, IAzureBearerTokenProvider? provider)
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
            KernelAzureBearerCredential = provider == null ? null : new KernelAzureBearerCredentialOptions(provider),
        };
    }

    private sealed class RecordingProvider : IAzureBearerTokenProvider, IDisposable
    {
        private readonly Func<int, AzureBearerToken>? acquire;
        private readonly Action<int>? beforeReturn;
        private int calls;

        internal RecordingProvider(Func<int, AzureBearerToken>? acquire = null, Action<int>? beforeReturn = null)
        {
            this.acquire = acquire;
            this.beforeReturn = beforeReturn;
        }

        internal int Calls => Volatile.Read(ref calls);

        internal bool Disposed { get; private set; }

        public Task<AzureBearerToken> GetTokenAsync(AzureTokenRequestContext context, CancellationToken cancellationToken)
        {
            Assert.Equal(TimeSpan.FromSeconds(90), context.MinimumLifetime);
            Assert.Equal(new[] { "https://storage.azure.com/.default" }, context.Scopes);
            cancellationToken.ThrowIfCancellationRequested();
            var call = Interlocked.Increment(ref calls);
            beforeReturn?.Invoke(call);
            return Task.FromResult(acquire?.Invoke(call)
                ?? new AzureBearerToken(call == 1 ? "A.synthetic==" : "B.synthetic==", DateTimeOffset.UtcNow.AddMinutes(36)));
        }

        public void Dispose()
        {
            Disposed = true;
        }
    }
}