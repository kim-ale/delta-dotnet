using Apache.Arrow;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;
using DeltaLake.Interfaces;
using DeltaLake.Table;

namespace DeltaLake.Tests.Table;

public class DeltaTableTests
{
    public static IEnumerable<object[]> ProtocolFeatureVersions()
    {
        yield return [0UL, 1, 2, System.Array.Empty<string>(), System.Array.Empty<string>()];
        yield return [1UL, 3, 7, System.Array.Empty<string>(), new[] { "appendOnly", "invariants" }];
        yield return [2UL, 3, 7, new[] { "deletionVectors" }, new[] { "appendOnly", "deletionVectors", "invariants" }];
    }

    [Fact]
    public async Task Create_InMemory_Test()
    {
        var uri = $"memory:///{Guid.NewGuid():N}/";
        using IEngine engine = new DeltaEngine(EngineOptions.Default);
        var builder = new Apache.Arrow.Schema.Builder();
        builder.Field(fb =>
        {
            fb.Name("test");
            fb.DataType(Int32Type.Default);
            fb.Nullable(false);
        });
        var schema = builder.Build();
        using var table = await engine.CreateTableAsync(
            new TableCreateOptions(uri, schema)
            {
                Configuration = new Dictionary<string, string>
                {
                    ["delta.dataSkippingNumIndexedCols"] = "32",
                }
            },
            CancellationToken.None);
        Assert.NotNull(table);
        var version = table.Version();
        Assert.Equal(0UL, version);
        var location = table.Location();
        Assert.Equal(uri, location);
        var files = await table.FilesAsync();
        Assert.Empty(files);
        var fileUris = await table.FileUrisAsync();
        Assert.Empty(fileUris);
        var returnedSchema = table.Schema();
        Assert.NotNull(returnedSchema);
        Assert.Equal(schema.FieldsList.Count, returnedSchema.FieldsList.Count);
        var protocol = table.ProtocolVersions();
        Assert.True(protocol.MinimumReaderVersion > 0);
        Assert.True(protocol.MinimumWriterVersion > 0);
        Assert.Empty(protocol.ReaderFeatures);
        Assert.Empty(protocol.WriterFeatures);
    }

    [Fact]
    public async Task Create_Cancellation()
    {
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            async () =>
            {
                var uri = $"memory:///{Guid.NewGuid():N}";
                using IEngine engine = new DeltaEngine(EngineOptions.Default);
                var builder = new Apache.Arrow.Schema.Builder();
                builder.Field(fb =>
                {
                    fb.Name("test");
                    fb.DataType(Int32Type.Default);
                    fb.Nullable(false);
                });
                var schema = builder.Build();
                using var table = await engine.CreateTableAsync(
                    new TableCreateOptions(uri, schema)
                    {
                        Configuration = new Dictionary<string, string>
                        {
                            ["delta.dataSkippingNumIndexedCols"] = "32",
                        }
                    },
                    new CancellationToken(true));
            });
    }

    [Fact]
    public async Task Create_InMemory_With_Partitions_Test()
    {
        var uri = $"memory:///{Guid.NewGuid():N}/";
        using IEngine engine = new DeltaEngine(EngineOptions.Default);
        var builder = new Apache.Arrow.Schema.Builder();
        builder.Field(fb =>
        {
            fb.Name("test");
            fb.DataType(Int32Type.Default);
            fb.Nullable(false);
        })
        .Field(fb =>
        {
            fb.Name("second");
            fb.DataType(Int32Type.Default);
            fb.Nullable(false);
        });
        var schema = builder.Build();
        var createOptions = new TableCreateOptions(uri, schema)
        {
            Configuration = new Dictionary<string, string>
            {
                ["delta.dataSkippingNumIndexedCols"] = "32",
            },
            PartitionBy = { "test" },
            Name = "table",
            Description = "this table has a description",
            CustomMetadata = new Dictionary<string, string> { ["test"] = "something" },
            StorageOptions = new Dictionary<string, string> { ["something"] = "here" },
        };
        using var table = await engine.CreateTableAsync(
            createOptions,
            CancellationToken.None);
        Assert.NotNull(table);
        var version = table.Version();
        Assert.Equal(0UL, version);
        var location = table.Location();
        Assert.Equal(uri, location);
        var metadata = table.Metadata();
        Assert.Single(metadata.PartitionColumns);
        Assert.Equal("test", metadata.PartitionColumns[0]);
        Assert.Equal(createOptions.Name, metadata.Name);
        Assert.Equal(createOptions.Description, metadata.Description);
    }

    [Fact]
    public async Task Load_Table_Test()
    {
        var location = Path.Combine(Settings.TestRoot, "simple_table");
        using IEngine engine = new DeltaEngine(EngineOptions.Default);
        using var table = await engine.LoadTableAsync(new TableOptions() { TableLocation = location }, CancellationToken.None);
        Assert.Equal(4UL, table.Version());
    }

    [Theory]
    [MemberData(nameof(ProtocolFeatureVersions))]
    public async Task GivenExistingTable_WhenProtocolRead_ReturnsVersionedFeatures(
        ulong version,
        int minimumReaderVersion,
        int minimumWriterVersion,
        string[] readerFeatures,
        string[] writerFeatures)
    {
        using IEngine engine = new DeltaEngine(EngineOptions.Default);
        using ITable table = await engine.LoadTableAsync(
            new TableOptions
            {
                TableLocation = TableIdentifier.TableWithDeletionLogs.TablePath(),
                Version = version,
            },
            CancellationToken.None);

        ProtocolInfo protocol = table.ProtocolVersions();

        Assert.Equal(minimumReaderVersion, protocol.MinimumReaderVersion);
        Assert.Equal(minimumWriterVersion, protocol.MinimumWriterVersion);
        Assert.Equal(
            readerFeatures,
            protocol.ReaderFeatures.OrderBy(feature => feature, StringComparer.Ordinal));
        Assert.Equal(
            writerFeatures,
            protocol.WriterFeatures.OrderBy(feature => feature, StringComparer.Ordinal));
    }

    [Fact]
    public async Task Load_Cancellation_Test()
    {
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            var location = Path.Combine(Settings.TestRoot, "simple_table");
            using IEngine engine = new DeltaEngine(EngineOptions.Default);
            using var table = await engine.LoadTableAsync(new TableOptions() { TableLocation = location }, new CancellationToken(true));
            Assert.Equal(4UL, table.Version());
        });
    }

    [Fact]
    public async Task Table_Insert_Test()
    {
        var uri = $"memory:///{Guid.NewGuid():N}";
        using IEngine engine = new DeltaEngine(EngineOptions.Default);
        var builder = new Apache.Arrow.Schema.Builder();
        builder.Field(fb =>
        {
            fb.Name("test");
            fb.DataType(Int32Type.Default);
            fb.Nullable(false);
        });
        var schema = builder.Build();
        using var table = await engine.CreateTableAsync(
            new TableCreateOptions(uri, schema),
            CancellationToken.None);
        Assert.NotNull(table);
        int length = 10;
        var allocator = new NativeMemoryAllocator();
        var recordBatchBuilder = new RecordBatch.Builder(allocator)
            .Append("test", false, col => col.Int32(arr => arr.AppendRange(Enumerable.Range(0, length))));


        var options = new InsertOptions
        {
            SaveMode = SaveMode.Append,
        };
        await table.InsertAsync([recordBatchBuilder.Build()], schema, options, CancellationToken.None);
        var version = table.Version();
        var queryResult = table.QueryAsync(new SelectQuery("SELECT test FROM test WHERE test > 1")
        {
            TableAlias = "test",
        },
        CancellationToken.None).ToBlockingEnumerable().ToList();
        Assert.Equal(1UL, version);
        var resultCount = 0;
        foreach (var batch in queryResult)
        {
            Assert.Equal(1, batch.ColumnCount);
            var column = batch.Column(0);
            if (column is not Int32Array integers)
            {
                throw new Exception("expected int32 array and got " + column.GetType());
            }

            foreach (var intValue in integers)
            {
                Assert.NotNull(intValue);
                Assert.True(intValue.Value > 1);
                ++resultCount;
            }
        }

        Assert.Equal(8, resultCount);
        await foreach (var result in table.QueryAsync(new SelectQuery("SELECT test FROM test WHERE test = 1")
        {
            TableAlias = "test",
        },
        CancellationToken.None))
        {
            Assert.NotNull(result);
        }
        var history = await table.HistoryAsync(1, CancellationToken.None);
        Assert.Single(history);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => table.HistoryAsync(1, new CancellationToken(true)));
        history = await table.HistoryAsync(default, CancellationToken.None);
        Assert.Equal(2, history.Length);
    }
}