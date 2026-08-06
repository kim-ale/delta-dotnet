using System.Text.Json;
using DeltaLake.Errors;
using DeltaLake.Kernel.Callbacks.Errors;
using DeltaLake.Table;

namespace DeltaLake.Tests.Table;

public class TableFeatureTests
{
    public static IEnumerable<object[]> LegacyWriterFeatures()
    {
        yield return [1, 2, new[] { "v2Checkpoint" }, new[] { "appendOnly", "invariants", "v2Checkpoint" }];
        yield return [1, 3, new[] { "v2Checkpoint" }, new[] { "appendOnly", "checkConstraints", "invariants", "v2Checkpoint" }];
        yield return [1, 4, new[] { "v2Checkpoint" }, new[] { "appendOnly", "changeDataFeed", "checkConstraints", "generatedColumns", "invariants", "v2Checkpoint" }];
        yield return [1, 5, new[] { "columnMapping", "v2Checkpoint" }, new[] { "appendOnly", "changeDataFeed", "checkConstraints", "columnMapping", "generatedColumns", "invariants", "v2Checkpoint" }];
        yield return [1, 6, new[] { "columnMapping", "v2Checkpoint" }, new[] { "appendOnly", "changeDataFeed", "checkConstraints", "columnMapping", "generatedColumns", "identityColumns", "invariants", "v2Checkpoint" }];
        yield return [2, 2, new[] { "columnMapping", "v2Checkpoint" }, new[] { "appendOnly", "columnMapping", "invariants", "v2Checkpoint" }];
    }

    public static IEnumerable<object[]> BridgeMutationCases()
    {
        yield return ["Insert"];
        yield return ["Update"];
        yield return ["Delete"];
        yield return ["Merge"];
    }

    [Fact]
    public async Task AddTableFeaturesAsync_DefaultProtocolIncrease_FailsWithoutCommit()
    {
        var info = DirectoryHelpers.CreateTempSubdirectory();
        try
        {
            var data = await TableHelpers.SetupTable(DirectoryHelpers.ToFileUri(info.FullName), 1);
            using var engine = data.engine;
            using var table = data.table;
            var version = table.Version();

            var exception = await Assert.ThrowsAsync<KernelException>(
                () => table.AddTableFeaturesAsync(
                    [TableFeature.V2Checkpoint],
                    CancellationToken.None));

            Assert.Contains("allow_protocol_versions_increase", exception.Message);
            Assert.Equal(version, table.Version());
        }
        finally
        {
            info.Delete(true);
        }
    }

    [Fact]
    public async Task AddTableFeaturesAsync_ProtocolIncreaseAndMetadata_CommitsV2CheckpointFeature()
    {
        var info = DirectoryHelpers.CreateTempSubdirectory();
        try
        {
            var data = await TableHelpers.SetupTable(DirectoryHelpers.ToFileUri(info.FullName), 1);
            using var engine = data.engine;
            using var table = data.table;

            await table.AddTableFeaturesAsync(
                [TableFeature.V2Checkpoint],
                new AddTableFeatureOptions
                {
                    AllowProtocolVersionsIncrease = true,
                    CustomMetadata = new Dictionary<string, string>
                    {
                        ["workItem"] = "add-table-features",
                    },
                },
                CancellationToken.None);

            Assert.Equal(2UL, table.Version());
            var protocol = table.ProtocolVersions();
            Assert.Equal(3, protocol.MinimumReaderVersion);
            Assert.Equal(7, protocol.MinimumWriterVersion);

            var commitPath = Path.Join(info.FullName, "_delta_log", "00000000000000000002.json");
            var protocolAction = ReadAction(commitPath, "protocol");
            Assert.Equal(
                ["v2Checkpoint"],
                ReadFeatureSet(protocolAction, "readerFeatures"));
            Assert.Equal(
                ["appendOnly", "invariants", "v2Checkpoint"],
                ReadFeatureSet(protocolAction, "writerFeatures"));

            var commitInfo = ReadAction(commitPath, "commitInfo");
            Assert.Equal("ADD FEATURE", commitInfo.GetProperty("operation").GetString());
            Assert.True(commitInfo.TryGetProperty("kernelVersion", out _));
            Assert.Equal("add-table-features", commitInfo.GetProperty("workItem").GetString());
            Assert.Equal(
                ["commitInfo", "protocol"],
                ReadActionNames(commitPath).OrderBy(name => name, StringComparer.Ordinal));
        }
        finally
        {
            info.Delete(true);
        }
    }

    [Theory]
    [MemberData(nameof(LegacyWriterFeatures))]
    public async Task AddTableFeaturesAsync_LegacyProtocol_PreservesImpliedWriterFeatures(
        int minReaderVersion,
        int minWriterVersion,
        string[] expectedReaderFeatures,
        string[] expectedWriterFeatures)
    {
        var info = DirectoryHelpers.CreateTempSubdirectory();
        try
        {
            using var schemaBatch = TableHelpers.BuildBasicRecordBatch(0);
            using var engine = new DeltaEngine(EngineOptions.Default);
            using var table = await engine.CreateTableAsync(
                new TableCreateOptions(DirectoryHelpers.ToFileUri(info.FullName), schemaBatch.Schema)
                {
                    Configuration = new Dictionary<string, string>
                    {
                        ["delta.minReaderVersion"] = minReaderVersion.ToString(),
                        ["delta.minWriterVersion"] = minWriterVersion.ToString(),
                    },
                },
                CancellationToken.None);

            await table.AddTableFeaturesAsync(
                [TableFeature.V2Checkpoint],
                new AddTableFeatureOptions { AllowProtocolVersionsIncrease = true },
                CancellationToken.None);

            var protocolAction = ReadAction(
                Path.Join(info.FullName, "_delta_log", "00000000000000000001.json"),
                "protocol");
            Assert.Equal(expectedReaderFeatures, ReadFeatureSet(protocolAction, "readerFeatures"));
            Assert.Equal(expectedWriterFeatures, ReadFeatureSet(protocolAction, "writerFeatures"));
        }
        finally
        {
            info.Delete(true);
        }
    }

    [Fact]
    public async Task AddTableFeaturesAsync_PreCancelled_DoesNotCommit()
    {
        var info = DirectoryHelpers.CreateTempSubdirectory();
        try
        {
            var data = await TableHelpers.SetupTable(DirectoryHelpers.ToFileUri(info.FullName), 1);
            using var engine = data.engine;
            using var table = data.table;
            var version = table.Version();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => table.AddTableFeaturesAsync(
                    [TableFeature.V2Checkpoint],
                    new AddTableFeatureOptions { AllowProtocolVersionsIncrease = true },
                    new CancellationToken(true)));

            Assert.Equal(version, table.Version());
        }
        finally
        {
            info.Delete(true);
        }
    }

    [Fact]
    public async Task AddTableFeaturesAsync_ValidMemoryTable_ThrowsNotSupportedException()
    {
        var data = await TableHelpers.SetupTable($"memory:///{Guid.NewGuid():N}", 1);
        using var engine = data.engine;
        using var table = data.table;
        var version = table.Version();

        var exception = await Assert.ThrowsAsync<NotSupportedException>(
            () => table.AddTableFeaturesAsync(
                [TableFeature.V2Checkpoint],
                new AddTableFeatureOptions { AllowProtocolVersionsIncrease = true },
                CancellationToken.None));

        Assert.Contains("kernel-backed table", exception.Message);
        Assert.Equal(version, table.Version());
    }

    [Fact]
    public async Task AddTableFeaturesAsync_NullFeatures_ThrowsArgumentNullException()
    {
        var data = await TableHelpers.SetupTable($"memory:///{Guid.NewGuid():N}", 1);
        using var engine = data.engine;
        using var table = data.table;

        await Assert.ThrowsAsync<ArgumentNullException>(
            () => table.AddTableFeaturesAsync(null!, CancellationToken.None));
    }

    [Fact]
    public async Task AddTableFeaturesAsync_NullOptions_ThrowsArgumentNullException()
    {
        var data = await TableHelpers.SetupTable($"memory:///{Guid.NewGuid():N}", 1);
        using var engine = data.engine;
        using var table = data.table;

        await Assert.ThrowsAsync<ArgumentNullException>(
            () => table.AddTableFeaturesAsync(
                [TableFeature.V2Checkpoint],
                null!,
                CancellationToken.None));
    }

    [Fact]
    public async Task AddTableFeaturesAsync_EmptyFeatures_ThrowsConfigurationException()
    {
        var data = await TableHelpers.SetupTable($"memory:///{Guid.NewGuid():N}", 1);
        using var engine = data.engine;
        using var table = data.table;
        var version = table.Version();

        var exception = await Assert.ThrowsAsync<DeltaConfigurationException>(
            () => table.AddTableFeaturesAsync([], CancellationToken.None));

        Assert.IsType<ArgumentException>(exception.InnerException);
        Assert.Equal(version, table.Version());
    }

    [Fact]
    public async Task AddTableFeaturesAsync_InvalidEnum_ThrowsArgumentOutOfRangeException()
    {
        var data = await TableHelpers.SetupTable($"memory:///{Guid.NewGuid():N}", 1);
        using var engine = data.engine;
        using var table = data.table;
        var version = table.Version();

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => table.AddTableFeaturesAsync(
                [(TableFeature)int.MaxValue],
                new AddTableFeatureOptions { AllowProtocolVersionsIncrease = true },
                CancellationToken.None));

        Assert.Equal(version, table.Version());
    }

    [Fact]
    public async Task AddTableFeaturesAsync_UnsupportedFeatures_ThrowWithoutCommit()
    {
        var data = await TableHelpers.SetupTable($"memory:///{Guid.NewGuid():N}", 1);
        using var engine = data.engine;
        using var table = data.table;
        var version = table.Version();
        var unsupportedFeatures = Enum.GetValues<TableFeature>()
            .Where(feature => feature != TableFeature.V2Checkpoint);

        foreach (var feature in unsupportedFeatures)
        {
            var exception = await Assert.ThrowsAsync<NotSupportedException>(
                () => table.AddTableFeaturesAsync(
                    [feature],
                    new AddTableFeatureOptions { AllowProtocolVersionsIncrease = true },
                    CancellationToken.None));

            Assert.Contains(feature.ToString(), exception.Message);
            Assert.Equal(version, table.Version());
        }
    }

    [Theory]
    [MemberData(nameof(BridgeMutationCases))]
    public async Task V2Checkpoint_BridgeMutation_RejectsWithoutCommit(string operation)
    {
        var info = DirectoryHelpers.CreateTempSubdirectory();
        try
        {
            var path = DirectoryHelpers.ToFileUri(info.FullName);
            {
                var data = await TableHelpers.SetupTable(path, 3);
                using var engine = data.engine;
                using var table = data.table;
                await table.AddTableFeaturesAsync(
                    [TableFeature.V2Checkpoint],
                    new AddTableFeatureOptions { AllowProtocolVersionsIncrease = true },
                    CancellationToken.None);
            }

            using var reopenedEngine = new DeltaEngine(EngineOptions.Default);
            using var reopened = await reopenedEngine.LoadTableAsync(
                new TableOptions { TableLocation = path },
                CancellationToken.None);
            var version = reopened.Version();

            var exception = await Assert.ThrowsAsync<DeltaRuntimeException>(
                () => InvokeBridgeMutationAsync(reopened, operation));

            Assert.Contains("Unsupported table features", exception.Message);
            Assert.Equal(version, reopened.Version());
        }
        finally
        {
            info.Delete(true);
        }
    }

    private static async Task InvokeBridgeMutationAsync(
        DeltaLake.Interfaces.ITable table,
        string operation)
    {
        switch (operation)
        {
            case "Insert":
                using (var batch = TableHelpers.BuildBasicRecordBatch(1))
                {
                    await table.InsertAsync(
                        [batch],
                        table.Schema(),
                        new InsertOptions { SaveMode = SaveMode.Append },
                        CancellationToken.None);
                }
                break;
            case "Update":
                await table.UpdateAsync(
                    "UPDATE test SET test = test + CAST(1 AS INT)",
                    CancellationToken.None);
                break;
            case "Delete":
                await table.DeleteAsync("test = CAST(0 AS INT)", CancellationToken.None);
                break;
            case "Merge":
                using (var batch = TableHelpers.BuildBasicRecordBatch(1))
                {
                    await table.MergeAsync(
                        "MERGE INTO mytable USING newdata ON mytable.test = newdata.test WHEN MATCHED THEN DELETE",
                        [batch],
                        batch.Schema,
                        CancellationToken.None);
                }
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(operation), operation, null);
        }
    }

    private static JsonElement ReadAction(string commitPath, string actionName)
    {
        foreach (var line in File.ReadLines(commitPath))
        {
            using var document = JsonDocument.Parse(line);
            if (document.RootElement.TryGetProperty(actionName, out var action))
            {
                return action.Clone();
            }
        }

        throw new InvalidOperationException($"Commit did not contain a {actionName} action.");
    }

    private static IEnumerable<string> ReadActionNames(string commitPath)
    {
        foreach (var line in File.ReadLines(commitPath))
        {
            using var document = JsonDocument.Parse(line);
            foreach (var action in document.RootElement.EnumerateObject())
            {
                yield return action.Name;
            }
        }
    }

    private static string[] ReadFeatureSet(JsonElement protocolAction, string propertyName) =>
        protocolAction
            .GetProperty(propertyName)
            .EnumerateArray()
            .Select(value => value.GetString()!)
            .OrderBy(value => value, StringComparer.Ordinal)
            .ToArray();
}