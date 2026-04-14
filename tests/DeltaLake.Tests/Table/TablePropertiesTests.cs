using DeltaLake.Errors;
using DeltaLake.Table;

namespace DeltaLake.Tests.Table
{
    public class TablePropertiesTests
    {
        [Fact]
        public async Task SetTableProperties_Memory_Test()
        {
            var tableParts = await TableHelpers.SetupTable($"memory:///{Guid.NewGuid():N}", 0);
            using var table = tableParts.table;
            var properties = new Dictionary<string, string>
            {
                ["delta.appendOnly"] = "true",
                ["delta.logRetentionDuration"] = "interval 30 days",
            };
            await table.SetTablePropertiesAsync(properties, CancellationToken.None);

            var metadata = table.Metadata();
            Assert.Contains("delta.appendOnly", metadata.Configuration.Keys);
            Assert.Equal("true", metadata.Configuration["delta.appendOnly"]);
        }

        [Fact]
        public async Task SetTableProperties_WithOptions_Memory_Test()
        {
            var tableParts = await TableHelpers.SetupTable($"memory:///{Guid.NewGuid():N}", 0);
            using var table = tableParts.table;
            var properties = new Dictionary<string, string>
            {
                ["delta.appendOnly"] = "true",
            };
            await table.SetTablePropertiesAsync(
                properties,
                raiseIfNotExists: true,
                customMetadata: null,
                CancellationToken.None);

            var metadata = table.Metadata();
            Assert.Contains("delta.appendOnly", metadata.Configuration.Keys);
            Assert.Equal("true", metadata.Configuration["delta.appendOnly"]);
        }

        [Fact]
        public async Task UpdateTableMetadata_Name_Memory_Test()
        {
            var tableParts = await TableHelpers.SetupTable($"memory:///{Guid.NewGuid():N}", 0);
            using var table = tableParts.table;
            await table.UpdateTableMetadataAsync("my_test_table", null, CancellationToken.None);

            var metadata = table.Metadata();
            Assert.Equal("my_test_table", metadata.Name);
        }

        [Fact]
        public async Task UpdateTableMetadata_Description_Memory_Test()
        {
            var tableParts = await TableHelpers.SetupTable($"memory:///{Guid.NewGuid():N}", 0);
            using var table = tableParts.table;
            await table.UpdateTableMetadataAsync(null, "A test description", CancellationToken.None);

            var metadata = table.Metadata();
            Assert.Equal("A test description", metadata.Description);
        }

        [Fact]
        public async Task UpdateTableMetadata_NameAndDescription_Memory_Test()
        {
            var tableParts = await TableHelpers.SetupTable($"memory:///{Guid.NewGuid():N}", 0);
            using var table = tableParts.table;
            await table.UpdateTableMetadataAsync("renamed_table", "Updated description", CancellationToken.None);

            var metadata = table.Metadata();
            Assert.Equal("renamed_table", metadata.Name);
            Assert.Equal("Updated description", metadata.Description);
        }

        [Fact]
        public async Task SetTableProperties_Cancellation_Test()
        {
            var tableParts = await TableHelpers.SetupTable($"memory:///{Guid.NewGuid():N}", 0);
            using var table = tableParts.table;
            var source = new CancellationTokenSource();
            await source.CancelAsync();
            var properties = new Dictionary<string, string>
            {
                ["delta.appendOnly"] = "true",
            };
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
                table.SetTablePropertiesAsync(properties, source.Token));
        }

        [Fact]
        public async Task UpdateTableMetadata_Cancellation_Test()
        {
            var tableParts = await TableHelpers.SetupTable($"memory:///{Guid.NewGuid():N}", 0);
            using var table = tableParts.table;
            var source = new CancellationTokenSource();
            await source.CancelAsync();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
                table.UpdateTableMetadataAsync("test", null, source.Token));
        }
    }
}
