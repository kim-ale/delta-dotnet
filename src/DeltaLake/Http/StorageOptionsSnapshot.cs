using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using DeltaLake.Table;

namespace DeltaLake.Http
{
    internal static class StorageOptionsSnapshot
    {
        private static readonly ConditionalWeakTable<TableStorageOptions, object> Snapshots = new();

        internal static TableOptions Capture(TableOptions options)
        {
            Validate(options);
            if (options.RequestHeaderProvider == null || Snapshots.TryGetValue(options, out _)) return options;
            var snapshot = options with
            {
                StorageOptions = Copy(options.StorageOptions) ?? new Dictionary<string, string>()
            };
            Snapshots.Add(snapshot, new object());
            return snapshot;
        }

        internal static TableCreateOptions Capture(TableCreateOptions options)
        {
            Validate(options);
            if (options.RequestHeaderProvider == null || Snapshots.TryGetValue(options, out _)) return options;
            var snapshot = options with
            {
                StorageOptions = Copy(options.StorageOptions) ?? new Dictionary<string, string>(),
                Configuration = Copy(options.Configuration),
                CustomMetadata = Copy(options.CustomMetadata),
                PartitionBy = new List<string>(options.PartitionBy)
            };
            Snapshots.Add(snapshot, new object());
            return snapshot;
        }

        internal static void Validate(TableStorageOptions options)
        {
            if (options.RequestHeaderProvider == null) return;
            if (!Uri.TryCreate(options.TableLocation, UriKind.Absolute, out var uri) ||
                !(uri.Scheme == "az" || uri.Scheme == "adl" || uri.Scheme == "azure" ||
                  uri.Scheme == "abfs" || uri.Scheme == "abfss"))
            {
                throw new NotSupportedException("Request header providers require a supported Azure storage scheme.");
            }
        }

        private static Dictionary<string, string>? Copy(Dictionary<string, string>? source) =>
            source == null ? null : new Dictionary<string, string>(source, source.Comparer);
    }
}