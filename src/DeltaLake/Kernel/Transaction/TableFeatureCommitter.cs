// -----------------------------------------------------------------------------
// <summary>
// Handles the FFI lifecycle for committing table features through Delta Kernel.
// </summary>
//
// <copyright company="The Delta Lake Project Authors">
// Copyright (2024) The Delta Lake Project Authors.  All rights reserved.
// Licensed under the Apache license. See LICENSE file in the project root for full license information.
// </copyright>
// -----------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;
using DeltaLake.Kernel.Callbacks.Errors;
using DeltaLake.Kernel.Interop;
using DeltaLake.Table;
using Methods = DeltaLake.Kernel.Interop.Methods;

namespace DeltaLake.Kernel.Transaction
{
    /// <summary>
    /// Commits table-feature protocol changes and returns an independently owned post-commit
    /// snapshot for installation in managed table state.
    /// </summary>
    internal static class TableFeatureCommitter
    {
        private const int V2CheckpointFeatureCode = 0;

        /// <summary>
        /// Commits the requested table features through Delta Kernel.
        /// </summary>
        /// <param name="snapshotPtr">The borrowed transaction-base snapshot.</param>
        /// <param name="enginePtr">The shared external engine.</param>
        /// <param name="features">The validated managed feature collection.</param>
        /// <param name="allowProtocolVersionsIncrease">Whether legacy protocol versions may be promoted.</param>
        /// <param name="customMetadata">Optional custom CommitInfo string metadata.</param>
        /// <param name="postCommitSnapshot">
        /// Receives one independently owned snapshot handle. The caller must install or free it.
        /// </param>
        /// <returns>The durably committed table version.</returns>
        internal static unsafe ulong Commit(
            SharedSnapshot* snapshotPtr,
            SharedExternEngine* enginePtr,
            IReadOnlyCollection<TableFeature> features,
            bool allowProtocolVersionsIncrease,
            IReadOnlyDictionary<string, string>? customMetadata,
            out SharedSnapshot* postCommitSnapshot)
        {
            int[] featureCodes = BuildFeatureCodes(features);
            MetadataBuffer[] metadataBuffers = BuildMetadataBuffers(customMetadata);
            FfiCommitInfoEntry[] metadataEntries = BuildMetadataEntries(metadataBuffers);
            ExclusiveCommittedTransaction* committedTxnPtr = null;
            postCommitSnapshot = null;

            try
            {
                fixed (int* featureCodesPtr = featureCodes)
                fixed (FfiCommitInfoEntry* metadataEntriesPtr = metadataEntries)
                {
                    ExternResultHandleExclusiveCommittedTransaction commitResult =
                        Methods.add_table_features(
                            snapshotPtr,
                            enginePtr,
                            featureCodesPtr,
                            (ulong)featureCodes.Length,
                            allowProtocolVersionsIncrease,
                            metadataEntries.Length == 0 ? null : metadataEntriesPtr,
                            (ulong)metadataEntries.Length);

                    if (commitResult.tag != ExternResultHandleExclusiveCommittedTransaction_Tag.OkHandleExclusiveCommittedTransaction)
                    {
                        throw KernelException.FromEngineError(
                            commitResult.Anonymous.Anonymous2.err,
                            "Failed to add table features via Delta Kernel");
                    }

                    committedTxnPtr = commitResult.Anonymous.Anonymous1.ok;
                }

                ulong committedVersion = Methods.committed_transaction_version(&committedTxnPtr);
                OptionalValueHandleSharedSnapshot snapshotResult =
                    Methods.committed_transaction_post_commit_snapshot(&committedTxnPtr);
                if (snapshotResult.tag != OptionalValueHandleSharedSnapshot_Tag.SomeHandleSharedSnapshot
                    || snapshotResult.Anonymous.Anonymous.some == null)
                {
                    throw new InvalidOperationException(
                        $"Delta Kernel committed table features at version {committedVersion}, " +
                        "but did not return the required post-commit snapshot. The commit was not rolled back.");
                }

                postCommitSnapshot = snapshotResult.Anonymous.Anonymous.some;
                return committedVersion;
            }
            finally
            {
                foreach (MetadataBuffer metadataBuffer in metadataBuffers)
                {
                    metadataBuffer.Dispose();
                }

                if (committedTxnPtr != null)
                {
                    Methods.free_committed_transaction(committedTxnPtr);
                }
            }
        }

        private static int[] BuildFeatureCodes(IReadOnlyCollection<TableFeature> features)
        {
            var featureCodes = new int[features.Count];
            int index = 0;
            foreach (TableFeature feature in features)
            {
                featureCodes[index++] = feature switch
                {
                    TableFeature.V2Checkpoint => V2CheckpointFeatureCode,
                    _ => throw new NotSupportedException(
                        $"Table feature '{feature}' is not supported by the Delta Kernel feature committer."),
                };
            }

            return featureCodes;
        }

        private static MetadataBuffer[] BuildMetadataBuffers(
            IReadOnlyDictionary<string, string>? customMetadata)
        {
            if (customMetadata == null || customMetadata.Count == 0)
            {
                return Array.Empty<MetadataBuffer>();
            }

            var buffers = new MetadataBuffer[customMetadata.Count];
            int index = 0;
            try
            {
                foreach (KeyValuePair<string, string> entry in customMetadata)
                {
                    buffers[index++] = new MetadataBuffer(entry.Key, entry.Value);
                }

                return buffers;
            }
            catch
            {
                for (int bufferIndex = 0; bufferIndex < index; bufferIndex++)
                {
                    buffers[bufferIndex].Dispose();
                }
                throw;
            }
        }

        private static unsafe FfiCommitInfoEntry[] BuildMetadataEntries(MetadataBuffer[] buffers)
        {
            var entries = new FfiCommitInfoEntry[buffers.Length];
            for (int index = 0; index < buffers.Length; index++)
            {
                entries[index] = new FfiCommitInfoEntry
                {
                    key = buffers[index].KeySlice,
                    value = buffers[index].ValueSlice,
                };
            }

            return entries;
        }

        private sealed class MetadataBuffer : IDisposable
        {
            private GCHandle keyHandle;
            private GCHandle valueHandle;

            internal unsafe MetadataBuffer(string key, string value)
            {
                byte[] keyBytes = Encoding.UTF8.GetBytes(key);
                byte[] valueBytes = Encoding.UTF8.GetBytes(value);

                try
                {
                    this.keyHandle = GCHandle.Alloc(keyBytes, GCHandleType.Pinned);
                    this.valueHandle = GCHandle.Alloc(valueBytes, GCHandleType.Pinned);
                    this.KeySlice = new KernelStringSlice
                    {
                        ptr = keyBytes.Length == 0
                            ? null
                            : (sbyte*)this.keyHandle.AddrOfPinnedObject().ToPointer(),
                        len = (ulong)keyBytes.Length,
                    };
                    this.ValueSlice = new KernelStringSlice
                    {
                        ptr = valueBytes.Length == 0
                            ? null
                            : (sbyte*)this.valueHandle.AddrOfPinnedObject().ToPointer(),
                        len = (ulong)valueBytes.Length,
                    };
                }
                catch
                {
                    this.Dispose();
                    throw;
                }
            }

            internal KernelStringSlice KeySlice { get; }

            internal KernelStringSlice ValueSlice { get; }

            public void Dispose()
            {
                if (this.keyHandle.IsAllocated)
                {
                    this.keyHandle.Free();
                }
                if (this.valueHandle.IsAllocated)
                {
                    this.valueHandle.Free();
                }
            }
        }
    }
}
