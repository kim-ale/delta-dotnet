namespace DeltaLake.Table
{
    /// <summary>
    /// A table feature that can be added to a Delta table protocol.
    /// </summary>
    /// <remarks>
    /// Adding a feature updates protocol support only. Features can require additional table
    /// properties, schema changes, or other setup before their behavior is enabled.
    /// </remarks>
    public enum TableFeature
    {
        /// <summary>Column mapping.</summary>
        ColumnMapping,

        /// <summary>Deletion vectors.</summary>
        DeletionVectors,

        /// <summary>Timestamps without a time zone.</summary>
        TimestampWithoutTimezone,

        /// <summary>V2 checkpoints, including checkpoint sidecars.</summary>
        V2Checkpoint,

        /// <summary>Append-only table enforcement.</summary>
        AppendOnly,

        /// <summary>Invariant enforcement.</summary>
        Invariants,

        /// <summary>Check constraints.</summary>
        CheckConstraints,

        /// <summary>Change data feed.</summary>
        ChangeDataFeed,

        /// <summary>Generated columns.</summary>
        GeneratedColumns,

        /// <summary>Identity columns.</summary>
        IdentityColumns,

        /// <summary>Row tracking.</summary>
        RowTracking,

        /// <summary>Domain metadata actions.</summary>
        DomainMetadata,

        /// <summary>Iceberg compatibility version 1.</summary>
        IcebergCompatV1,

        /// <summary>Materialized partition columns.</summary>
        MaterializePartitionColumns,
    }
}