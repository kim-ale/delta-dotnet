namespace DeltaLake.Errors
{
    /// <summary>
    /// Identifies the kind of failure reported by the delta lake runtime.
    /// </summary>
    /// <remarks>
    /// Values mirror the runtime's own error codes, so they are deliberately non-contiguous and must
    /// not be treated as a range. A <see cref="DeltaRuntimeException"/> exposes the current value
    /// through <see cref="DeltaRuntimeException.Code"/>.
    /// </remarks>
    public enum DeltaTableErrorCode
    {
        /// <summary>A string could not be decoded as UTF-8.</summary>
        Utf8 = 0,

        /// <summary>The underlying object store reported a failure.</summary>
        ObjectStore = 2,

        /// <summary>Reading or writing parquet failed.</summary>
        Parquet = 3,

        /// <summary>Converting to or from Arrow failed.</summary>
        Arrow = 4,

        /// <summary>A delta log entry contained malformed JSON.</summary>
        InvalidJsonLog = 5,

        /// <summary>A statistics record contained malformed JSON.</summary>
        InvalidStatsJson = 6,

        /// <summary>The requested table version is not valid for this table.</summary>
        InvalidVersion = 8,

        /// <summary>A date or time string could not be parsed.</summary>
        InvalidDateTimeString = 10,

        /// <summary>The supplied data failed validation.</summary>
        InvalidData = 11,

        /// <summary>The location does not contain a delta table.</summary>
        NotATable = 12,

        /// <summary>The table metadata does not define a schema.</summary>
        NoSchema = 14,

        /// <summary>The supplied schema does not match the table schema.</summary>
        SchemaMismatch = 16,

        /// <summary>A partition could not be resolved or applied.</summary>
        PartitionError = 17,

        /// <summary>A partition filter was malformed.</summary>
        InvalidPartitionFilter = 18,

        /// <summary>An I/O operation failed.</summary>
        Io = 20,

        /// <summary>A transaction could not be completed, typically because of a conflicting commit.</summary>
        Transaction = 21,

        /// <summary>The commit targeted a version that already exists.</summary>
        VersionAlreadyExists = 22,

        /// <summary>The table version did not match the expected version.</summary>
        VersionMismatch = 23,

        /// <summary>The table requires a reader or writer feature that is not supported.</summary>
        MissingFeature = 24,

        /// <summary>The table location or URI is not valid.</summary>
        InvalidTableLocation = 25,

        /// <summary>A log entry could not be serialized to JSON.</summary>
        SerializeLogJson = 26,

        /// <summary>
        /// An uncategorized failure.
        /// </summary>
        /// <remarks>
        /// Several distinct runtime failures collapse into this value, including change data feed
        /// errors, so the message is currently the only way to tell them apart.
        /// </remarks>
        Generic = 28,

        /// <summary>An uncategorized failure that carries an underlying source error.</summary>
        GenericError = 29,

        /// <summary>
        /// The delta kernel reported a failure.
        /// </summary>
        /// <remarks>
        /// This is a category rather than a specific failure; the kernel's own error kind is not
        /// currently carried across the runtime boundary.
        /// </remarks>
        Kernel = 30,

        /// <summary>The table metadata could not be read or was invalid.</summary>
        MetaDataError = 31,

        /// <summary>The table has not been initialized.</summary>
        NotInitialized = 32,

        /// <summary>The operation was canceled.</summary>
        OperationCanceled = 33,

        /// <summary>Query execution failed.</summary>
        DataFusion = 34,

        /// <summary>A SQL statement could not be parsed.</summary>
        SqlParser = 35,

        /// <summary>A timestamp value was not valid.</summary>
        InvalidTimestamp = 36,
    }
}
