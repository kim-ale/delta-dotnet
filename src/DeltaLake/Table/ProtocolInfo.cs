using System;
using System.Collections.Generic;

namespace DeltaLake.Table
{
    /// <summary>
    /// Protocol information for a delta table
    /// </summary>
#pragma warning disable CA1815 // Override equals and operator equals on value types
    public readonly struct ProtocolInfo
#pragma warning restore CA1815 // Override equals and operator equals on value types
    {
        private readonly IReadOnlyList<string>? readerFeatures;
        private readonly IReadOnlyList<string>? writerFeatures;

        /// <summary>
        /// Initializes a new instance of the <see cref="ProtocolInfo"/> struct.
        /// </summary>
        /// <param name="minimumReaderVersion">The minimum reader version.</param>
        /// <param name="minimumWriterVersion">The minimum writer version.</param>
        /// <param name="readerFeatures">The explicit reader features.</param>
        /// <param name="writerFeatures">The explicit writer features.</param>
        internal ProtocolInfo(
            int minimumReaderVersion,
            int minimumWriterVersion,
            IReadOnlyList<string> readerFeatures,
            IReadOnlyList<string> writerFeatures)
        {
            this.MinimumReaderVersion = minimumReaderVersion;
            this.MinimumWriterVersion = minimumWriterVersion;
            this.readerFeatures = readerFeatures;
            this.writerFeatures = writerFeatures;
        }

        /// <summary>
        /// Table minimum reader version
        /// </summary>
        public int MinimumReaderVersion { get; init; }

        /// <summary>
        /// Table minimum writer version
        /// </summary>
        public int MinimumWriterVersion { get; init; }

        /// <summary>
        /// Gets the table's explicit reader features.
        /// </summary>
        /// <remarks>
        /// The collection is empty when the table uses legacy protocol versioning without an
        /// explicit reader feature list.
        /// </remarks>
        public IReadOnlyList<string> ReaderFeatures => this.readerFeatures ?? Array.Empty<string>();

        /// <summary>
        /// Gets the table's explicit writer features.
        /// </summary>
        /// <remarks>
        /// The collection is empty when the table uses legacy protocol versioning without an
        /// explicit writer feature list.
        /// </remarks>
        public IReadOnlyList<string> WriterFeatures => this.writerFeatures ?? Array.Empty<string>();
    }
}