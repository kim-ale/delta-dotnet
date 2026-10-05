using System.Collections.Generic;
using DeltaLake.Credentials;

namespace DeltaLake.Table
{
    /// <summary>
    /// Detla table storage options.
    /// </summary>
    public record TableStorageOptions
    {
        /// <summary>
        /// A map of string options
        /// </summary>
        public Dictionary<string, string> StorageOptions { get; init; } = new Dictionary<string, string>();

        /// <summary>
        /// Gets optional refreshable Azure bearer credentials for kernel operations only.
        /// </summary>
        /// <remarks>
        /// Bridge load/create uses a private static bootstrap token and bridge operations
        /// do not refresh it. The SDK does not dispose the caller-owned credential provider.
        /// </remarks>
        public KernelAzureBearerCredentialOptions? KernelAzureBearerCredential { get; init; }

        /// <summary>
        /// Location of the delta table
        /// memory://, s3://, azure://, etc
        /// </summary>
        public string TableLocation { get; init; } = string.Empty;
    }
}