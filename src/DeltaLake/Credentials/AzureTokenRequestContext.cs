using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

namespace DeltaLake.Credentials
{
    /// <summary>Describes the immutable scope and lifetime requirements for an acquisition.</summary>
    public sealed class AzureTokenRequestContext
    {
        [SuppressMessage("Maintainability", "CA1510", Justification = "Explicit null guards preserve net472 compatibility; ArgumentNullException.ThrowIfNull is unavailable on that target.")]
        [SuppressMessage("Maintainability", "CA1512", Justification = "Explicit range guards preserve net472 compatibility; ArgumentOutOfRangeException throw helpers are unavailable on that target.")]
        internal AzureTokenRequestContext(IReadOnlyList<string> scopes, TimeSpan minimumLifetime)
        {
            if (scopes == null)
            {
                throw new ArgumentNullException(nameof(scopes));
            }

            if (minimumLifetime <= TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(nameof(minimumLifetime));
            }

            Scopes = KernelAzureBearerCredentialOptions.CopyScopes(scopes);
            MinimumLifetime = minimumLifetime;
        }

        /// <summary>Gets the explicitly configured resource scopes.</summary>
        public IReadOnlyList<string> Scopes { get; }

        /// <summary>Gets the minimum useful lifetime required at receipt.</summary>
        public TimeSpan MinimumLifetime { get; }
    }
}