using System;
using System.Collections.Generic;

namespace DeltaLake.Credentials
{
    /// <summary>Configures caller-owned bearer credentials for the kernel engine.</summary>
    public sealed class KernelAzureBearerCredentialOptions
    {
        private const string DefaultScope = "https://storage.azure.com/.default";

        /// <summary>Creates immutable credential configuration without taking ownership of the provider.</summary>
        /// <param name="provider">The application-owned token provider.</param>
        /// <param name="scopes">Explicit HTTPS resource default scopes, or null for Azure Storage.</param>
        /// <param name="acquisitionTimeout">A whole-millisecond timeout from one to 120 seconds; null selects 30 seconds.</param>
        /// <param name="maxTokenBytes">The maximum token size from one to 65,536 UTF-8 bytes.</param>
        /// <exception cref="ArgumentNullException">The provider is null.</exception>
        /// <exception cref="ArgumentException">A scope is not an HTTPS resource default scope, or the collection is empty.</exception>
        /// <exception cref="ArgumentOutOfRangeException">A timeout or token limit is outside its bounds.</exception>
        public KernelAzureBearerCredentialOptions(
            IAzureBearerTokenProvider provider,
            IEnumerable<string>? scopes = null,
            TimeSpan? acquisitionTimeout = null,
            int maxTokenBytes = 65536)
        {
            Provider = provider ?? throw new ArgumentNullException(nameof(provider));
            Scopes = CopyScopes(scopes ?? new[] { DefaultScope });
            var timeout = acquisitionTimeout ?? TimeSpan.FromSeconds(30);
            if (timeout < TimeSpan.FromSeconds(1) || timeout > TimeSpan.FromSeconds(120)
                || timeout.Ticks % TimeSpan.TicksPerMillisecond != 0)
            {
                throw new ArgumentOutOfRangeException(nameof(acquisitionTimeout));
            }

            if (maxTokenBytes < 1 || maxTokenBytes > 65536)
            {
                throw new ArgumentOutOfRangeException(nameof(maxTokenBytes));
            }

            AcquisitionTimeout = timeout;
            MaxTokenBytes = maxTokenBytes;
        }

        /// <summary>Gets the application-owned provider. The SDK never disposes it.</summary>
        public IAzureBearerTokenProvider Provider { get; }

        /// <summary>Gets a read-only copy of the explicitly configured scopes.</summary>
        public IReadOnlyList<string> Scopes { get; }

        /// <summary>Gets the acquisition deadline budget.</summary>
        public TimeSpan AcquisitionTimeout { get; }

        /// <summary>Gets the maximum accepted token size in UTF-8 bytes.</summary>
        public int MaxTokenBytes { get; }

        /// <summary>Returns a description without provider or scope contents.</summary>
        /// <returns>A redacted credential configuration description.</returns>
        public override string ToString() => "KernelAzureBearerCredentialOptions { Provider = [REDACTED], Scopes = [REDACTED] }";

        internal static IReadOnlyList<string> CopyScopes(IEnumerable<string> scopes)
        {
            var copy = new List<string>();
            try
            {
                foreach (var scope in scopes)
                {
                    if (string.IsNullOrWhiteSpace(scope) || ContainsWhitespace(scope)
                        || !Uri.TryCreate(scope, UriKind.Absolute, out var resource)
                        || !string.Equals(resource.Scheme, Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase)
                        || resource.UserInfo.Length != 0 || resource.Query.Length != 0 || resource.Fragment.Length != 0
                        || !resource.AbsolutePath.EndsWith("/.default", StringComparison.Ordinal))
                    {
                        throw new ArgumentException(null, nameof(scopes));
                    }

                    copy.Add(scope);
                }
            }
            catch (Exception)
            {
                throw new ArgumentException(null, nameof(scopes));
            }

            if (copy.Count == 0)
            {
                throw new ArgumentException(null, nameof(scopes));
            }

            return copy.AsReadOnly();
        }

        private static bool ContainsWhitespace(string value)
        {
            foreach (var character in value)
            {
                if (char.IsWhiteSpace(character))
                {
                    return true;
                }
            }

            return false;
        }
    }
}