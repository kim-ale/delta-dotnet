using System;
using System.Threading;
using System.Threading.Tasks;

namespace DeltaLake.Credentials
{
    /// <summary>Adapts an application callback to the Azure bearer token provider contract.</summary>
    public sealed class DelegateAzureBearerTokenProvider : IAzureBearerTokenProvider
    {
        private readonly Func<AzureTokenRequestContext, CancellationToken, Task<AzureBearerToken>> _provider;

        /// <summary>Creates an adapter without taking ownership of the callback.</summary>
        /// <param name="provider">The token acquisition callback.</param>
        /// <exception cref="ArgumentNullException">The callback is null.</exception>
        public DelegateAzureBearerTokenProvider(Func<AzureTokenRequestContext, CancellationToken, Task<AzureBearerToken>> provider)
        {
            _provider = provider ?? throw new ArgumentNullException(nameof(provider));
        }

        /// <inheritdoc/>
        public Task<AzureBearerToken> GetTokenAsync(AzureTokenRequestContext context, CancellationToken cancellationToken)
            => _provider(context, cancellationToken);
    }
}