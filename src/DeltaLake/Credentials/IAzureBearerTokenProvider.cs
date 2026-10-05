using System.Threading;
using System.Threading.Tasks;

namespace DeltaLake.Credentials
{
    /// <summary>Supplies Azure bearer tokens for kernel storage requests.</summary>
    public interface IAzureBearerTokenProvider
    {
        /// <summary>Acquires a token satisfying the requested scope and useful lifetime.</summary>
        /// <param name="context">The immutable token request.</param>
        /// <param name="cancellationToken">Cooperative acquisition cancellation.</param>
        /// <returns>The acquired token.</returns>
        Task<AzureBearerToken> GetTokenAsync(AzureTokenRequestContext context, CancellationToken cancellationToken);
    }
}