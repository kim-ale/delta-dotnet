using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace DeltaLake.Table
{
    /// <summary>
    /// Supplies fresh custom headers for an Azure storage HTTP request.
    /// </summary>
    /// <remarks>
    /// Implementations may be called concurrently. The caller owns the provider;
    /// the SDK never disposes it. Returned headers are not cached. Provider failures
    /// fail the request. Native acquisition times out after 30 seconds.
    /// With account-key (Shared Key) authentication, provider results cannot contain
    /// signature-controlled headers: all x-ms-prefixed names (including x-ms-*),
    /// Content-Encoding, Content-Language, Content-Length, Content-MD5, Content-Type,
    /// Date, If-Modified-Since, If-Match, If-None-Match, If-Unmodified-Since and Range.
    /// These results fail with a sanitized provider error before sending; native
    /// Authorization is never replaced. Unsigned custom headers such as x-custom
    /// remain supported. Bearer and SAS requests allow otherwise permitted x-ms-*
    /// headers. Fabric bypass headers are intended for bearer authentication.
    /// Stock redirect behavior is preserved: custom headers may be forwarded to
    /// another destination without calling the provider again.
    /// </remarks>
    public interface IStorageRequestHeaderProvider
    {
        /// <summary>Gets custom headers for the original storage request.</summary>
        /// <param name="request">The immutable HTTP method and request URI.</param>
        /// <param name="token">Cooperative cancellation for header acquisition.</param>
        /// <returns>A non-null dictionary of custom header names and values.</returns>
        Task<IReadOnlyDictionary<string, string>> GetHeadersAsync(
            StorageRequestContext request, CancellationToken token);
    }
}