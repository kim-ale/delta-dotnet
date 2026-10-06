using System.Collections.Generic;
using System.Text;

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
        /// Location of the delta table
        /// memory://, s3://, azure://, etc
        /// </summary>
        public string TableLocation { get; init; } = string.Empty;

        /// <summary>Gets the optional custom Azure storage request header provider.</summary>
        /// <remarks>
        /// Supported schemes are az, adl, azure, abfs and abfss. Other schemes are
        /// rejected before invoking the provider. The caller owns the provider,
        /// which may be called concurrently and is never disposed by the SDK.
        /// Headers are acquired afresh; faults fail the request and acquisition
        /// times out after 30 seconds. Stock redirects are preserved and can
        /// forward custom headers to another destination without callback reentry.
        /// Authorization, Host, Cookie, Content-Length, Transfer-Encoding,
        /// Connection, Upgrade, Proxy-*, x-ms-date and x-ms-version are forbidden.
        /// With account-key (Shared Key) authentication, provider results cannot
        /// contain signature-controlled headers: all x-ms-prefixed names (including
        /// x-ms-*), Content-Encoding, Content-Language, Content-Length, Content-MD5,
        /// Content-Type, Date, If-Modified-Since, If-Match, If-None-Match,
        /// If-Unmodified-Since and Range. These results fail with a sanitized
        /// provider error before sending; native Authorization is never replaced.
        /// Unsigned custom headers such as x-custom remain supported. Bearer and SAS
        /// requests allow otherwise permitted x-ms-* headers. Fabric bypass headers
        /// are intended for bearer authentication.
        /// Results are limited to 32 entries, 128 UTF-8 bytes per name, 8 KiB per
        /// value and 64 KiB of serialized JSON. Names must be valid HTTP tokens;
        /// values must not contain prohibited HTTP control characters.
        /// </remarks>
        public IStorageRequestHeaderProvider? RequestHeaderProvider { get; init; }

        /// <summary>Formats options without executing caller-controlled provider formatting.</summary>
        /// <param name="builder">Destination for member descriptions.</param>
        /// <returns>Whether any members were written.</returns>
        protected virtual bool PrintMembers(StringBuilder builder)
        {
            builder.Append("StorageOptions = ").Append(StorageOptions);
            builder.Append(", TableLocation = ").Append(TableLocation);
            builder.Append(", RequestHeaderProvider = ");
            builder.Append(RequestHeaderProvider == null ? "<none>" : "<configured>");
            return true;
        }
    }
}