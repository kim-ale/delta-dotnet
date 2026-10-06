using System;

namespace DeltaLake.Table
{
    /// <summary>The original HTTP request for which custom headers are acquired.</summary>
    public sealed class StorageRequestContext
    {
        internal StorageRequestContext(string method, Uri requestUri)
        {
            Method = method;
            RequestUri = requestUri;
        }

        /// <summary>Gets the HTTP method.</summary>
        public string Method { get; }

        /// <summary>Gets the original request URI, before any redirects.</summary>
        public Uri RequestUri { get; }
    }
}