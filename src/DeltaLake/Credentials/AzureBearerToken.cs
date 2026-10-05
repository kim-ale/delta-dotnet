using System;

namespace DeltaLake.Credentials
{
    /// <summary>Represents an application-supplied Azure bearer token and its expiry.</summary>
    public sealed class AzureBearerToken
    {
        /// <summary>Creates a token with an expiry normalized to UTC.</summary>
        /// <param name="token">The bearer token.</param>
        /// <param name="expiresOn">The token expiry.</param>
        /// <exception cref="ArgumentNullException">The token is null.</exception>
        public AzureBearerToken(string token, DateTimeOffset expiresOn)
        {
            Token = token ?? throw new ArgumentNullException(nameof(token));
            ExpiresOn = expiresOn.ToUniversalTime();
        }

        /// <summary>Gets the bearer token. Treat this value as a secret.</summary>
        public string Token { get; }

        /// <summary>Gets the token expiry in UTC.</summary>
        public DateTimeOffset ExpiresOn { get; }

        /// <summary>Returns a description without token contents.</summary>
        /// <returns>A redacted token description.</returns>
        public override string ToString() => "AzureBearerToken { Token = [REDACTED] }";
    }
}