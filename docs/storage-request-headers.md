---
title: Storage Request Headers
description: Provide asynchronous custom headers for Azure storage requests without replacing native authentication
ms.date: 2026-10-05
---

## Configure a Provider

Implement `IStorageRequestHeaderProvider` and assign it to
`TableOptions.RequestHeaderProvider` or `TableCreateOptions.RequestHeaderProvider`.
Existing string storage options still configure the location and normal storage
authentication. Without a provider, the existing construction path is used.

```csharp
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using DeltaLake.Table;

sealed class FabricHeaderProvider : IStorageRequestHeaderProvider
{
    private readonly Func<CancellationToken, Task<string>> acquireActorToken;
    private readonly string accessContext;

    public FabricHeaderProvider(
        Func<CancellationToken, Task<string>> acquireActorToken,
        string accessContext)
    {
        this.acquireActorToken = acquireActorToken;
        this.accessContext = accessContext;
    }

    public async Task<IReadOnlyDictionary<string, string>> GetHeadersAsync(
        StorageRequestContext request,
        CancellationToken token)
    {
        var actorToken = await acquireActorToken(token).ConfigureAwait(false);
        return new Dictionary<string, string>
        {
            ["x-ms-s2s-actor-authorization"] = "Bearer " + actorToken,
            ["x-ms-fabric-s2s-access-context"] = accessContext
        };
    }
}
```

Pass the provider through the existing options argument:

```csharp
var options = new TableOptions
{
    TableLocation = tableLocation,
    StorageOptions = existingStorageOptions,
    RequestHeaderProvider = new FabricHeaderProvider(acquireActorToken, accessContext)
};

using var table = await engine.LoadTableAsync(options, cancellationToken);
```

## Invocation and Ownership

Headers are obtained before each original storage send, including retry attempts,
through both the delta-rs bridge and the separate Delta Kernel. The SDK does not
cache returned header sets. Native identity-endpoint requests do not invoke the
storage header provider.

The context contains the original method and URI. URIs can contain sensitive query
parameters; do not log them indiscriminately. The caller owns the provider, and the
SDK never disposes it. Implementations must support concurrent calls and cooperative
cancellation. The SDK retains managed state until native owners and actual provider
work finish. Native acquisition has a 30-second deadline. A provider that ignores
cancellation still occupies a bounded worker slot until its Task finishes.

Do not acquire headers by awaiting an operation on the same table whose request is
waiting for those headers. That creates an application-level dependency cycle.
Use separate table handles for concurrent table operations; this feature does not
introduce a same-handle concurrency guarantee.
Provider exceptions, invalid results and capacity failures fail the request; the
SDK does not silently send it without required headers. Error messages are sanitized.

## Redirect Behavior

Stock redirects remain enabled. When the underlying HTTP client follows a redirect,
custom headers can reach another destination and the provider is not called again
for that hop. The context describes the original request, not the redirected one.
Only use sensitive custom headers with destinations whose redirect behavior you trust.

Preserving redirects does not guarantee every request is redirected successfully.
Streamed write bodies that the stock client cannot replay can return a redirect
error. Read and write redirect behavior is not overridden by the provider.

## Supported Headers and Locations

The initial integration supports `az`, `adl`, `azure`, `abfs` and `abfss` locations.
Provider-enabled bridge construction does not support emulator mode. Other schemes
fail explicitly rather than ignoring the provider. Existing no-provider backends
and emulator behavior remain available.

Native storage authentication remains separate. `Authorization`, `Host`, `Cookie`,
`Content-Length`, `Transfer-Encoding`, `Connection`, `Upgrade`, `Proxy-*`,
`x-ms-date` and `x-ms-version` cannot be supplied by this provider.

With Azure Shared Key authentication, headers participating in its signature,
including all `x-ms`-prefixed fields, are also rejected before sending. Use bearer
authentication for the Fabric bypass headers in the example. Generic headers that
do not affect the signature, such as `x-custom`, remain supported with Shared Key.

Results allow at most 32 case-insensitively unique names, 128 UTF-8 bytes per name,
8 KiB per value and 64 KiB of serialized JSON. Names must be valid HTTP header
tokens, and values cannot contain prohibited control characters. Modules and static
callback trampolines remain loaded for process lifetime; module unloading is not
part of this capability.