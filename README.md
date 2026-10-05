---
title: DeltaLake for .NET
description: Build and use the DeltaLake .NET wrapper with kernel-scoped Azure bearer credentials.
---

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Quick Start](#quick-start)
- [Kernel Azure Bearer Credentials](#kernel-azure-bearer-credentials)
  - [Support Boundaries](#support-boundaries)
  - [Native Packaging And Regeneration](#native-packaging-and-regeneration)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

This package is a C# wrapper around [delta-rs](https://github.com/delta-io/delta-rs/tree/rust-v0.17.0) and implements the FFI ([Foreign Function Interface](https://en.wikipedia.org/wiki/Foreign_function_interface)) for [delta-kernel-rs](https://github.com/delta-incubator/delta-kernel-rs).

It uses the [tokio-rs](https://tokio.rs/) runtime to provide asynchronous behavior. This allows the usage of .NET Tasks and async/await to take advantage of the same behavior provided by the underlying rust library.
This library also takes advantage of the [Apache Arrow](https://github.com/apache/arrow/blob/main/csharp/README.md) [C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html) to minimize the amount of copying required to move data between runtimes.

![alt text](/media/images/delta-dot-net-pkg.png "Using a Rust bridge and Kernel library with .NET p/invoke")

The bridge library incorporates delta-rs, delta-kernel-rs and [tokio-rs](https://tokio.rs/) as shown in the image below.

> The [Delta Kernel FFI](https://delta.io/blog/delta-kernel/) integration is pinned to releases of the Kernel, as new support is added to Kernel, support in C# will be easily extended as a fast follower.
> The library also benefits from [Apache DataFusion](https://datafusion.apache.org/) SQL support, which is [part of delta-rs](https://delta-io.github.io/delta-rs/integrations/delta-lake-datafusion/)

![alt text](/media/images/bridge-library.png "Rust bridge library with tokio and Kernel FFI")

NOTE: On unix systems, there is the possibility of a stack overflow due to small stack sizes for the .NET framework. The default size should correspond to `ulimit -s`, but we can override this by setting the environment variable `DOTNET_DefaultStackSize` to a hexadecimal number of bytes. The unit tests use `180000`.

## Quick Start

This section explains how build and run Delta Dotnet locally from scratch on a fresh Linux Machine. 
> If you're on windows, [WSL](https://learn.microsoft.com/en-us/windows/wsl/install) provides a great way to spinup Linux VMs rapidly for testing.

Step 1: Clone this repo, and initiate the `delta-kernel-rs` submodule:

```bash
cd ~/
git clone https://github.com/delta-incubator/delta-dotnet.git
cd delta-dotnet

GIT_ROOT=$(git rev-parse --show-toplevel)

chmod +x ${GIT_ROOT}/.scripts/checkout-delta-kernel-rs.sh && ${GIT_ROOT}/.scripts/checkout-delta-kernel-rs.sh
```

Step 2: Install dev dependencies:

```bash
chmod +x ${GIT_ROOT}/.scripts/bootstrap-dev-env.sh && ${GIT_ROOT}/.scripts/bootstrap-dev-env.sh
source ~/.bashrc
```

Step 3: Run the unit tests, which also builds the Rust binaries:

```bash
dotnet test -c Debug --logger "console;verbosity=detailed"
```

> All tests should run green ✅

Step 4: Run the example project, which writes delta tables to Azure Storage and reads it back as a [DataFrame](https://learn.microsoft.com/en-us/dotnet/machine-learning/how-to-guides/getting-started-dataframe):

```bash
az login --use-device-code
dotnet run --project ${GIT_ROOT}/examples/local/local.csproj -- "abfss://container@storageaccount.dfs.core.windows.net/a/b/demo-table" "20"

# Table root path: abfss://container@storageaccount.dfs.core.windows.net/a/b/demo-table
# Table partition columns: colHostTest
# Table version before transaction: 0
# Table version after transaction: 1
# Table: 3 columns by 20 rows
#
# colStringTest | colIntegerTest | colHostTest
# --------------|----------------|-------------
# yUnmZGYdeY    | 568935462      | Desktop    
# PfAc0LT7ZQ    | 683443233      | Desktop    
# Tg5xBKy3N3    | 897232561      | Desktop    
# pGMIGfmtRI    | 415054756      | Desktop    
# ormadktrAp    | 1114613767     | Desktop    
# OUYjEQpMxs    | 1159354204     | Desktop    
# J3mECDWmoc    | 1059212885     | Desktop    
# KSlaMMYiaT    | 525569187      | Desktop    
# KA5ZiD1ZTq    | 274902831      | Desktop    
# GRcqxFnF87    | 727541254      | Desktop    
# ZjFFAx6LZt    | 704318687      | Desktop    
# GUBZOqmRU9    | 62468794       | Desktop    
# 6sZdWl5xeV    | 439777191      | Desktop    
# 3oDrKCMZ9c    | 330135342      | Desktop    
# Goiltladv2    | 350043751      | Desktop    
# ts0v56YIDN    | 1381983219     | Desktop    
# 1dyWhaM7SU    | 1935291772     | Desktop    
# 7lBUQeMdeQ    | 1339188314     | Desktop    
# QFfm4Y7Q3w    | 498941470      | Desktop    
# 9yRU65qBy1    | 1105953095     | Desktop  
```

## Kernel Azure Bearer Credentials

Use `KernelAzureBearerCredentialOptions` with a caller-owned
`IAzureBearerTokenProvider` to refresh Azure bearer tokens for kernel-backed
reads, checkpoints, transactions, and change data feed (CDF). No Azure Identity
dependency is required. This example accepts your application's asynchronous
token acquisition delegate:

```csharp
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using DeltaLake.Credentials;
using DeltaLake.Table;

static async Task ReadWithKernelCredentialsAsync(
  System.Func<AzureTokenRequestContext, CancellationToken, Task<AzureBearerToken>> acquireToken,
  CancellationToken cancellationToken)
{
  var provider = new DelegateAzureBearerTokenProvider(async (context, cancellation) =>
    await acquireToken(context, cancellation).ConfigureAwait(false));

  using var engine = new DeltaEngine(EngineOptions.Default);
  using var table = await engine.LoadTableAsync(new TableOptions
  {
    TableLocation = "az://container/table",
    StorageOptions = new Dictionary<string, string>
    {
      ["account_name"] = "storageaccount",
    },
    KernelAzureBearerCredential = new KernelAzureBearerCredentialOptions(provider),
  }, cancellationToken);

  using var rows = await table.ReadAsArrowTableAsync(cancellationToken);
}
```

Your delegate returns `new AzureBearerToken(token, expiresOn)` with the actual
expiry from your token source. Honor the cancellation token and
`context.MinimumLifetime`; forward `context.Scopes` to the token source. The
default scope is `https://storage.azure.com/.default`. You can supply explicit
HTTPS resource default scopes for another Azure cloud. Never log token contents.

Applications already using Azure SDK credentials can adapt an `Azure.Core`
`TokenCredential` by calling `GetTokenAsync` with a `TokenRequestContext` built
from `context.Scopes`, then returning an `AzureBearerToken` from the resulting
`AccessToken.Token` and `AccessToken.ExpiresOn`. `Azure.Core` and `Azure.Identity`
remain optional application dependencies.

### Support Boundaries

- Bridge-first load/create acquires a static bootstrap token. Bridge-backed
 operations keep that token and do not use the refreshable kernel provider.
 This is not refreshable authentication for every `ITable` method.
- Kernel token renewal does not reload the bridge table or automatically rebuild
 table/snapshot state. Reopen the table when you need a new bridge bootstrap
 token; kernel renewal alone does not extend bridge authentication.
- Your application owns the provider and any underlying credential/client.
 DeltaLake does not dispose them. Keep them alive until tables and returned
 native-backed results have been disposed.
- Use each table handle serially. Credential refresh does not make concurrent
 operations on a single handle supported.
- Set the Azure account explicitly in `StorageOptions`, even if the table URI
 contains an account name. Do not combine this option with static bearer tokens,
 account keys, SAS, ambient credential modes, or authentication bypasses.
- Use HTTPS in production. `allow_http=true` requires an explicit, approved
 development endpoint; it is not an emulator or authentication bypass switch.
- Cancellation is cooperative during token acquisition. It does not interrupt an
 already admitted synchronous kernel FFI call.

### Native Packaging And Regeneration

The credential-aware native host builds as `delta_dotnet_kernel`. Local builds
copy it under the existing `delta_kernel_ffi` platform filename using MSBuild
`Link` metadata. Package builds stage the same host under that filename for
`linux-x64`, `linux-arm64`, `osx-x64`, `osx-arm64`, and `win-x64`. Stock and
credential exports must come from this single image; do not replace it with a
stock-only kernel library. Older libraries fail credential ABI preflight rather
than silently falling back to static kernel authentication.

Build output is pinned to each crate's local `target` directory, including
`src/DeltaLake/Kernel/NativeHost/target`. `SkipNativeBuild=true` skips Cargo but
still requires the native binaries at the configured copy paths.

For regeneration on Linux or WSL, install the host's cbindgen version alongside
the existing LLVM 20 and ClangSharp tools:

```bash
cargo install cbindgen --version 0.29.2 --locked
make generate-kernel-credential-bindings
```

The target builds NativeHost with `--locked` and an explicit target directory,
copies the stock dependency header, and generates the separate credential header
at `src/DeltaLake/Kernel/include/delta_dotnet_kernel_credentials.h`. It clears
`CARGO_TARGET_DIR` for that build so the stock FFI header retains its submodule
location. The test assembly embeds the credential header for ABI layout checks.

ClangSharp uses `src/DeltaLake/Kernel/GenerateCredentialInterop.rsp` and writes
comparison bindings to a temporary directory printed by Make. Review them
against the checked sequential layouts in `KernelCredentialInterop.cs`; do not
automatically replace the managed declarations. `make generate-kernel-bindings`
continues to generate only stock kernel interop. Credential regeneration does
not regenerate Bridge bindings.
