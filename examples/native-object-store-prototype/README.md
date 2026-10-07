---
title: Native Object Store Package Prototype
description: Isolated public Kernel ABI package and Windows x64 consumer for independent native object stores
ms.date: 2026-10-06
ms.topic: how-to
---

## Scope

The net8.0 package `DeltaKernel.NativePrototype` version `0.1.0-prototype.3`
wraps the public Kernel C ABI. Its consumer uses an exact `PackageReference`,
not a project reference, DeltaLake internals, Rust source, or Cargo hooks.
Production projects, solution files, native pins, and Table interop are unchanged.

Both independently built DLLs are packaged as Windows x64 native assets.
The provider constructs its own Rust store and credentials. Only the plain C
descriptor crosses the DLL boundary. Managed code supplies no store I/O or
credential callbacks. The HTTP server is a loopback Blob fixture, not a managed
object-store implementation.

## Public artifact contract

Bindings are handwritten from the generated `delta_kernel_ffi.h` and the public
`provider.h`, not generated from Rust internals. The inspected Kernel header
declares `FFIKernelError` at line 24, `EngineError` and `AllocateErrorFn` near
lines 719 and 798, the descriptor at line 2452, the builder APIs near lines
6690 and 6810, snapshot APIs near lines 6857 and 6926, `version` at line 6994,
and native adoption/free at lines 7784 and 7799. Header line numbers are
artifact-specific; recheck declarations when changing the native artifacts.

The x64 descriptor is 56 bytes: two `uint32_t` header fields, context, and five
native function pointers. String slices are pointer/length pairs (16 bytes);
metadata is 32 bytes. All bound `ExternResultHandle...` types use a 32-bit
tag at offset zero, an aligned pointer union at offset eight, and a total size
of 16 bytes. Each public result has success tag zero and error tag one.
No extra result variants or provider status values are invented. Native
provider status zero is success; nonzero values are reported numerically.

`NativeStoreSession.Adopt(ref descriptor, tableUri)` clears the caller's value
and takes cleanup responsibility. On failed adoption it calls the native
release slot once; on successful adoption only Kernel releases the context.
Generic callers must supply a valid native descriptor and keep their provider
module loaded. Do not copy/adopt/free the same context twice or supply managed
I/O callback pointers. The prototype's provider and Kernel modules are pinned
for process lifetime, without preventing normal context release.

Engine and snapshot builders have typed SafeHandles. Every consuming call
invalidates its input before invoking native code, including error paths.
Store, engine, and snapshot borrows use SafeHandle leases; session operations
and disposal share one lock. The wrapper frees its caller-owned store handle
after attaching it and before building the engine. Fresh snapshots and native
memory append subsequently rely on the engine's retained store reference.

The process-rooted Cdecl error allocator copies the UTF-8 error into one
caller-owned allocation with the C `EngineError` prefix. Returned errors are
decoded and freed in `finally`. Allocation failures cannot unwind through
the callback; a null error pointer produces a managed failure when returned.
Counters track normal allocated/returned error records. This is not an
out-of-memory recovery guarantee for native code.

## Local package and consumer

Prerequisites are Windows x64, the .NET 8 SDK, and the previously built native
prototype artifacts. `KernelArtifactRoot` and `ProviderArtifactRoot` are
packaging inputs only. They do not create a source-project dependency or
compile native code. `ProviderHeader` is optional and is packed only when it
exists. Supply it to include both public headers in the package.

Run from this example directory. Replace the artifact variables with your
existing native output directories. No absolute checkout path is embedded
in either project.

```powershell
$kernelArtifacts = 'C:/Users/kimal/ASA/delta-kernel-rs-worktrees/native-object-store/target'
$providerArtifacts = 'C:/Users/kimal/ASA/delta-kernel-rs-worktrees/native-object-store/target-provider'
$providerHeader = 'C:/Users/kimal/ASA/delta-kernel-rs-worktrees/native-object-store/ffi/examples/native-object-store-provider/provider.h'

dotnet build Contract/DeltaKernel.NativePrototype.csproj --configuration Debug
dotnet pack Contract/DeltaKernel.NativePrototype.csproj --configuration Debug --no-build --output artifacts/feed "-p:KernelArtifactRoot=$kernelArtifacts" "-p:ProviderArtifactRoot=$providerArtifacts" "-p:ProviderHeader=$providerHeader"
dotnet restore Consumer/NativeObjectStore.Consumer.csproj
dotnet run --project Consumer/NativeObjectStore.Consumer.csproj --configuration Debug --no-restore
```

The consumer resolves packages from `../artifacts/feed` relative to its own
project directory and the official NuGet source. The latter also resolves
inherited repository analyzers and .NET runtime packs. No additional test
framework package is required. The contract library contains code and XML
API documentation, DLLs under `runtimes/win-x64/native/`, and public headers
under `build/native/include/`. NuGet's RID asset resolution supplies native
DLLs; there is no source DLL-copy target or custom consumer resolver.

For parent validation, inspect the local nupkg and consumer
`obj/project.assets.json`: `DeltaKernel.NativePrototype/0.1.0-prototype.3`
must resolve as a package, not a project, with both win-x64 native assets.
Confirm both DLLs appear in the consumer output and that the process does
not load an existing production Kernel DLL from another example.

Local nupkgs, feed artifacts, and build outputs are ignored only inside this
example. No package is published and no remote repository CI is requested.

## Runtime probes

The console exits zero only after all assertions pass. It logs synthetic
counts and outcomes, never Authorization values.

* Descriptor, string, metadata, result, and descriptor-field layouts
* Unknown descriptor version rejected by Kernel, no I/O, caller release once
* Invalid engine-builder URL cleanup after successful store adoption
* Repeated snapshot-builder failures with a retained engine and freed errors
* Native memory version zero, native append, version one on the same engine
* Repeated append failure, concurrent serialized reads, repeated disposal,
  and use-after-dispose rejection
* Independent session state and one final release per native context
* Azure Blob list/read requests observed by a loopback fixture, with changing
  native-generated bearer Authorization on repeated snapshots of one engine
* Native credential/callback counters advance and Azure context releases once

`CreateMemory` defaults to `memory:///table/`. Its descriptor-version argument
supports the rejection probe. `AppendCommit` is a one-time native fixture
mutation, not a general transaction API. `CreateAzure` defaults to
`az://container/table/` with the provider's fixed account/container names.

The fixture serves a minimal Delta protocol/metadata commit, Blob XML listing,
HEAD/read metadata, and missing checkpoint/CRC responses. It observes requests
but never updates native tokens or rebuilds the native store. The ephemeral
port is selected with a temporary loopback listener; a competing process can
claim it before HttpListener starts. Startup errors fail the probe rather than
claiming a pass. HttpListener permissions or local network policy can also
block the fixture.

## Validation status and limits

The local `0.1.0-prototype.3` package was built, packed, restored and executed.
All console probes passed, including caller store-handle release before engine
construction, memory mutation from version zero to one on the same engine,
failed-adoption/builder/snapshot cleanup and final context release once.
The Azure fixture observed six native credential retrievals and three distinct
outgoing Authorization generations with the same store and engine. C# did not
update credentials. Package assets resolve both native DLLs as regular win-x64
package assets, not source-project dependencies.

The probes cover Windows x64 snapshot/list/read and native synthetic credential
refresh. They do not establish live Azure OAuth/MSI, TLS/cloud acceptance,
full scans/checkpoints, other runtime identifiers, or production DeltaLake
Table support. The native prototype has limited operations and a 64 MiB body
buffer bound, enforced before metadata-oversized reads and during stream collection.
The provider intentionally buffers and sorts a full native listing for each page.
Production integration needs a separate native-version and
Bridge/fallback decision; this example does not change those boundaries.

An initial restore could not reach NuGet vulnerability metadata (NU1900). The
final local validation passed with `-p:NuGetAudit=false` on pack/restore, so no
successful vulnerability audit is claimed. Use a new prerelease package version
when changing native assets rather than overwriting a version cached by NuGet.