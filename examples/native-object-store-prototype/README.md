---
title: Native Object Store Package Prototype
description: Isolated public Kernel ABI package and Windows x64 consumer for independent native object stores
ms.date: 2026-10-07
ms.topic: how-to
---

## Scope

The net8.0 package `DeltaKernel.NativePrototype` version `0.1.0-prototype.7`
wraps the public Kernel C ABI. Its consumer uses an exact `PackageReference`,
not a project reference, DeltaLake internals, Rust source, or Cargo hooks.
Production projects, solution files, native pins, and Table interop are unchanged.

Both independently built DLLs are packaged as Windows x64 native assets.
The provider constructs its own Rust store and credentials. Only the plain C
descriptor crosses the DLL boundary. Managed code supplies no store I/O or
credential callbacks. The HTTP server is a loopback Blob fixture, not a managed
object-store implementation.

## Public artifact contract

Bindings use the generated public `delta_kernel_ffi.h` and `provider.h`, not
Rust internals. Use the current header under `target/ffi-headers`, not an older
copy in `target`. C/C++ consumers define `DEFINE_DEFAULT_ENGINE_BASE` and
`DEFINE_DEFAULT_ENGINE_RUSTLS` for the matching engine exports and types.

The version-3 x64 descriptor is exactly 72 bytes, not 80. All callback slots
remain opaque native addresses in the managed binding.

| Field | Offset | Size |
|-------|--------|------|
| `AbiVersion` | 0 | 4 |
| `StructSize` | 4 | 4 |
| `Context` | 8 | 8 |
| `Get` | 16 | 8 |
| `ListOpen` | 24 | 8 |
| `ListNext` | 32 | 8 |
| `ListClose` | 40 | 8 |
| `Put` | 48 | 8 |
| `Delete` | 56 | 8 |
| `Release` | 64 | 8 |

Versions 1, 2, and 99 are rejection probes; legacy layouts are not reinterpreted.
String slices are pointer/length pairs (16 bytes); metadata is 32 bytes.
All bound `ExternResultHandle...` types use a 32-bit tag at offset zero,
an aligned pointer union at offset eight, and a total size of 16 bytes.
Each public result has success tag zero and error tag one.

The injected ABI forwards full GET, HEAD, bounded/offset/suffix ranges, native
listing streams, atomic full-object Create/Overwrite PUT, and individual DELETE.
Native GET options occupy 24 bytes but need no managed binding because managed
code never invokes or implements these callbacks. Each cursor advance returns
at most 128 entries, with no total listing limit. Ordering is the native store's
order. Cursor ownership includes close after failed advances and cancellation.
Ranges are not emulated by full-object downloads, and cursor advancement does
not emulate a stream by re-listing. Bodies are bounded at 64 MiB, paths at 64 KiB.
Copy, delimiter listing, multipart upload, and ETag/version conditions beyond
atomic Create are not supported. Multi-range reads use the ObjectStore dependency's
default coalescing implementation, and bulk deletes are decomposed into individual
native calls. These do not preserve provider-specific batch optimizations. This is
not full ObjectStore operation parity.

`NativeStoreSession.Adopt(ref descriptor, tableUri)` clears the caller's value
and takes cleanup responsibility. On failed adoption it calls the native
release slot once; on successful adoption only Kernel releases the context.
Generic callers must supply a valid native descriptor and keep their provider
module loaded. Do not copy/adopt/free the same context twice or supply managed
I/O callback pointers. The prototype's provider and Kernel modules are pinned
for process lifetime, without preventing normal context release.

Engine and snapshot builders, transactions, and committed transactions have
typed SafeHandles. Every consuming call
invalidates its input before invoking native code, including error paths.
Store, engine, and snapshot borrows use SafeHandle leases; session operations
and disposal share one lock. The wrapper frees its caller-owned store handle
after attaching it and before building the engine. Fresh snapshots and native
memory append subsequently rely on the engine's retained store reference.

`NativeStoreSession.CommitInfo(engineInfo)` starts a public Kernel transaction,
attaches the engine marker, commits without Add actions, and returns its version.
It is an experimental fixture operation, not a general Delta write API.
Both `with_engine_info` and `commit` consume their input handles before the call;
unconsumed transaction and committed owners have their matching native frees.
The version accessor accepts a pointer to the raw committed handle, bound as
`in nint`, while an explicit `DangerousAddRef`/`DangerousRelease` lease keeps the
owner alive. Failures free returned errors without disposing the retained engine.
No raw transaction or provider context is exposed by this session method.

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
`obj/project.assets.json`: `DeltaKernel.NativePrototype/0.1.0-prototype.7`
must resolve as a package, not a project, with both win-x64 native assets.
Confirm both DLLs appear in the consumer output and that the process does
not load an existing production Kernel DLL from another example.

Local nupkgs, feed artifacts, and build outputs are ignored only inside this
example. No package is published and no remote repository CI is requested.

## Runtime probes

The console exits zero only after all assertions pass. It logs synthetic
counts and outcomes, never Authorization values.

* Descriptor, string, metadata, result, and descriptor-field layouts
* Legacy versions 1 and 2 and unknown version 99 rejected, no I/O, caller release once
* Invalid engine-builder URL cleanup after successful store adoption
* Repeated snapshot-builder and transaction failures with a retained engine and freed errors
* Native memory version zero, native append, version one on the same engine
* Repeated append failure, concurrent serialized reads, repeated disposal,
  and use-after-dispose rejection
* Independent session state and one final release per native context
* Azure Blob list/read requests observed by a loopback fixture, with changing
  native-generated bearer Authorization on repeated snapshots of one engine
* Public Kernel `CommitInfo("native-store-write-probe")` returns version one after
  seed version zero, then a fresh snapshot on the same engine reads version one
* Captured native Azure PUT targets the version-one JSON log with
  `If-None-Match: *`; every JSON line parses, `commitInfo.engineInfo` contains the
  marker, and no Add action appears; this probe never calls `AppendCommit`
* Native credential/callback counters advance and Azure context releases once

`CreateMemory` defaults to `memory:///table/` and ABI version 3. Its version argument
supports the rejection probe. `AppendCommit` is a one-time native fixture
mutation retained separately from the public Kernel write probe, not a general
transaction API. `CreateAzure` defaults to
`az://container/table/` with the provider's fixed account/container names.

The fixture serves a minimal Delta protocol/metadata commit, Blob XML listing,
HEAD/read metadata, and missing checkpoint/CRC responses. Its private in-memory
HTTP response state persists the actual native PUT body. Conditional Create
uses atomic insertion, returns 409 `BlobAlreadyExists` for a conflict, and
returns 201 with ETag/Last-Modified and a zero-length response body on success.
GET/HEAD serve the requested stored blob; XML listings filter current paths by
prefix and sort ordinally. This is local Blob protocol response handling, not
managed ObjectStore or authentication callbacks. It observes request bodies
and `If-None-Match` but never logs Authorization values, updates native tokens,
or rebuilds the native store. Credential rotation is process-local synthetic
provider behavior, not OAuth refresh. The ephemeral
port is selected with a temporary loopback listener; a competing process can
claim it before HttpListener starts. Startup errors fail the probe rather than
claiming a pass. HttpListener permissions or local network policy can also
block the fixture.

## Validation status and limits

The ABI v3 Contract builds with warnings-as-errors. Local prototype.7 has been
packed, force-restored and executed using freshly built independent DLLs. All
runtime probes pass: 72-byte layout, legacy/unknown rejection, failure cleanup,
memory mutation, independent sessions and native credential rotation.

The write probe passed an actual public Kernel no-Add transaction from version
zero to one, then read version one on the same engine. The fixture captured the
native conditional-create PUT and checked commitInfo JSON with the engine marker
and no Add actions. This proof did not call the provider append helper. It observed
11 native callbacks and six credential generations; the separate repeated-snapshot
probe observed six callbacks/requests and three generations. These are fixture
observations, not fixed protocol counts or OAuth/expiry-refresh proof.

The probes target Windows x64 snapshots and one no-Add commit with native
synthetic process credential rotation. ABI capability declarations do not imply
that the Consumer exercises every operation. The probes do not establish live
Azure OAuth/MSI, TLS/cloud acceptance, full scans/checkpoints, other runtime
identifiers, or production DeltaLake Table support. Production integration needs
a separate native-version and Bridge/fallback decision; this example does not
change those boundaries or add unsupported operations automatically.

An initial restore could not reach NuGet vulnerability metadata (NU1900). The
final local validation passed with `-p:NuGetAudit=false` on pack/restore, so no
successful vulnerability audit is claimed. Use a new prerelease package version
when changing native assets rather than overwriting a version cached by NuGet.