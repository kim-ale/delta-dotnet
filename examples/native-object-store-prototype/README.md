---
title: Native Object Store Package Prototype
description: Isolated public Kernel ABI package and Windows x64 consumer for independent native object stores
ms.date: 2026-10-07
ms.topic: how-to
---

## Scope

The net8.0 package `DeltaKernel.NativePrototype` version `0.1.0-prototype.9`
wraps the public Kernel C ABI. Its consumer uses an exact `PackageReference`,
not a project reference, DeltaLake internals, Rust source, or Cargo hooks.
Production projects, solution files, native pins, and Table interop are unchanged.

Both independently built DLLs are packaged as Windows x64 native assets.
The provider constructs its own Rust store and credentials. Only the plain C
descriptor crosses the DLL boundary. Managed code supplies no store I/O or
credential callbacks. The HTTP server is a loopback Blob fixture, not a managed
object-store implementation.

## Public artifact contract

Descriptor bindings follow the frozen native-store ABI V4 and its public C
declarations. Packaging uses generated public `delta_kernel_ffi.h` and
`provider.h`, not Rust source. Use the current header under `target/ffi-headers`,
not an older copy in `target`. The generated descriptor header and both DLLs
must match V4 before packing; the public checkpoint ABI is unchanged.
C/C++ consumers define `DEFINE_DEFAULT_ENGINE_BASE` and
`DEFINE_DEFAULT_ENGINE_RUSTLS` for the matching engine exports and types.

The version-4 x64 descriptor is exactly 160 bytes. All callback slots
remain opaque native addresses in the managed binding.

| Field | Offset | Size |
|-------|--------|------|
| `AbiVersion` | 0 | 4 |
| `StructSize` | 4 | 4 |
| `Context` | 8 | 8 |
| `Get` | 16 | 8 |
| `GetRanges` | 24 | 8 |
| `ListOpen` | 32 | 8 |
| `ListNext` | 40 | 8 |
| `ListClose` | 48 | 8 |
| `ListDelimiter` | 56 | 8 |
| `Put` | 64 | 8 |
| `DeleteBatch` | 72 | 8 |
| `Copy` | 80 | 8 |
| `Rename` | 88 | 8 |
| `MultipartOpen` | 96 | 8 |
| `MultipartPartOpen` | 104 | 8 |
| `MultipartPartWait` | 112 | 8 |
| `MultipartPartClose` | 120 | 8 |
| `MultipartComplete` | 128 | 8 |
| `MultipartAbort` | 136 | 8 |
| `MultipartClose` | 144 | 8 |
| `Release` | 152 | 8 |

Versions 1, 2, 3, and 99 are rejection probes; legacy layouts are not reinterpreted.
String slices are pointer/length pairs (16 bytes). V4 metadata is 64 bytes:
location at zero, size at 16, modification time at 24, ETag at 32, and version at
48. ETag/version are native string slices; null empty means absent and non-null
empty means a present empty string. All bound `ExternResultHandle...` types use a 32-bit tag at offset zero,
an aligned pointer union at offset eight, and a total size of 16 bytes.
Each public result has success tag zero and error tag one.

The separate checkpoint result is 24 bytes: outer tag at zero, success outcome
tag at eight, and owned snapshot pointer at 16. Written is outcome zero and
AlreadyExists is outcome one. On outer error, the allocated error pointer is at
eight instead of the success outcome; it must not be read as a snapshot handle.

The injected V4 ABI forwards GET/HEAD, bounded/offset/suffix and batch ranges,
ETag/version/time conditions, native listing streams and delimiter listings,
Create/Overwrite/conditional Update PUT, native batch deletion, copy/rename, and
multipart open/part-open/part-wait/part-close/complete/abort/close. Native metadata
includes ETag/version; GET attributes and write tags/attributes stay native.
GET/write options need no C# binding because managed code neither invokes nor
implements these callbacks. Each native batch-range request delegates once to
the provider's `get_ranges`; deletion delegates to its `delete_stream`, preserving
native result order and per-item errors, including shorter aggregate-error results.
The adapter does not emulate these batches with individual managed operations.

Every slot is mandatory; unsupported backends return native error statuses.
Opened cursors, multipart uploads, and part futures close through their matching
native slots before the retained context releases. Part handles retain upload
lifetime; close does not invent an abort policy. Sink outputs are borrowed and
deep-copied before return. The contract covers the requested typed operations,
not Rust `Extensions` or unbounded transfers. Bodies remain bounded at 64 MiB,
paths at 64 KiB, list/delete batches at 128 entries, and other collections and
delimiter results at 4096 entries. Native listing streams retain native order
without a total listing cap; per-handle operations must not race.

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

Every engine uses the public consuming
`builder_with_multithreaded_executor(builder, 2, 0)` export before `builder_build`.
Its return is a raw owned builder handle, not an `ExternResult`. The wrapper
invalidates the configured input builder before the call and gives the result
to a separate SafeHandle. The multithreaded executor supports checkpoint's nested
native work; the session lock still serializes managed calls and disposal.

`NativeStoreSession.CommitInfo(engineInfo)` starts a public Kernel transaction,
attaches the engine marker, commits without Add actions, and returns its version.
It is an experimental fixture operation, not a general Delta write API.
Both `with_engine_info` and `commit` consume their input handles before the call;
unconsumed transaction and committed owners have their matching native frees.
The version accessor accepts a pointer to the raw committed handle, bound as
`in nint`, while an explicit `DangerousAddRef`/`DangerousRelease` lease keeps the
owner alive. Failures free returned errors without disposing the retained engine.
No raw transaction or provider context is exposed by this session method.

`NativeStoreSession.CheckpointSnapshot()` builds a fresh snapshot using the
public snapshot-builder exports, then calls
`checkpoint_snapshot(snapshot, engine, null)`. Kernel auto-selects the format.
It returns `true` for Written and `false` for AlreadyExists, not a public enum.
Both successful outcomes produce an independently owned snapshot, whose version
must equal the input version and whose handle is always freed, even if its pointer
equals the borrowed input's. The P/Invoke leases both input SafeHandles. Outer
errors use the existing decode/free/throw path; unknown tags fail without guessing
at union storage or freeing an unverified pointer. No provider context or I/O
operation is exposed by this method.

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
`obj/project.assets.json`: `DeltaKernel.NativePrototype/0.1.0-prototype.9`
must resolve as a package, not a project, with both win-x64 native assets.
Confirm both DLLs appear in the consumer output and that the process does
not load an existing production Kernel DLL from another example.

Local nupkgs, feed artifacts, and build outputs are ignored only inside this
example. No package is published and no remote repository CI is requested.

## Runtime probes

The console exits zero only after all assertions pass. It logs synthetic
counts and outcomes, never Authorization values.

* V4 descriptor (160), string (16), metadata (64), handle result (16), checkpoint
  result (24), and all descriptor-field layouts
* Legacy versions 1, 2, and 3 and unknown version 99 rejected, no I/O, caller release once
* Invalid engine-builder URL cleanup after successful store adoption
* Repeated snapshot-builder and transaction failures with a retained engine and freed errors
* Native memory version zero, native append, version one on the same engine
* Public Kernel memory `CommitInfo("native-store-checkpoint-probe")` returns one,
  the first checkpoint reports Written, and the repeated checkpoint reports
  AlreadyExists; fresh snapshots remain at version one and the context releases once
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

`CreateMemory` defaults to `memory:///table/` and ABI version 4. Its version argument
supports the rejection probe. `AppendCommit` is a one-time native fixture
mutation retained separately from the public Kernel write probe, not a general
transaction API. `CreateAzure` defaults to
`az://container/table/` with the provider's fixed account/container names.

The memory checkpoint proof makes one public commit, two live checkpoint calls,
and four public version reads, then verifies one release after repeated disposal
and rejection of a disposed checkpoint call. Each checkpoint method builds its own
fresh input snapshot and frees its returned snapshot. Native callback totals
include checkpoint writes and are observed, not fixed assertions or Miri totals.
The small checkpoint uses native `BufWriter` full-object PUT below the 10 MiB
multipart threshold. It does not exercise multipart. Independent DLL/Rust adapter
acceptance belongs to parent validation, including batch/copy/multipart and full
metadata/tag/attribute parity; the managed package stays an opaque consumer.

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

The v4 wrapper builds with warnings-as-errors. The local package has been packed,
force-restored and executed with independently built v4 native DLLs. All consumer
assertions pass: layout, rejection of 1/2/3/99, failure cleanup, memory mutation,
independent contexts, native Azure rotation and an actual public Kernel no-Add write.
The public checkpoint probe reports Written then AlreadyExists at version one;
fresh snapshots remain at one and context release occurs once.

Both Arrow tracks pass all 670 FFI unit tests, including actual Windows cross-DLL
acceptance of native batches, copy/rename, delimiter listing, multipart and metadata.
Both tracks pass all 209 applicable adapter cases under Miri, with no ignored cases
and safety checks enabled. The separate provider passes 41 native-operation tests.
Clippy, public documentation and formatting checks pass. These local results do
not establish full cloud or production acceptance.

The probes target Windows x64 snapshots, no-Add commits, a small memory checkpoint,
and native synthetic process credential rotation. ABI capability declarations do
not imply that the Consumer exercises every operation. They do not establish
multipart coverage through checkpoint, live Azure OAuth/MSI, TLS/cloud acceptance,
full scans, all checkpoint shapes, other runtime identifiers, or production
DeltaLake Table support. Production integration needs a separate native-version
and Bridge/fallback decision; this example changes neither boundary.

Prior restore attempts encountered unavailable NuGet vulnerability metadata
(NU1900). Final local pack/restore uses `-p:NuGetAudit=false`; no successful
vulnerability audit is claimed. Use a new prerelease package version when changing
native assets rather than overwriting a version cached by NuGet.