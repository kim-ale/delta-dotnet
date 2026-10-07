using System.Runtime.InteropServices;

namespace DeltaKernel.NativePrototype;

[StructLayout(LayoutKind.Sequential, Pack = 8)]
internal readonly struct NativeStringSlice(nint pointer, nuint length)
{
    internal readonly nint Pointer = pointer;
    internal readonly nuint Length = length;
}

[StructLayout(LayoutKind.Sequential, Pack = 8)]
internal struct NativeObjectMetadata
{
    internal NativeStringSlice Location;
    internal ulong Size;
    internal long LastModifiedUnixMilliseconds;
    internal NativeStringSlice ETag;
    internal NativeStringSlice Version;
}

[StructLayout(LayoutKind.Explicit, Size = 16)]
internal struct NativeHandleResult
{
    [FieldOffset(0)]
    internal uint Tag;

    [FieldOffset(8)]
    internal nint Value;
}

[StructLayout(LayoutKind.Explicit, Size = 24)]
internal struct NativeCheckpointResult
{
    [FieldOffset(0)]
    internal uint Tag;

    [FieldOffset(8)]
    internal uint OutcomeTag;

    [FieldOffset(16)]
    internal nint Snapshot;

    [FieldOffset(8)]
    internal nint Error;
}

internal static class InteropLayout
{
    internal static void Verify()
    {
        if (!OperatingSystem.IsWindows() || RuntimeInformation.ProcessArchitecture != Architecture.X64)
        {
            throw new PlatformNotSupportedException("This native artifact package supports Windows x64 only.");
        }

        if (Marshal.SizeOf<NativeStoreDescriptor>() != 160 ||
            Marshal.SizeOf<NativeStringSlice>() != 16 ||
            Marshal.SizeOf<NativeObjectMetadata>() != 64 ||
            Marshal.SizeOf<NativeHandleResult>() != 16 ||
            Marshal.SizeOf<NativeCheckpointResult>() != 24 ||
            Marshal.OffsetOf<NativeStoreDescriptor>(nameof(NativeStoreDescriptor.AbiVersion)).ToInt32() != 0 ||
            Marshal.OffsetOf<NativeStoreDescriptor>(nameof(NativeStoreDescriptor.StructSize)).ToInt32() != 4 ||
            Marshal.OffsetOf<NativeStoreDescriptor>(nameof(NativeStoreDescriptor.Context)).ToInt32() != 8 ||
            Marshal.OffsetOf<NativeStoreDescriptor>(nameof(NativeStoreDescriptor.Get)).ToInt32() != 16 ||
            Marshal.OffsetOf<NativeStoreDescriptor>(nameof(NativeStoreDescriptor.GetRanges)).ToInt32() != 24 ||
            Marshal.OffsetOf<NativeStoreDescriptor>(nameof(NativeStoreDescriptor.ListOpen)).ToInt32() != 32 ||
            Marshal.OffsetOf<NativeStoreDescriptor>(nameof(NativeStoreDescriptor.ListNext)).ToInt32() != 40 ||
            Marshal.OffsetOf<NativeStoreDescriptor>(nameof(NativeStoreDescriptor.ListClose)).ToInt32() != 48 ||
            Marshal.OffsetOf<NativeStoreDescriptor>(nameof(NativeStoreDescriptor.ListDelimiter)).ToInt32() != 56 ||
            Marshal.OffsetOf<NativeStoreDescriptor>(nameof(NativeStoreDescriptor.Put)).ToInt32() != 64 ||
            Marshal.OffsetOf<NativeStoreDescriptor>(nameof(NativeStoreDescriptor.DeleteBatch)).ToInt32() != 72 ||
            Marshal.OffsetOf<NativeStoreDescriptor>(nameof(NativeStoreDescriptor.Copy)).ToInt32() != 80 ||
            Marshal.OffsetOf<NativeStoreDescriptor>(nameof(NativeStoreDescriptor.Rename)).ToInt32() != 88 ||
            Marshal.OffsetOf<NativeStoreDescriptor>(nameof(NativeStoreDescriptor.MultipartOpen)).ToInt32() != 96 ||
            Marshal.OffsetOf<NativeStoreDescriptor>(nameof(NativeStoreDescriptor.MultipartPartOpen)).ToInt32() != 104 ||
            Marshal.OffsetOf<NativeStoreDescriptor>(nameof(NativeStoreDescriptor.MultipartPartWait)).ToInt32() != 112 ||
            Marshal.OffsetOf<NativeStoreDescriptor>(nameof(NativeStoreDescriptor.MultipartPartClose)).ToInt32() != 120 ||
            Marshal.OffsetOf<NativeStoreDescriptor>(nameof(NativeStoreDescriptor.MultipartComplete)).ToInt32() != 128 ||
            Marshal.OffsetOf<NativeStoreDescriptor>(nameof(NativeStoreDescriptor.MultipartAbort)).ToInt32() != 136 ||
            Marshal.OffsetOf<NativeStoreDescriptor>(nameof(NativeStoreDescriptor.MultipartClose)).ToInt32() != 144 ||
            Marshal.OffsetOf<NativeStoreDescriptor>(nameof(NativeStoreDescriptor.Release)).ToInt32() != 152 ||
            Marshal.OffsetOf<NativeStringSlice>(nameof(NativeStringSlice.Pointer)).ToInt32() != 0 ||
            Marshal.OffsetOf<NativeStringSlice>(nameof(NativeStringSlice.Length)).ToInt32() != 8 ||
            Marshal.OffsetOf<NativeObjectMetadata>(nameof(NativeObjectMetadata.Location)).ToInt32() != 0 ||
            Marshal.OffsetOf<NativeObjectMetadata>(nameof(NativeObjectMetadata.Size)).ToInt32() != 16 ||
            Marshal.OffsetOf<NativeObjectMetadata>(nameof(NativeObjectMetadata.LastModifiedUnixMilliseconds)).ToInt32() != 24 ||
            Marshal.OffsetOf<NativeObjectMetadata>(nameof(NativeObjectMetadata.ETag)).ToInt32() != 32 ||
            Marshal.OffsetOf<NativeObjectMetadata>(nameof(NativeObjectMetadata.Version)).ToInt32() != 48 ||
            Marshal.OffsetOf<NativeHandleResult>(nameof(NativeHandleResult.Tag)).ToInt32() != 0 ||
            Marshal.OffsetOf<NativeHandleResult>(nameof(NativeHandleResult.Value)).ToInt32() != 8 ||
            Marshal.OffsetOf<NativeCheckpointResult>(nameof(NativeCheckpointResult.Tag)).ToInt32() != 0 ||
            Marshal.OffsetOf<NativeCheckpointResult>(nameof(NativeCheckpointResult.OutcomeTag)).ToInt32() != 8 ||
            Marshal.OffsetOf<NativeCheckpointResult>(nameof(NativeCheckpointResult.Snapshot)).ToInt32() != 16 ||
            Marshal.OffsetOf<NativeCheckpointResult>(nameof(NativeCheckpointResult.Error)).ToInt32() != 8)
        {
            throw new InvalidOperationException("The managed layouts do not match the public x64 C ABI.");
        }
    }
}

internal static class KernelNativeMethods
{
    private const string Library = "delta_kernel_ffi.dll";
    private static readonly nint Module;

    static KernelNativeMethods()
    {
        InteropLayout.Verify();
        Module = NativeLibrary.Load(Library, typeof(KernelNativeMethods).Assembly, null);
        string[] exports =
        [
            "get_native_object_store", "builder_with_object_store", "free_native_object_store",
            "get_engine_builder", "builder_build", "free_engine_builder", "free_engine",
            "builder_with_multithreaded_executor",
            "get_snapshot_builder", "snapshot_builder_build", "free_snapshot_builder",
            "free_snapshot", "version", "checkpoint_snapshot",
            "transaction", "with_engine_info", "commit", "committed_transaction_version",
            "free_transaction", "free_committed_transaction",
        ];
        foreach (var export in exports)
        {
            _ = NativeLibrary.GetExport(Module, export);
        }
    }

    internal static void EnsureLoaded() => _ = Module;

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    internal static extern NativeHandleResult get_native_object_store(
        in NativeStoreDescriptor descriptor, KernelErrors.AllocateError allocate_error);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    internal static extern NativeHandleResult get_engine_builder(
        NativeStringSlice path, KernelErrors.AllocateError allocate_error);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    internal static extern NativeHandleResult builder_with_object_store(nint builder, NativeStoreHandle store);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    internal static extern nint builder_with_multithreaded_executor(
        nint builder, nuint workerThreads, nuint maxBlockingThreads);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    internal static extern NativeHandleResult builder_build(nint builder);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    internal static extern void free_native_object_store(nint store);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    internal static extern void free_engine_builder(nint builder);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    internal static extern void free_engine(nint engine);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    internal static extern NativeHandleResult get_snapshot_builder(NativeStringSlice path, EngineHandle engine);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    internal static extern NativeHandleResult snapshot_builder_build(nint builder);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    internal static extern NativeCheckpointResult checkpoint_snapshot(
        SnapshotHandle snapshot, EngineHandle engine, nint spec);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    internal static extern void free_snapshot_builder(nint builder);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    internal static extern void free_snapshot(nint snapshot);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    internal static extern ulong version(SnapshotHandle snapshot);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    internal static extern NativeHandleResult transaction(NativeStringSlice path, EngineHandle engine);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    internal static extern NativeHandleResult with_engine_info(
        nint transaction, NativeStringSlice engineInfo, EngineHandle engine);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    internal static extern NativeHandleResult commit(nint transaction, EngineHandle engine);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    internal static extern ulong committed_transaction_version(in nint transaction);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    internal static extern void free_transaction(nint transaction);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    internal static extern void free_committed_transaction(nint transaction);
}

internal static class PluginNativeMethods
{
    private const string Library = "native_object_store_provider.dll";
    private static readonly nint Module;

    static PluginNativeMethods()
    {
        InteropLayout.Verify();
        Module = NativeLibrary.Load(Library, typeof(PluginNativeMethods).Assembly, null);
        string[] exports =
        [
            "prototype_create_memory", "prototype_create_azure", "prototype_append_commit",
            "prototype_release_count", "prototype_credential_requests", "prototype_credential_generation",
            "prototype_callback_count", "prototype_descriptor_size",
        ];
        foreach (var export in exports)
        {
            _ = NativeLibrary.GetExport(Module, export);
        }
    }

    internal static void EnsureLoaded() => _ = Module;

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    internal static extern int prototype_create_memory(ref NativeStoreDescriptor descriptor);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    internal static extern int prototype_create_azure(NativeStringSlice endpoint, ref NativeStoreDescriptor descriptor);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    internal static extern int prototype_append_commit(nint context);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    internal static extern ulong prototype_release_count();

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    internal static extern ulong prototype_credential_requests();

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    internal static extern ulong prototype_credential_generation();

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    internal static extern ulong prototype_callback_count();

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
    internal static extern uint prototype_descriptor_size();
}