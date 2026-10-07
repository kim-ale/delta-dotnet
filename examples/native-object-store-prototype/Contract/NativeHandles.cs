using Microsoft.Win32.SafeHandles;

namespace DeltaKernel.NativePrototype;

internal abstract class KernelHandle() : SafeHandleZeroOrMinusOneIsInvalid(ownsHandle: true)
{
    internal void Initialize(nint value) => SetHandle(value);

    internal nint Consume()
    {
        ObjectDisposedException.ThrowIf(IsClosed || IsInvalid, this);
        var value = DangerousGetHandle();
        SetHandleAsInvalid();
        return value;
    }
}

internal sealed class NativeStoreHandle : KernelHandle
{
    protected override bool ReleaseHandle()
    {
        KernelNativeMethods.free_native_object_store(handle);
        return true;
    }
}

internal sealed class EngineBuilderHandle : KernelHandle
{
    protected override bool ReleaseHandle()
    {
        KernelNativeMethods.free_engine_builder(handle);
        return true;
    }
}

internal sealed class EngineHandle : KernelHandle
{
    protected override bool ReleaseHandle()
    {
        KernelNativeMethods.free_engine(handle);
        return true;
    }
}

internal sealed class SnapshotBuilderHandle : KernelHandle
{
    protected override bool ReleaseHandle()
    {
        KernelNativeMethods.free_snapshot_builder(handle);
        return true;
    }
}

internal sealed class SnapshotHandle : KernelHandle
{
    protected override bool ReleaseHandle()
    {
        KernelNativeMethods.free_snapshot(handle);
        return true;
    }
}