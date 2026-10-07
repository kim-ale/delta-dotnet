using System.Runtime.InteropServices;

namespace DeltaKernel.NativePrototype;

/// <summary>Matches the public x64 KernelNativeObjectStoreDescriptorV1 C layout.</summary>
/// <remarks>
/// All pointers must originate from a compatible native provider. Do not implement I/O or
/// authentication callbacks in managed code. Keep the provider module loaded for process lifetime.
/// Copying this value does not create another owner of its context.
/// </remarks>
[StructLayout(LayoutKind.Sequential, Pack = 8)]
public struct NativeStoreDescriptor
{
    /// <summary>The provider ABI version, which must be one for adoption.</summary>
    public uint AbiVersion;

    /// <summary>The exact descriptor size, which is 56 bytes on Windows x64.</summary>
    public uint StructSize;

    /// <summary>The provider-owned opaque context.</summary>
    public nint Context;

    /// <summary>The native GET/HEAD callback address.</summary>
    public nint Get;

    /// <summary>The native ordered-list callback address.</summary>
    public nint List;

    /// <summary>The native full-object write callback address.</summary>
    public nint Put;

    /// <summary>The native object-deletion callback address.</summary>
    public nint DeleteObject;

    /// <summary>The native final context-release callback address.</summary>
    public nint Release;
}