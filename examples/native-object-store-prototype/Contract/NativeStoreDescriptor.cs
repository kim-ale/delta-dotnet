using System.Runtime.InteropServices;

namespace DeltaKernel.NativePrototype;

/// <summary>Matches the public x64 KernelNativeObjectStoreDescriptorV3 C layout.</summary>
/// <remarks>
/// All pointers must originate from a compatible native provider. Do not implement I/O or
/// authentication callbacks in managed code. Keep the provider module loaded for process lifetime.
/// Copying this value does not create another owner of its context.
/// </remarks>
[StructLayout(LayoutKind.Sequential, Pack = 8)]
public struct NativeStoreDescriptor
{
    /// <summary>The provider ABI version at offset zero, which must be three for adoption.</summary>
    public uint AbiVersion;

    /// <summary>The exact descriptor size at offset four, which is 72 bytes on Windows x64.</summary>
    public uint StructSize;

    /// <summary>The provider-owned opaque context at offset eight.</summary>
    public nint Context;

    /// <summary>The native GET, HEAD, and range callback address at offset 16.</summary>
    public nint Get;

    /// <summary>The native listing cursor-open callback address at offset 24.</summary>
    public nint ListOpen;

    /// <summary>The native listing cursor-advance callback address at offset 32.</summary>
    public nint ListNext;

    /// <summary>The native listing cursor-close callback address at offset 40.</summary>
    public nint ListClose;

    /// <summary>The native atomic full-object Create or Overwrite PUT callback address at offset 48.</summary>
    public nint Put;

    /// <summary>The native individual-object deletion callback address at offset 56.</summary>
    public nint Delete;

    /// <summary>The native final context-release callback address at offset 64.</summary>
    public nint Release;
}