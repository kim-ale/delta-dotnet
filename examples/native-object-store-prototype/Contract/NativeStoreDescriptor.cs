using System.Runtime.InteropServices;

namespace DeltaKernel.NativePrototype;

/// <summary>Matches the public x64 KernelNativeObjectStoreDescriptorV4 C layout.</summary>
/// <remarks>
/// All pointers must originate from a compatible native provider. Do not implement I/O or
/// authentication callbacks in managed code. Keep the provider module loaded for process lifetime.
/// Copying this value does not create another owner of its context.
/// </remarks>
[StructLayout(LayoutKind.Sequential, Pack = 8)]
public struct NativeStoreDescriptor
{
    /// <summary>The provider ABI version at offset zero, which must be four for adoption.</summary>
    public uint AbiVersion;

    /// <summary>The exact descriptor size at offset four, which is 160 bytes on Windows x64.</summary>
    public uint StructSize;

    /// <summary>The provider-owned opaque context at offset eight.</summary>
    public nint Context;

    /// <summary>The native GET, HEAD, range, conditional, and version callback address at offset 16.</summary>
    public nint Get;

    /// <summary>The native batch range-read callback address at offset 24.</summary>
    public nint GetRanges;

    /// <summary>The native listing cursor-open callback address at offset 32.</summary>
    public nint ListOpen;

    /// <summary>The native listing cursor-advance callback address at offset 40.</summary>
    public nint ListNext;

    /// <summary>The native listing cursor-close callback address at offset 48.</summary>
    public nint ListClose;

    /// <summary>The native delimiter-listing callback address at offset 56.</summary>
    public nint ListDelimiter;

    /// <summary>The native Create, Overwrite, or conditional Update PUT callback address at offset 64.</summary>
    public nint Put;

    /// <summary>The native batch-deletion callback address at offset 72.</summary>
    public nint DeleteBatch;

    /// <summary>The native Overwrite or Create copy callback address at offset 80.</summary>
    public nint Copy;

    /// <summary>The native Overwrite or Create rename callback address at offset 88.</summary>
    public nint Rename;

    /// <summary>The native multipart upload-open callback address at offset 96.</summary>
    public nint MultipartOpen;

    /// <summary>The native multipart part-future-open callback address at offset 104.</summary>
    public nint MultipartPartOpen;

    /// <summary>The native multipart part-future-wait callback address at offset 112.</summary>
    public nint MultipartPartWait;

    /// <summary>The native multipart part-future-close callback address at offset 120.</summary>
    public nint MultipartPartClose;

    /// <summary>The native multipart completion callback address at offset 128.</summary>
    public nint MultipartComplete;

    /// <summary>The native multipart abort callback address at offset 136.</summary>
    public nint MultipartAbort;

    /// <summary>The native multipart upload-close callback address at offset 144.</summary>
    public nint MultipartClose;

    /// <summary>The native final context-release callback address at offset 152.</summary>
    public nint Release;
}