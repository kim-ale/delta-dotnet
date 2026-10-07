namespace DeltaKernel.NativePrototype;

/// <summary>Process-wide native probe counters and managed ABI sizes.</summary>
/// <remarks>Use before/after baselines. Counter reads are not an atomic combined snapshot.</remarks>
public readonly struct ProbeStatistics
{
    internal ProbeStatistics(
        ulong releases, ulong credentialRequests, ulong credentialGeneration, ulong callbacks,
        uint descriptorSize, int stringSliceSize, int metadataSize, int resultSize,
        long errorAllocations, long errorReleases)
    {
        Releases = releases;
        CredentialRequests = credentialRequests;
        CredentialGeneration = credentialGeneration;
        Callbacks = callbacks;
        DescriptorSize = descriptorSize;
        StringSliceSize = stringSliceSize;
        ObjectMetadataSize = metadataSize;
        HandleResultSize = resultSize;
        ErrorAllocations = errorAllocations;
        ErrorReleases = errorReleases;
    }

    /// <summary>Gets the number of provider context releases.</summary>
    public ulong Releases { get; }

    /// <summary>Gets the number of native synthetic credential acquisitions.</summary>
    public ulong CredentialRequests { get; }

    /// <summary>Gets the largest synthetic credential generation issued by the provider.</summary>
    public ulong CredentialGeneration { get; }

    /// <summary>Gets GET/LIST/PUT/DELETE callback attempts, including failures.</summary>
    public ulong Callbacks { get; }

    /// <summary>Gets the descriptor size reported by the independent native provider.</summary>
    public uint DescriptorSize { get; }

    /// <summary>Gets the managed size of the public pointer/length string layout.</summary>
    public int StringSliceSize { get; }

    /// <summary>Gets the managed size of the public native object-metadata layout.</summary>
    public int ObjectMetadataSize { get; }

    /// <summary>Gets the managed size of a public tagged handle result.</summary>
    public int HandleResultSize { get; }

    /// <summary>Gets the number of managed-owned native Kernel error allocations.</summary>
    public long ErrorAllocations { get; }

    /// <summary>Gets the number of returned Kernel error allocations freed by the wrapper.</summary>
    public long ErrorReleases { get; }
}