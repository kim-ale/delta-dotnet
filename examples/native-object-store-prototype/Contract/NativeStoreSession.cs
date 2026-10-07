using System.Runtime.InteropServices;

namespace DeltaKernel.NativePrototype;

/// <summary>Owns one Kernel engine using an independently constructed native store.</summary>
/// <remarks>
/// Operations and disposal are serialized. Every version read builds and disposes a fresh snapshot
/// on the same engine. CommitInfo exercises a public Kernel transaction without adding files;
/// AppendCommit remains native memory fixture mutation only. Native module handles and the error
/// allocator delegate remain rooted for process lifetime.
/// </remarks>
public sealed class NativeStoreSession : IDisposable
{
    private readonly object gate = new();
    private readonly EngineHandle engine;
    private readonly string tableUri;
    private readonly nint memoryContext;
    private bool disposed;

    private NativeStoreSession(EngineHandle engine, string tableUri, nint memoryContext)
    {
        this.engine = engine;
        this.tableUri = tableUri;
        this.memoryContext = memoryContext;
    }

    /// <summary>Adopts a native descriptor and attaches it to a new Kernel engine.</summary>
    /// <param name="descriptor">The exclusively owned descriptor, cleared on entry.</param>
    /// <param name="tableUri">The absolute table URL passed to the public Kernel builder.</param>
    /// <returns>The session owning the engine and its retained native store.</returns>
    /// <remarks>
    /// This method takes cleanup responsibility even on failure. Before adoption it invokes the
    /// supplied native release slot once; after adoption only Kernel releases the context.
    /// The caller must provide valid initialized storage and a callable release slot, must not
    /// retain another context owner, and must keep its native provider module loaded.
    /// </remarks>
    /// <exception cref="InvalidOperationException">Kernel rejects the descriptor or engine setup fails.</exception>
    /// <exception cref="PlatformNotSupportedException">The process is not Windows x64.</exception>
    public static NativeStoreSession Adopt(ref NativeStoreDescriptor descriptor, string tableUri)
        => AdoptCore(ref descriptor, tableUri, allowMemoryAppend: false);

    /// <summary>Creates the independent provider's seeded native memory store.</summary>
    /// <param name="tableUri">The table URL; the seeded table is at memory:///table/.</param>
    /// <param name="descriptorVersion">The ABI version passed to Kernel, including rejection probes.</param>
    /// <returns>A session over the native memory store.</returns>
    /// <exception cref="InvalidOperationException">Native factory, adoption, or builder setup fails.</exception>
    public static NativeStoreSession CreateMemory(string tableUri = "memory:///table/", uint descriptorVersion = 3)
    {
        PluginNativeMethods.EnsureLoaded();
        NativeStoreDescriptor descriptor = default;
        ThrowProviderError(PluginNativeMethods.prototype_create_memory(ref descriptor), "create memory");
        descriptor.AbiVersion = descriptorVersion;
        return AdoptCore(ref descriptor, tableUri, allowMemoryAppend: true);
    }

    /// <summary>Creates the independent native Azure provider with synthetic rotating credentials.</summary>
    /// <param name="endpoint">The service URL, normally the local HTTP fixture endpoint.</param>
    /// <param name="tableUri">The table URL under the provider's fixed container named container.</param>
    /// <returns>A session over the native Azure store.</returns>
    /// <remarks>This prototype uses account and container fixture names, not production OAuth.</remarks>
    /// <exception cref="InvalidOperationException">Native factory, adoption, or builder setup fails.</exception>
    public static NativeStoreSession CreateAzure(string endpoint, string tableUri = "az://container/table/")
    {
        PluginNativeMethods.EnsureLoaded();
        using var endpointBytes = new Utf8Argument(endpoint);
        NativeStoreDescriptor descriptor = default;
        ThrowProviderError(PluginNativeMethods.prototype_create_azure(endpointBytes.Slice, ref descriptor), "create Azure");
        return AdoptCore(ref descriptor, tableUri, allowMemoryAppend: false);
    }

    /// <summary>Reads the latest version using a new snapshot on the retained engine.</summary>
    /// <returns>The snapshot version reported by Kernel.</returns>
    /// <exception cref="ObjectDisposedException">The session has been disposed.</exception>
    /// <exception cref="InvalidOperationException">Kernel cannot construct the snapshot.</exception>
    public ulong GetVersion()
    {
        lock (gate)
        {
            ObjectDisposedException.ThrowIf(disposed, this);
            using var path = new Utf8Argument(tableUri);
            using var builder = new SnapshotBuilderHandle();
            using var snapshot = new SnapshotHandle();
            builder.Initialize(KernelErrors.Unwrap(KernelNativeMethods.get_snapshot_builder(path.Slice, engine)));
            snapshot.Initialize(KernelErrors.Unwrap(KernelNativeMethods.snapshot_builder_build(builder.Consume())));
            return KernelNativeMethods.version(snapshot);
        }
    }

    /// <summary>Commits engine info through the public Kernel transaction exports.</summary>
    /// <param name="engineInfo">The nonempty engine marker persisted in commitInfo.</param>
    /// <returns>The version reported by the committed transaction.</returns>
    /// <remarks>
    /// This experimental fixture operation adds no files and is not a general Delta write API.
    /// Kernel writes the commit through the native store retained by this session's engine.
    /// </remarks>
    /// <exception cref="ArgumentException">The engine marker is empty or whitespace.</exception>
    /// <exception cref="ObjectDisposedException">The session has been disposed.</exception>
    /// <exception cref="InvalidOperationException">Kernel cannot start, configure, or commit the transaction.</exception>
    public ulong CommitInfo(string engineInfo)
    {
        lock (gate)
        {
            ObjectDisposedException.ThrowIf(disposed, this);
            using var path = new Utf8Argument(tableUri);
            using var info = new Utf8Argument(engineInfo);
            using var transaction = new TransactionHandle();
            using var configuredTransaction = new TransactionHandle();
            using var committedTransaction = new CommittedTransactionHandle();
            transaction.Initialize(KernelErrors.Unwrap(KernelNativeMethods.transaction(path.Slice, engine)));
            configuredTransaction.Initialize(KernelErrors.Unwrap(
                KernelNativeMethods.with_engine_info(transaction.Consume(), info.Slice, engine)));
            committedTransaction.Initialize(KernelErrors.Unwrap(
                KernelNativeMethods.commit(configuredTransaction.Consume(), engine)));

            var lease = false;
            try
            {
                committedTransaction.DangerousAddRef(ref lease);
                var rawTransaction = committedTransaction.DangerousGetHandle();
                return KernelNativeMethods.committed_transaction_version(in rawTransaction);
            }
            finally
            {
                if (lease)
                {
                    committedTransaction.DangerousRelease();
                }
            }
        }
    }

    /// <summary>Asks the native memory provider to append its version-one fixture commit.</summary>
    /// <remarks>The provider creates the commit once. This is not a general Delta write API.</remarks>
    /// <exception cref="ObjectDisposedException">The session has been disposed.</exception>
    /// <exception cref="InvalidOperationException">The session is not a memory fixture, or append fails.</exception>
    public void AppendCommit()
    {
        lock (gate)
        {
            ObjectDisposedException.ThrowIf(disposed, this);
            if (memoryContext == 0)
            {
                throw new InvalidOperationException("AppendCommit is restricted to the native memory fixture.");
            }

            var lease = false;
            try
            {
                engine.DangerousAddRef(ref lease);
                ThrowProviderError(PluginNativeMethods.prototype_append_commit(memoryContext), "append commit");
            }
            finally
            {
                if (lease)
                {
                    engine.DangerousRelease();
                }
            }
        }
    }

    /// <summary>Reads native process-wide probe counters without returning credentials.</summary>
    /// <returns>The current native counters, managed error counters, and ABI sizes.</returns>
    public static ProbeStatistics GetStatistics()
    {
        PluginNativeMethods.EnsureLoaded();
        return new ProbeStatistics(
            PluginNativeMethods.prototype_release_count(),
            PluginNativeMethods.prototype_credential_requests(),
            PluginNativeMethods.prototype_credential_generation(),
            PluginNativeMethods.prototype_callback_count(),
            PluginNativeMethods.prototype_descriptor_size(),
            Marshal.SizeOf<NativeStringSlice>(), Marshal.SizeOf<NativeObjectMetadata>(),
            Marshal.SizeOf<NativeHandleResult>(), KernelErrors.Allocations, KernelErrors.Releases);
    }

    /// <summary>Releases the engine and its final native store reference, once.</summary>
    public void Dispose()
    {
        lock (gate)
        {
            if (disposed)
            {
                return;
            }

            disposed = true;
            engine.Dispose();
        }
    }

    private static NativeStoreSession AdoptCore(ref NativeStoreDescriptor descriptor, string tableUri, bool allowMemoryAppend)
    {
        var ownedDescriptor = descriptor;
        descriptor = default;
        var adopted = false;
        EngineHandle? engineOwner = null;
        try
        {
            KernelNativeMethods.EnsureLoaded();
            using var store = new NativeStoreHandle();
            using var initialBuilder = new EngineBuilderHandle();
            using var configuredBuilder = new EngineBuilderHandle();
            using var path = new Utf8Argument(tableUri);
            store.Initialize(KernelErrors.Unwrap(
                KernelNativeMethods.get_native_object_store(in ownedDescriptor, KernelErrors.Callback)));
            adopted = true;
            initialBuilder.Initialize(KernelErrors.Unwrap(
                KernelNativeMethods.get_engine_builder(path.Slice, KernelErrors.Callback)));
            configuredBuilder.Initialize(KernelErrors.Unwrap(
                KernelNativeMethods.builder_with_object_store(initialBuilder.Consume(), store)));
            store.Dispose();
            engineOwner = new EngineHandle();
            engineOwner.Initialize(KernelErrors.Unwrap(KernelNativeMethods.builder_build(configuredBuilder.Consume())));
            return new NativeStoreSession(engineOwner, tableUri, allowMemoryAppend ? ownedDescriptor.Context : 0);
        }
        catch
        {
            engineOwner?.Dispose();
            throw;
        }
        finally
        {
            if (!adopted && ownedDescriptor.Context != 0 && ownedDescriptor.Release != 0)
            {
                Marshal.GetDelegateForFunctionPointer<ReleaseContext>(ownedDescriptor.Release)(ownedDescriptor.Context);
            }
        }
    }

    private static void ThrowProviderError(int status, string operation)
    {
        if (status != 0)
        {
            throw new InvalidOperationException($"Native provider {operation} failed with status {status}.");
        }
    }

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    private delegate void ReleaseContext(nint context);
}