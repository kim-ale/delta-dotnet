using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Apache.Arrow.C;
using DeltaLake.Bridge.Interop;
using DeltaLake.Errors;
using DeltaLake.Http;

namespace DeltaLake.Bridge
{
    /// <summary>
    /// Core-owned Delta Rust runtime provisions physical Delta Tables at the
    /// storage level and delta-rs owned pointers to the table.
    /// </summary>
    internal class Runtime : SafeHandle
    {
        /// <summary>
        /// Gets the pointer to the bridged delta-rs runtime.
        /// </summary>
        internal unsafe Interop.Runtime* Ptr { get; private init; }

        /// <summary>
        /// Initializes a new instance of the <see cref="Runtime"/> class.
        /// </summary>
        /// <param name="options">Engine options.</param>
        /// <exception cref="InvalidOperationException">Any internal core error.</exception>
        internal Runtime(DeltaLake.Table.EngineOptions options)
            : base(IntPtr.Zero, true)
        {
            using var scope = new Scope();

            unsafe
            {
                var interopOptions = new RuntimeOptions
                {
                    data_fusion_execution_batch_size = new UIntPtr(options.DataFusionExecutionBatchSize),
                    data_fusion_runtime_max_spill_size = new UIntPtr(options.DataFusionRuntimeMaxSpillSize),
                    data_fusion_runtime_temp_directory = scope.ByteArray(options.DataFusionRuntimeTempDirectory),
                    data_fusion_runtime_max_temp_directory_size = new UIntPtr(options.DataFusionRuntimeMaxTempDirectorySize)
                };

                var res = Interop.Methods.runtime_new(&interopOptions);
                // If it failed, copy byte array, free runtime and byte array. Otherwise just
                // return runtime.
                if (res.fail != null)
                {
                    var message = ByteArrayRef.StrictUTF8.GetString(
                        res.fail->data,
                        (int)res.fail->size);
                    Interop.Methods.byte_array_free(res.runtime, res.fail);
                    Interop.Methods.runtime_free(res.runtime);
                    throw new InvalidOperationException(message);
                }
                Ptr = res.runtime;
                SetHandle((IntPtr)Ptr);
            }
        }

        internal virtual async Task<IntPtr> LoadTablePtrAsync(
            DeltaLake.Table.TableOptions options,
            System.Threading.CancellationToken cancellationToken)
        {
            options = StorageOptionsSnapshot.Capture(options);
            var buffer = ArrayPool<byte>.Shared.Rent(System.Text.Encoding.UTF8.GetByteCount(options.TableLocation));
#if NETCOREAPP
            var encodedLength = System.Text.Encoding.UTF8.GetBytes(options.TableLocation, buffer);
#else
            var encodedLength = System.Text.Encoding.UTF8.GetBytes(options.TableLocation, 0, options.TableLocation.Length, buffer, 0);
#endif
            try
            {
                return await LoadTablePtrAsync(buffer.AsMemory(0, encodedLength), options, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }

        internal virtual async Task<IntPtr> LoadTablePtrAsync(
            Memory<byte> tableUri,
            DeltaLake.Table.TableOptions options,
            System.Threading.CancellationToken cancellationToken)
        {
            options = StorageOptionsSnapshot.Capture(options);
            var tsc = new TaskCompletionSource<IntPtr>(TaskCreationOptions.RunContinuationsAsynchronously);
            using (var scope = new Scope(this))
            {
                var registration = options.RequestHeaderProvider == null ? null : scope.KeepAlive(
                    HeaderDispatcher.Register(NativeHeaderModule.Bridge, options.RequestHeaderProvider));
                unsafe
                {
                    var nativeOptions = new Interop.TableOptions()
                    {
                        version = new IntPtr(options.Version.HasValue ? unchecked((long)options.Version.Value) : -1L),
                        without_files = (byte)(options.WithoutFiles ? 1 : 0),
                        log_buffer_size = options.LogBufferSize ?? (nuint)0,
                        storage_options = options.StorageOptions != null ? scope.Dictionary(this, options.StorageOptions) : null,
                    };
                    var callback = scope.FunctionPointer<Interop.TableNewCallback>((success, fail) =>
                    {
                        CompleteTable(tsc, success, fail, cancellationToken);
                    });
                    var uri = scope.Pointer(scope.ByteArray(tableUri));
                    var nativeOptionsPtr = scope.Pointer(nativeOptions);
                    var token = scope.CancellationToken(cancellationToken);
                    if (registration == null) Interop.Methods.table_new(Ptr, uri, nativeOptionsPtr, token, callback);
                    else NativeMethods.table_new_with_headers(Ptr, uri, nativeOptionsPtr, token, registration.Context, callback);
                }

                return await tsc.Task.ConfigureAwait(false);
            }
        }

        internal virtual async Task<IntPtr> CreateTablePtrAsync(
            DeltaLake.Table.TableCreateOptions options,
            System.Threading.CancellationToken cancellationToken)
        {
            options = StorageOptionsSnapshot.Capture(options);
            var tsc = new TaskCompletionSource<IntPtr>(TaskCreationOptions.RunContinuationsAsynchronously);
            using (var scope = new Scope(this))
            {
                var registration = options.RequestHeaderProvider == null ? null : scope.KeepAlive(
                    HeaderDispatcher.Register(NativeHeaderModule.Bridge, options.RequestHeaderProvider));
                unsafe
                {
                    var nativeSchema = CArrowSchema.Create();
                    try
                    {
                        CArrowSchemaExporter.ExportSchema(options.Schema, nativeSchema);
                        var saveMode = Table.ConvertSaveMode(options.SaveMode);
                        var nativeOptions = new Interop.TableCreatOptions()
                        {
                            table_uri = scope.ByteArray(options.TableLocation),
                            schema = nativeSchema,
                            partition_by = scope.ArrayPointer(options.PartitionBy.Select(x => scope.ByteArray(x)).ToArray()),
                            partition_count = (nuint)options.PartitionBy.Count,
                            mode = saveMode.Ref,
                            name = scope.ByteArray(options.Name),
                            description = scope.ByteArray(options.Description),
                            configuration = scope.Dictionary(this, options.Configuration ?? new Dictionary<string, string>()),
                            custom_metadata = scope.Dictionary(this, options.CustomMetadata ?? new Dictionary<string, string>()),
                            storage_options = scope.Dictionary(this, options.StorageOptions ?? new Dictionary<string, string>()),
                        };
                        var callback = scope.FunctionPointer<Interop.TableNewCallback>((success, fail) =>
                            CompleteTable(tsc, success, fail, cancellationToken));
                        var nativeOptionsPtr = scope.Pointer(nativeOptions);
                        var token = scope.CancellationToken(cancellationToken);
                        if (registration == null) Interop.Methods.create_deltalake(Ptr, nativeOptionsPtr, token, callback);
                        else NativeMethods.create_deltalake_with_headers(Ptr, nativeOptionsPtr, token, registration.Context, callback);
                    }
                    finally
                    {
                        CArrowSchema.Free(nativeSchema);
                    }
                }

                return await tsc.Task.ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Free a byte array.
        /// </summary>
        /// <param name="byteArray">Byte array to free.</param>
        internal unsafe void FreeByteArray(Interop.ByteArray* byteArray)
        {
            using var lease = new SafeHandleLease(this);
            Interop.Methods.byte_array_free(Ptr, byteArray);
        }

        private unsafe void CompleteTable(TaskCompletionSource<IntPtr> completion, RawDeltaTable* success,
            DeltaTableError* fail, System.Threading.CancellationToken token)
        {
            try
            {
                var error = fail == null ? null : DeltaRuntimeException.FromDeltaTableError(Ptr, fail);
                if (token.IsCancellationRequested)
                {
                    if (success != null) Interop.Methods.table_free(success);
                    completion.TrySetCanceled(token);
                }
                else if (error != null)
                {
                    if (success != null) Interop.Methods.table_free(success);
                    completion.TrySetException(error);
                }
                else if (success == null)
                {
                    completion.TrySetException(new InvalidOperationException("Native table construction failed."));
                }
                else if (!completion.TrySetResult((IntPtr)success)) Interop.Methods.table_free(success);
            }
            catch (Exception error)
            {
                if (success != null) Interop.Methods.table_free(success);
                completion.TrySetException(error);
            }
        }

        #region SafeHandle implementation

        /// <inheritdoc />
        public override unsafe bool IsInvalid => handle == IntPtr.Zero;

        /// <inheritdoc />
        protected override unsafe bool ReleaseHandle()
        {
            Interop.Methods.runtime_free(Ptr);
            return true;
        }

        #endregion SafeHandle implementation
    }
}