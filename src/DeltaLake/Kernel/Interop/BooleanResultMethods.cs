using System;
using System.Runtime.InteropServices;

namespace DeltaLake.Kernel.Interop
{
    internal static unsafe class BooleanResultMethods
    {
        [StructLayout(LayoutKind.Sequential)]
        internal struct BooleanResult
        {
            internal ExternResultbool_Tag Tag;
            internal Payload Value;

            internal ExternResultbool ToManaged()
            {
                var result = new ExternResultbool { tag = Tag };
                if (Tag == ExternResultbool_Tag.Okbool) result.Anonymous.Anonymous1.ok = Value.Ok != 0;
                else result.Anonymous.Anonymous2.err = Value.Error;
                return result;
            }
        }

        [StructLayout(LayoutKind.Explicit)]
        internal struct Payload
        {
            [FieldOffset(0)]
            internal byte Ok;
            [FieldOffset(0)]
            internal EngineError* Error;
        }

        internal static ExternResultbool ScanMetadataNext(SharedScanMetadataIterator* iterator, void* context, IntPtr visitor) =>
            ScanMetadataNextNative(iterator, context, visitor).ToManaged();

        internal static ExternResultbool ReadResultNext(ExclusiveFileReadResultIterator* iterator, void* context, IntPtr visitor) =>
            ReadResultNextNative(iterator, context, visitor).ToManaged();

        internal static ExternResultbool SetBuilderOption(EngineBuilder* builder, KernelStringSlice key, KernelStringSlice value) =>
            SetBuilderOptionNative(builder, key, value).ToManaged();

        internal static ExternResultbool VisitScanMetadata(SharedScanMetadata* metadata, SharedExternEngine* engine, void* context, IntPtr visitor) =>
            VisitScanMetadataNative(metadata, engine, context, visitor).ToManaged();

        [DllImport("delta_kernel_ffi", EntryPoint = "selection_vector_from_dv", CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
        internal static extern ExternResultKernelBoolSlice SelectionVectorFromDv(IntPtr info, SharedExternEngine* engine, KernelStringSlice root);

        [DllImport("delta_kernel_ffi", EntryPoint = "scan_metadata_next", CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
        private static extern BooleanResult ScanMetadataNextNative(SharedScanMetadataIterator* iterator, void* context, IntPtr visitor);

        [DllImport("delta_kernel_ffi", EntryPoint = "read_result_next", CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
        private static extern BooleanResult ReadResultNextNative(ExclusiveFileReadResultIterator* iterator, void* context, IntPtr visitor);

        [DllImport("delta_kernel_ffi", EntryPoint = "set_builder_option", CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
        private static extern BooleanResult SetBuilderOptionNative(EngineBuilder* builder, KernelStringSlice key, KernelStringSlice value);

        [DllImport("delta_kernel_ffi", EntryPoint = "visit_scan_metadata", CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
        private static extern BooleanResult VisitScanMetadataNative(SharedScanMetadata* metadata, SharedExternEngine* engine, void* context, IntPtr visitor);
    }
}