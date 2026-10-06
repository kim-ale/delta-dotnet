using System;
using System.Runtime.InteropServices;
using DeltaLake.Bridge.Interop;
using DeltaLake.Kernel.Interop;

namespace DeltaLake.Http
{
    internal enum NativeHeaderModule { Bridge, Kernel }

    [StructLayout(LayoutKind.Sequential)]
    internal struct ByteSlice
    {
        internal IntPtr Data;
        internal UIntPtr Length;
    }

    [StructLayout(LayoutKind.Sequential)]
    internal struct HeaderCallbacks
    {
        internal uint AbiVersion;
        internal uint StructSize;
        internal ulong Context;
        internal IntPtr Begin;
        internal IntPtr Cancel;
        internal IntPtr Released;
    }

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate uint HeaderBegin(ulong context, ulong request, ByteSlice method, ByteSlice uri);
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void HeaderCancel(ulong context, ulong request);
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void HeaderReleased(ulong context);

    internal static unsafe class NativeMethods
    {
        [DllImport("delta_rs_bridge", CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
        internal static extern uint bridge_headers_register(in HeaderCallbacks callbacks);
        [DllImport("delta_rs_bridge", CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
        internal static extern void bridge_headers_unregister(ulong context);
        [DllImport("delta_rs_bridge", CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
        internal static extern uint bridge_headers_complete(ulong context, ulong request, uint status, ByteSlice bytes);
        [DllImport("delta_rs_bridge", CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
        internal static extern void table_new_with_headers(DeltaLake.Bridge.Interop.Runtime* runtime,
            ByteArrayRef* uri, DeltaLake.Bridge.Interop.TableOptions* options,
            DeltaLake.Bridge.Interop.CancellationToken* token, ulong context, IntPtr callback);
        [DllImport("delta_rs_bridge", CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
        internal static extern void create_deltalake_with_headers(DeltaLake.Bridge.Interop.Runtime* runtime,
            TableCreatOptions* options, DeltaLake.Bridge.Interop.CancellationToken* token,
            ulong context, IntPtr callback);

        [DllImport("delta_kernel_ffi", CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
        internal static extern uint kernel_headers_register(in HeaderCallbacks callbacks);
        [DllImport("delta_kernel_ffi", CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
        internal static extern void kernel_headers_unregister(ulong context);
        [DllImport("delta_kernel_ffi", CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
        internal static extern uint kernel_headers_complete(ulong context, ulong request, uint status, ByteSlice bytes);
        [DllImport("delta_kernel_ffi", CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
        internal static extern ExternResultHandleSharedExternEngine kernel_engine_with_headers(
            KernelStringSlice path, KernelStringSlice* keys, KernelStringSlice* values,
            UIntPtr count, ulong context, IntPtr allocateError);
    }
}