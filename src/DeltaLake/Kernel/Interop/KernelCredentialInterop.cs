using System;
using System.Runtime.InteropServices;

namespace DeltaLake.Kernel.Interop
{
    [StructLayout(LayoutKind.Sequential)]
    internal struct KernelCredentialRequestV1
    {
        internal uint AbiVersion;
        internal uint StructSize;
        internal ulong ContextId;
        internal ulong RequestId;
        internal uint CredentialKind;
        internal uint TimeoutMs;
        internal uint MinimumLifetimeMs;
        internal uint Flags;
        internal ulong Reserved0;
        internal ulong Reserved1;
    }

    [StructLayout(LayoutKind.Sequential)]
    internal unsafe struct KernelCredentialResultV1
    {
        internal uint AbiVersion;
        internal uint StructSize;
        internal uint Status;
        internal uint CredentialKind;
        internal byte* TokenUtf8;
        internal ulong TokenLen;
        internal long ExpiresUnixMs;
        internal ulong Reserved0;
        internal ulong Reserved1;
    }

    [StructLayout(LayoutKind.Sequential)]
    internal struct KernelCredentialRegistrationV1
    {
        internal uint AbiVersion;
        internal uint StructSize;
        internal ulong ContextId;
        internal uint CredentialKind;
        internal uint AcquisitionTimeoutMs;
        internal uint MaxTokenBytes;
        internal uint Flags;
        internal IntPtr Begin;
        internal IntPtr Cancel;
        internal IntPtr Released;
        internal ulong Reserved0;
        internal ulong Reserved1;
    }

    [StructLayout(LayoutKind.Sequential)]
    internal unsafe struct KernelCredentialOptionV1
    {
        internal byte* KeyUtf8;
        internal ulong KeyLen;
        internal byte* ValueUtf8;
        internal ulong ValueLen;
    }

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal unsafe delegate uint KernelCredentialBegin(KernelCredentialRequestV1* request);

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void KernelCredentialCancel(ulong contextId, ulong requestId, uint reason);

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void KernelCredentialReleased(ulong contextId);

    internal static unsafe class KernelCredentialInterop
    {
        internal const uint AbiVersion = 1;
        internal const uint AzureBearer = 1;

        [DllImport("delta_kernel_ffi", CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
        internal static extern uint kernel_credential_abi_version();

        [DllImport("delta_kernel_ffi", CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
        internal static extern uint kernel_credential_context_new(out ulong contextId);

        [DllImport("delta_kernel_ffi", CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
        internal static extern uint kernel_credential_register(KernelCredentialRegistrationV1* registration);

        [DllImport("delta_kernel_ffi", CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
        internal static extern uint kernel_credential_activate(ulong contextId);

        [DllImport("delta_kernel_ffi", CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
        internal static extern uint kernel_credential_unregister(ulong contextId);

        [DllImport("delta_kernel_ffi", CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
        internal static extern uint kernel_credential_complete(ulong contextId, ulong requestId, KernelCredentialResultV1* result);

        [DllImport("delta_kernel_ffi", CallingConvention = CallingConvention.Cdecl, ExactSpelling = true)]
        internal static extern ExternResultHandleSharedExternEngine kernel_engine_new_with_credential_v1(
            KernelStringSlice tableUri,
            KernelCredentialOptionV1* options,
            ulong optionCount,
            ulong contextId,
            IntPtr allocateError,
            uint workerThreads,
            uint maxBlockingThreads);
    }
}