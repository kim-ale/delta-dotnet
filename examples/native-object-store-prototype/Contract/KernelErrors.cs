using System.Runtime.InteropServices;

namespace DeltaKernel.NativePrototype;

internal enum FFIKernelError : int
{
    UnknownError = 0,
    FFIError = 1,
    ArrowError = 2,
    EngineDataTypeError = 3,
    ExtractError = 4,
    GenericError = 5,
    IOErrorError = 6,
    ParquetError = 7,
    ObjectStoreError = 8,
    ObjectStorePathError = 9,
    ReqwestError = 10,
    FileNotFoundError = 11,
    MissingColumnError = 12,
    UnexpectedColumnTypeError = 13,
    MissingDataError = 14,
    MissingVersionError = 15,
    DeletionVectorError = 16,
    InvalidUrlError = 17,
    MalformedJsonError = 18,
    MissingMetadataError = 19,
    MissingProtocolError = 20,
    InvalidProtocolError = 21,
    MissingMetadataAndProtocolError = 22,
    ParseError = 23,
    JoinFailureError = 24,
    Utf8Error = 25,
    ParseIntError = 26,
    InvalidColumnMappingModeError = 27,
    InvalidTableLocationError = 28,
    InvalidDecimalError = 29,
    InvalidStructDataError = 30,
    InternalError = 31,
    InvalidExpression = 32,
    InvalidLogPath = 33,
    FileAlreadyExists = 34,
    UnsupportedError = 35,
    ParseIntervalError = 36,
    ChangeDataFeedUnsupported = 37,
    ChangeDataFeedIncompatibleSchema = 38,
    InvalidCheckpoint = 39,
    CheckpointWriteError = 41,
    SchemaError = 42,
    LogHistoryError = 43,
    RowTrackingChangeFeedUnsupported = 44,
    CancelledError = 45,
    InvalidTransactionStateError = 46,
    InvalidLogSegment = 47,
    UnpublishedVersionError = 48,
    EmptyLogError = 49,
    InvalidSnapshotHint = 50,
    StartVersionNotFound = 51,
    InvalidGeoParamsError = 52,
    MaxCatalogVersionError = 53,
}

internal static class KernelErrors
{
    internal static readonly AllocateError Callback = Allocate;
    private static long allocations;
    private static long releases;

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate nint AllocateError(FFIKernelError error, NativeStringSlice message);

    internal static long Allocations => Interlocked.Read(ref allocations);
    internal static long Releases => Interlocked.Read(ref releases);

    internal static nint Unwrap(NativeHandleResult result)
    {
        if (result.Tag == 0)
        {
            if (result.Value == 0)
            {
                throw new InvalidOperationException("Kernel returned a null success handle.");
            }

            return result.Value;
        }

        if (result.Tag != 1)
        {
            throw new InvalidOperationException($"Kernel returned an unknown result tag: {result.Tag}.");
        }

        if (result.Value == 0)
        {
            throw new InvalidOperationException("Kernel returned an error without an allocated error record.");
        }

        try
        {
            var error = (FFIKernelError)Marshal.ReadInt32(result.Value);
            var message = Marshal.PtrToStringUTF8(Marshal.ReadIntPtr(result.Value, 8)) ?? string.Empty;
            throw new InvalidOperationException($"Kernel error {(int)error} ({error}): {message}");
        }
        finally
        {
            Marshal.FreeHGlobal(result.Value);
            Interlocked.Increment(ref releases);
        }
    }

    private static nint Allocate(FFIKernelError error, NativeStringSlice message)
    {
        nint allocation = 0;
        try
        {
            var length = checked((int)message.Length);
            var bytes = new byte[length];
            if (length != 0)
            {
                Marshal.Copy(message.Pointer, bytes, 0, length);
            }

            allocation = Marshal.AllocHGlobal(checked(16 + length + 1));
            var text = allocation + 16;
            Marshal.WriteInt32(allocation, (int)error);
            Marshal.WriteInt32(allocation, 4, 0);
            Marshal.WriteIntPtr(allocation, 8, text);
            Marshal.Copy(bytes, 0, text, length);
            Marshal.WriteByte(text, length, 0);
            Interlocked.Increment(ref allocations);
            return allocation;
        }
        catch (Exception)
        {
            if (allocation != 0)
            {
                Marshal.FreeHGlobal(allocation);
            }

            return 0;
        }
    }
}