using System.Runtime.InteropServices;
using System.Text;
using Microsoft.Win32.SafeHandles;

namespace DeltaKernel.NativePrototype;

internal sealed class Utf8Argument : SafeHandleZeroOrMinusOneIsInvalid
{
    private readonly nuint length;

    internal Utf8Argument(string value) : base(ownsHandle: true)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value);
        var bytes = Encoding.UTF8.GetBytes(value);
        length = (nuint)bytes.Length;
        SetHandle(Marshal.AllocHGlobal(checked(bytes.Length + 1)));
        try
        {
            Marshal.Copy(bytes, 0, handle, bytes.Length);
            Marshal.WriteByte(handle, bytes.Length, 0);
        }
        catch
        {
            Dispose();
            throw;
        }
    }

    internal NativeStringSlice Slice => new(handle, length);

    protected override bool ReleaseHandle()
    {
        Marshal.FreeHGlobal(handle);
        return true;
    }
}