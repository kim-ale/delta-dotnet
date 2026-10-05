using System;
using System.Runtime.InteropServices;
using System.Threading;

namespace DeltaLake.Kernel.State
{
    internal sealed class SafeHandleLease : IDisposable
    {
        private SafeHandle? _handle;

        internal SafeHandleLease(SafeHandle handle)
        {
            var acquired = false;
            try
            {
                handle.DangerousAddRef(ref acquired);
                _handle = handle;
            }
            catch
            {
                if (acquired) handle.DangerousRelease();
                throw;
            }
        }

        public void Dispose() => Interlocked.Exchange(ref _handle, null)?.DangerousRelease();
    }
}