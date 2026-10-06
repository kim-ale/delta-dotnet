using System;
using System.Runtime.InteropServices;
using System.Threading;

namespace DeltaLake.Http
{
    internal sealed class SafeHandleLease : IDisposable
    {
        private SafeHandle? _owner;

        internal SafeHandleLease(SafeHandle owner)
        {
            var acquired = false;
            try
            {
                owner.DangerousAddRef(ref acquired);
                _owner = owner;
            }
            catch
            {
                if (acquired) owner.DangerousRelease();
                throw;
            }
        }

        public void Dispose() => Interlocked.Exchange(ref _owner, null)?.DangerousRelease();
    }
}