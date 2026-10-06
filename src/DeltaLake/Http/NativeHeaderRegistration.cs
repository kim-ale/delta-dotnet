using System;
using System.Threading;

namespace DeltaLake.Http
{
    internal sealed class NativeHeaderRegistration : IDisposable
    {
        private readonly Action<ulong> _unregister;
        private int _disposed;

        internal NativeHeaderRegistration(ulong context, Action<ulong> unregister)
        {
            Context = context;
            _unregister = unregister;
        }

        internal ulong Context { get; }

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 0) _unregister(Context);
        }
    }
}