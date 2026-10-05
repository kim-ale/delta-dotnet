using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading;

namespace DeltaLake.Kernel.State
{
    internal sealed class KernelLifetimeOwner
    {
        private readonly object _gate = new object();
        private Action? _cleanup;
        private bool _closed;
        private int _leases;

        internal KernelLifetimeOwner(Action cleanup)
        {
            _cleanup = cleanup ?? throw new ArgumentNullException(nameof(cleanup));
        }

        [SuppressMessage("Maintainability", "CA1513", Justification = "Explicit disposal guards preserve net472 compatibility; ObjectDisposedException.ThrowIf is unavailable on that target.")]
        internal IDisposable EnterOperation()
        {
            lock (_gate)
            {
                if (_closed)
                {
                    throw new ObjectDisposedException(nameof(KernelLifetimeOwner));
                }

                var lease = new Lease(this);
                _leases++;
                return lease;
            }
        }

        internal void Close()
        {
            Action? cleanup;
            lock (_gate)
            {
                _closed = true;
                cleanup = TakeCleanup();
            }

            cleanup?.Invoke();
        }

        private Action? TakeCleanup()
        {
            if (!_closed || _leases != 0)
            {
                return null;
            }

            var cleanup = _cleanup;
            _cleanup = null;
            return cleanup;
        }

        private void ReleaseOperation()
        {
            Action? cleanup;
            lock (_gate)
            {
                _leases--;
                cleanup = TakeCleanup();
            }

            cleanup?.Invoke();
        }

        private sealed class Lease : IDisposable
        {
            private KernelLifetimeOwner? _owner;

            internal Lease(KernelLifetimeOwner owner)
            {
                _owner = owner;
            }

            public void Dispose()
            {
                Interlocked.Exchange(ref _owner, null)?.ReleaseOperation();
            }
        }
    }
}