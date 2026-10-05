// -----------------------------------------------------------------------------
// <summary>
// The Delta Kernel FFI Runtime.
// </summary>
//
// <copyright company="The Delta Lake Project Authors">
// Copyright (2024) The Delta Lake Project Authors.  All rights reserved.
// Licensed under the Apache license. See LICENSE file in the project root for full license information.
// </copyright>
// -----------------------------------------------------------------------------

using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using DeltaLake.Bridge.Interop;
using DeltaLake.Kernel.Credentials;
using DeltaLake.Kernel.State;
using DeltaLake.Table;
using DeltaRustBridge = DeltaLake.Bridge;

namespace DeltaLake.Kernel.Core
{
    /// <summary>
    /// Core-owned Delta Kernel runtime.
    ///
    /// The idea is, we prioritize the Kernel FFI implementations, and
    /// operations not supported by the FFI (such as create table, load version)
    /// yet falls back to Delta RS Runtime implementation.
    /// </summary>
    internal class Runtime : DeltaRustBridge.Runtime
    {
        private int closing;

        /// <summary>
        /// Initializes a new instance of the <see cref="Runtime"/> class.
        /// </summary>
        /// <param name="options">Engine options.</param>
        internal Runtime(EngineOptions options)
            : base(options) { }

        /// <summary>
        /// A thin wrapper around the Delta Rust Runtime to load a table.
        /// </summary>
        /// <param name="options">Table options.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        internal async Task<Table> LoadTableAsync(
            DeltaLake.Table.TableOptions options,
            System.Threading.CancellationToken cancellationToken
        )
        {
            var credential = options.KernelAzureBearerCredential;
            if (credential != null)
            {
                ThrowIfClosing();
                using var runtimeLease = new SafeHandleLease(this);
                var snapshot = KernelCredentialBootstrap.Snapshot(options) with { KernelAzureBearerCredential = credential };
                KernelCredentialRegistration.EnsureSupported();
                KernelCredentialBootstrap.ValidateStorage(snapshot);
                var token = await KernelCredentialBootstrap.AcquireAsync(credential, cancellationToken).ConfigureAwait(false);
                ThrowIfClosing();
                var bridgeOptions = KernelCredentialBootstrap.WithToken(snapshot, token);
                IntPtr providerTablePtr = await base.LoadTablePtrAsync(bridgeOptions, cancellationToken).ConfigureAwait(false);
                unsafe
                {
                    return new Table(this, (RawDeltaTable*)providerTablePtr, snapshot);
                }
            }

            IntPtr tablePtr = await base.LoadTablePtrAsync(options, cancellationToken).ConfigureAwait(false);
            unsafe
            {
                return new Table(this, (RawDeltaTable*)tablePtr, options);
            }
        }

        /// <summary>
        /// A thin wrapper around the Delta Rust Runtime to create a table.
        /// </summary>
        /// <param name="options">Table creation options.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        internal async Task<Table> CreateTableAsync(
            DeltaLake.Table.TableCreateOptions options,
            System.Threading.CancellationToken cancellationToken
        )
        {
            var credential = options.KernelAzureBearerCredential;
            if (credential != null)
            {
                ThrowIfClosing();
                using var runtimeLease = new SafeHandleLease(this);
                var snapshot = KernelCredentialBootstrap.Snapshot(options) with { KernelAzureBearerCredential = credential };
                KernelCredentialRegistration.EnsureSupported();
                KernelCredentialBootstrap.ValidateStorage(snapshot);
                var token = await KernelCredentialBootstrap.AcquireAsync(credential, cancellationToken).ConfigureAwait(false);
                ThrowIfClosing();
                var bridgeOptions = KernelCredentialBootstrap.WithToken(snapshot, token);
                IntPtr providerTablePtr = await base.CreateTablePtrAsync(bridgeOptions, cancellationToken).ConfigureAwait(false);
                unsafe
                {
                    return new Table(this, (RawDeltaTable*)providerTablePtr, snapshot);
                }
            }

            IntPtr tablePtr = await base.CreateTablePtrAsync(options, cancellationToken).ConfigureAwait(false);
            unsafe
            {
                return new Table(this, (RawDeltaTable*)tablePtr, options);
            }
        }

        protected override void Dispose(bool disposing)
        {
            Interlocked.Exchange(ref closing, 1);
            base.Dispose(disposing);
        }

        [SuppressMessage("Maintainability", "CA1513", Justification = "Explicit disposal guards preserve net472 compatibility; ObjectDisposedException.ThrowIf is unavailable on that target.")]
        private void ThrowIfClosing()
        {
            if (Volatile.Read(ref closing) != 0) throw new ObjectDisposedException(nameof(Runtime));
        }
    }
}
