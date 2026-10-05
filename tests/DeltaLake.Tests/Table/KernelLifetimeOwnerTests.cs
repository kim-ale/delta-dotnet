using System.Runtime.CompilerServices;
using DeltaLake.Kernel.Shim.Async;
using DeltaLake.Kernel.State;
using DeltaLake.Table;

namespace DeltaLake.Tests.Table
{
    public class KernelLifetimeOwnerTests
    {
        [Fact]
        public async Task GivenConcurrentConsumers_WhenClosingAndDisposing_CleanupRunsExactlyOnce()
        {
            var cleanups = 0;
            var owner = new KernelLifetimeOwner(() => Interlocked.Increment(ref cleanups));
            var leases = Enumerable.Range(0, 32).Select(_ => owner.EnterOperation()).ToArray();

            await Task.WhenAll(leases.Select(lease => Task.Run(() =>
            {
                owner.Close();
                lease.Dispose();
                lease.Dispose();
            })));

            owner.Close();
            Assert.Equal(1, cleanups);
            Assert.Throws<ObjectDisposedException>(() => owner.EnterOperation());
        }

        [Fact]
        public void GivenFailingCleanup_WhenClosedAgain_DoesNotRepeatCleanup()
        {
            var cleanups = 0;
            var owner = new KernelLifetimeOwner(() =>
            {
                cleanups++;
                throw new InvalidOperationException("Cleanup failed.");
            });

            Assert.Throws<InvalidOperationException>(() => owner.Close());
            owner.Close();

            Assert.Equal(1, cleanups);
            Assert.Throws<ObjectDisposedException>(() => owner.EnterOperation());
        }

        [Fact]
        public void GivenClosedOwner_WhenEnteringOperation_ThrowsWithoutRepeatingCleanup()
        {
            var cleanups = 0;
            var owner = new KernelLifetimeOwner(() => cleanups++);

            owner.Close();
            owner.Close();

            Assert.Throws<ObjectDisposedException>(() => owner.EnterOperation());
            Assert.Equal(1, cleanups);
        }

        [Fact]
        public void GivenMultipleConsumers_WhenClosed_CleansUpOnlyAfterLastConsumer()
        {
            var events = new List<string>();
            var owner = new KernelLifetimeOwner(() =>
            {
                events.Add("state");
                events.Add("schema-and-pins");
                events.Add("engine");
                events.Add("registration");
            });
            var operation = owner.EnterOperation();
            var iterator = owner.EnterOperation();

            owner.Close();
            operation.Dispose();
            operation.Dispose();
            Assert.Empty(events);
            Assert.Throws<ObjectDisposedException>(() => owner.EnterOperation());

            iterator.Dispose();
            iterator.Dispose();
            owner.Close();

            Assert.Equal(new[] { "state", "schema-and-pins", "engine", "registration" }, events);
        }

        [Fact]
        public void GivenOpenOwner_WhenLeaseEnds_CleanupWaitsForClose()
        {
            var cleanups = 0;
            var owner = new KernelLifetimeOwner(() => cleanups++);

            owner.EnterOperation().Dispose();
            Assert.Equal(0, cleanups);

            owner.Close();
            Assert.Equal(1, cleanups);
        }

        [Fact]
        public async Task GivenCanceledQueuedOperation_WhenShimReturns_ReleasesAdmission()
        {
            var cleanups = 0;
            var invoked = false;
            var owner = new KernelLifetimeOwner(() => cleanups++);
            var cancellation = new CancellationToken(canceled: true);

            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            {
                using var lease = owner.EnterOperation();
                owner.Close();
                await SyncToAsyncShim.ExecuteAsync(() => invoked = true, cancellation);
            });

            Assert.False(invoked);
            Assert.Equal(1, cleanups);
        }

        [Fact]
        public async Task GivenRunningOperation_WhenClosedAndCanceled_CleanupWaitsForActualReturn()
        {
            var cleanups = 0;
            var entered = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var finish = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var owner = new KernelLifetimeOwner(() => Interlocked.Increment(ref cleanups));
            using var cancellation = new CancellationTokenSource();

            async Task ExecuteAsync()
            {
                using var lease = owner.EnterOperation();
                await SyncToAsyncShim.ExecuteAsync(() =>
                {
                    entered.SetResult(true);
                    finish.Task.GetAwaiter().GetResult();
                    Assert.Equal(0, cleanups);
                    return true;
                }, cancellation.Token);
                Assert.Equal(0, cleanups);
            }

            var pending = ExecuteAsync();
            await entered.Task;
            try
            {
                owner.Close();
#if NET8_0_OR_GREATER
                await cancellation.CancelAsync();
#else
                cancellation.Cancel();
#endif
                Assert.Equal(0, cleanups);
                Assert.Throws<ObjectDisposedException>(() => owner.EnterOperation());
            }
            finally
            {
                finish.TrySetResult(true);
            }

            await pending;
            Assert.Equal(1, cleanups);
        }

        [Fact]
        public void GivenRetainedLease_WhenCollected_TableStorageAndOutputLiveUntilCleanupThenBecomeCollectible()
        {
            var (owner, lease, table, storage, output) = CreateRetainedResource();
            owner.Close();
            Collect();
            Assert.True(table.IsAlive);
            Assert.True(storage.IsAlive);
            Assert.True(output.IsAlive);

            lease.Dispose();
            Collect();
            Assert.False(table.IsAlive);
            Assert.False(storage.IsAlive);
            Assert.False(output.IsAlive);
            GC.KeepAlive(owner);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static (KernelLifetimeOwner Owner, IDisposable Lease, WeakReference Table, WeakReference Storage, WeakReference Output) CreateRetainedResource()
        {
            var table = new RetainedTableResources();
            var owner = new KernelLifetimeOwner(() => GC.KeepAlive(table));
            return (owner, owner.EnterOperation(), new WeakReference(table),
                new WeakReference(table.Storage), new WeakReference(table.Output));
        }

        private static void Collect()
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
        }

        private sealed class RetainedTableResources
        {
            internal TableStorageOptions Storage { get; } = new TableStorageOptions();
            internal object Output { get; } = new object();
        }
    }
}