using System;
using FluentAssertions;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;

namespace Alluvial.Tests.Distributors
{
    [TestFixture]
    public class DistributorBaseTests
    {
        private Leasable<int>[] leasables;

        [SetUp]
        public void SetUp()
        {
            leasables = Enumerable.Range(1, 10)
                                  .Select(i => new Leasable<int>(i, i.ToString()))
                                  .ToArray();
        }

        [Test]
        public async Task An_exception_during_AcquireLease_doesnt_stop_the_distributor()
        {
            var acquireCount = 0;
            var receiveCount = 0;

            using (var distributor = new TestDistributor<int>(
                leasables,
                beforeAcquire: async () =>
                {
                    if (Interlocked.Increment(ref acquireCount) == 2)
                    {
                        throw new Exception("dang!");
                    }
                }).Trace())
            {
                distributor.OnReceive(async (lease, next) => { Interlocked.Increment(ref receiveCount); });

                await distributor.Start();

                await Task.Delay(100);
            }

            receiveCount.Should().BeGreaterThan(5);
        }

        [Test]
        public async Task An_exception_during_ReleaseLease_doesnt_stop_the_distributor()
        {
            var releaseCount = 0;
            var receiveCount = 0;

            using (var distributor = new TestDistributor<int>(
                leasables,
                beforeRelease: async lease =>
                {
                    if (Interlocked.Increment(ref releaseCount) == 2)
                    {
                        throw new Exception("dang!");
                    }
                }).Trace())
            {
                distributor.OnReceive(async (lease, next) => { Interlocked.Increment(ref receiveCount); });

                await distributor.Start();

                await Task.Delay(100);
            }

            receiveCount.Should().BeGreaterThan(5);
        }
    }

    public class TestDistributor<T> : InMemoryDistributor<T>
    {
        private readonly Func<Task> beforeAcquire;
        private readonly Func<Lease<T>, Task> beforeRelease;

        public TestDistributor(
            Leasable<T>[] leasables,
            Func<Task> beforeAcquire = null,
            Func<Lease<T>, Task> beforeRelease = null,
            [CallerMemberName] string pool = null,
            int maxDegreesOfParallelism = 5) :
                base(leasables, pool, maxDegreesOfParallelism)
        {
            this.beforeAcquire = beforeAcquire ?? (async () => { });
            this.beforeRelease = beforeRelease ?? (async lease => { });
        }

        protected override async Task ReleaseLease(Lease<T> lease)
        {
            await beforeRelease(lease);
            await base.ReleaseLease(lease);
        }

        protected override async Task<Lease<T>> AcquireLease()
        {
            await beforeAcquire();
            return await base.AcquireLease();
        }
    }
}