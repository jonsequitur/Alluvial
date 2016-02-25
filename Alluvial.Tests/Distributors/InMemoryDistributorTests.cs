using System;
using System.Threading.Tasks;
using NUnit.Framework;

namespace Alluvial.Tests.Distributors
{
    [TestFixture]
    public class InMemoryDistributorTests : DistributorTests
    {
        private InMemoryDistributor<int> distributor;

        protected override IDistributor<int> CreateDistributor(
            Func<Lease<int>, Task> onReceive = null,
            Leasable<int>[] leasables = null,
            int maxDegreesOfParallelism = 5,
            string name = null,
            TimeSpan? waitInterval = null,
            string pool = null)
        {
            distributor = new InMemoryDistributor<int>(
                leasables ?? DefaultLeasables,
                pool ?? DateTimeOffset.UtcNow.Ticks.ToString(),
                maxDegreesOfParallelism,
                waitInterval,
                DefaultLeaseDuration);
            if (onReceive != null)
            {
                distributor.OnReceive(onReceive);
            }
            return distributor;
        }

        protected override TimeSpan DefaultLeaseDuration => TimeSpan.FromSeconds(1);

        protected override TimeSpan ClockDriftTolerance => TimeSpan.FromMilliseconds(30);

        [TearDown]
        public void TearDown() => distributor?.Dispose();
    }
}