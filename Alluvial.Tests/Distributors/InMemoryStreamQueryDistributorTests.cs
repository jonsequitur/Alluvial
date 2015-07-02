using System;
using System.Threading.Tasks;
using Alluvial.Distributors;
using NUnit.Framework;

namespace Alluvial.Tests.Distributors
{
    [TestFixture]
    public class InMemoryStreamQueryDistributorTests : StreamQueryDistributorTests
    {
        private InMemoryStreamQueryDistributor distributor;

        protected override IStreamQueryDistributor CreateDistributor(
            Func<Lease, Task> onReceive = null,
            LeasableResource[] leasableResources = null,
            int maxDegreesOfParallelism = 5,
            string name = null,
            TimeSpan? waitInterval = null)
        {
            distributor = new InMemoryStreamQueryDistributor(
                leasableResources ?? DefaultLeasableResources,
                maxDegreesOfParallelism,
                waitInterval);
            if (onReceive != null)
            {
                distributor.OnReceive(onReceive);
            }
            return distributor;
        }

        protected override TimeSpan DefaultLeaseDuration
        {
            get
            {
                return TimeSpan.FromSeconds(1);
            }
        }

        [TearDown]
        public void TearDown()
        {
            if (distributor != null)
            {
                distributor.Dispose();
            }
        }
    }
}