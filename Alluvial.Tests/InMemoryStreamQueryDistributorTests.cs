using System;
using System.Threading.Tasks;
using NUnit.Framework;

namespace Alluvial.Tests
{
    [TestFixture]
    public class InMemoryStreamQueryDistributorTests : StreamQueryDistributorTests
    {
        private InMemoryStreamQueryDistributor distributor;

        protected override IStreamQueryDistributor CreateDistributor(
            Lease[] leases,
            Func<DistributorUnitOfWork, Task> onReceive = null,
            int maxDegreesOfParallelism = 5,
            string name = null,
            TimeSpan? waitInterval = null)
        {
            distributor = new InMemoryStreamQueryDistributor(leases, maxDegreesOfParallelism, waitInterval);
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