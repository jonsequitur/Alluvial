using System;
using System.Threading.Tasks;
using Alluvial.Distributors.Sql;
using NUnit.Framework;

namespace Alluvial.Tests.Distributors
{
    [TestFixture]
    public class SqlBrokeredStreamQueryDistributorTests : StreamQueryDistributorTests
    {
        private IStreamQueryDistributor distributor;

        private readonly SqlBrokeredStreamQueryDistributorDatabaseSettings settings = new SqlBrokeredStreamQueryDistributorDatabaseSettings
        {
            ConnectionString = @"Data Source=(localdb)\v11.0; Integrated Security=True; MultipleActiveResultSets=False; Initial Catalog=AlluvialSqlDistributor"
        };

        protected override IStreamQueryDistributor CreateDistributor(
            Func<DistributorUnitOfWork, Task> onReceive = null,
            DistributorLease[] leases = null,
            int maxDegreesOfParallelism = 5,
            string name = null,
            TimeSpan? waitInterval = null)
        {
            distributor = new SqlBrokeredStreamQueryDistributor(
                leases ?? DefaultLeases,
                settings,
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
                return TimeSpan.FromSeconds(2);
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