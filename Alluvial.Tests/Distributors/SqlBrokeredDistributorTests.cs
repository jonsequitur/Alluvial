using System;
using System.Threading.Tasks;
using Alluvial.Distributors;
using Alluvial.Distributors.Sql;
using NUnit.Framework;

namespace Alluvial.Tests.Distributors
{
    [TestFixture]
    public class SqlBrokeredDistributorTests : DistributorTests
    {
        private SqlBrokeredDistributor<int> distributor;

        public static readonly SqlBrokeredDistributorDatabase Database = new SqlBrokeredDistributorDatabase(
            @"Data Source=(localdb)\v11.0; Integrated Security=True; MultipleActiveResultSets=False; Initial Catalog=AlluvialSqlDistributor");

        protected override IDistributor<int> CreateDistributor(
            Func<Lease<int>, Task> onReceive = null,
            Leasable<int>[] leasables = null,
            int maxDegreesOfParallelism = 1,
            string name = null,
            TimeSpan? waitInterval = null,
            string pool = null)
        {
            leasables = leasables ?? DefaultLeasable;

            pool = pool ?? DateTimeOffset.UtcNow.Ticks.ToString();
            distributor = new SqlBrokeredDistributor<int>(
                leasables,
                Database,
                pool,
                maxDegreesOfParallelism,
                waitInterval,
                DefaultLeaseDuration);

            if (onReceive != null)
            {
                distributor.OnReceive(onReceive);
            }

            Database.RegisterLeasableResources(leasables, pool).Wait();

            return distributor;
        }

        protected override TimeSpan DefaultLeaseDuration
        {
            get
            {
                return TimeSpan.FromSeconds(2);
            }
        }

        protected override TimeSpan ClockDriftTolerance
        {
            get
            {
                return TimeSpan.FromSeconds(3);
            }
        }

        [TestFixtureSetUp]
        public void TestFixtureSetUp()
        {
            Database.CreateDatabase().Wait();
        }

        [TearDown]
        public void TearDown()
        {
            if (distributor != null)
            {
                distributor.Stop().Wait();
            }
        }
    }
}