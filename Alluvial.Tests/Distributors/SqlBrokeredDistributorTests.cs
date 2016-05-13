using System;
using System.Linq;
using System.Threading.Tasks;
using Alluvial.Distributors.Sql;
using FluentAssertions;
using NUnit.Framework;

namespace Alluvial.Tests.Distributors
{
    [TestFixture]
    public class SqlBrokeredDistributorTests : DistributorTests
    {
        private SqlBrokeredDistributor<int> distributor;

        public static readonly SqlBrokeredDistributorDatabase Database = new SqlBrokeredDistributorDatabase(
            @"Data Source=(localdb)\MSSQLLocalDB; Integrated Security=True; MultipleActiveResultSets=False; Initial Catalog=AlluvialSqlDistributorTests");

        protected override IDistributor<int> CreateDistributor(
            Func<Lease<int>, Task> onReceive = null,
            Leasable<int>[] leasables = null,
            int maxDegreesOfParallelism = 1,
            TimeSpan? waitInterval = null,
            string pool = null)
        {
            leasables = leasables ?? DefaultLeasables;

            pool = pool ?? DateTimeOffset.UtcNow.Ticks.ToString();
            distributor = new SqlBrokeredDistributor<int>(
                leasables,
                Database,
                pool,
                maxDegreesOfParallelism,
                waitInterval ?? TimeSpan.FromSeconds(1),
                DefaultLeaseDuration);

            if (onReceive != null)
            {
                distributor.OnReceive(onReceive);
            }

            Database.RegisterLeasableResources(leasables, pool).Wait();

            return distributor;
        }

        protected override TimeSpan DefaultLeaseDuration => TimeSpan.FromSeconds(2);

        protected override TimeSpan ClockDriftTolerance => TimeSpan.FromSeconds(3);

        [TestFixtureSetUp]
        public void TestFixtureSetUp() => Database.CreateDatabase().Timeout().Wait();

        [TearDown]
        public void TearDown() => distributor?.Stop().Timeout().Wait();

        [Test]
        public async Task Database_initialization_is_idempotent()
        {
            Action initialize = () =>
                                Database.InitializeSchema().Wait();

            initialize.ShouldNotThrow();
        }
    }
}