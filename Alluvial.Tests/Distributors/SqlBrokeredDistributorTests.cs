using System;
using Alluvial.Distributors.Sql;
using FluentAssertions;
using System.Threading.Tasks;
using NUnit.Framework;

namespace Alluvial.Tests.Distributors
{
    [TestFixture]
    public class SqlBrokeredDistributorTests : DistributorTests
    {
        private SqlBrokeredDistributor<int> distributor;

        private static readonly string connectionString =
            @"Data Source=(localdb)\MSSQLLocalDB; Integrated Security=True; MultipleActiveResultSets=False; Initial Catalog=AlluvialSqlDistributorTests";

        public static readonly SqlBrokeredDistributorDatabase Database = new SqlBrokeredDistributorDatabase(
            connectionString);

        protected override IDistributor<int> CreateDistributor(
            Func<Lease<int>, Task> onReceive = null,
            Leasable<int>[] leasables = null,
            int maxDegreesOfParallelism = 1,
            string pool = null,
            TimeSpan? defaultLeaseDuration = null,
            bool autoRelease = true)
        {
            leasables = leasables ?? DefaultLeasables;

            pool = pool ?? Guid.NewGuid().ToString();

            Console.WriteLine(new { pool });

            if (pool.Length > 75)
            {
                pool = pool.Substring(0, 75);
            }

            distributor = new SqlBrokeredDistributor<int>(
                leasables,
                Database,
                pool,
                maxDegreesOfParallelism,
                defaultLeaseDuration ?? DefaultLeaseDuration)
            {
                AutoReleaseLeases = autoRelease
            };

            if (onReceive != null)
            {
                distributor.OnReceive(onReceive);
            }

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