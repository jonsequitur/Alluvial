using System;
using System.Collections.Concurrent;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Entity;
using System.Data.SqlClient;
using Alluvial.Tests;
using FluentAssertions;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Alluvial.Distributors.Sql;
using Its.Log.Instrumentation;
using Microsoft.Its.Domain.Sql;
using Microsoft.Its.Domain.Sql.Migrations;
using NUnit.Framework;

namespace Alluvial.For.ItsDomainSql.Tests
{
    [TestFixture]
    public class DatabaseSetupTests
    {
        [SetUp]
        public void SetUp()
        {
            try
            {
                Database.Delete(SchemaTestDbContext.ConnectionString);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToLogString());
                throw;
            }
        }

        [Test]
        public async Task The_ItsDomainSql_database_initializer_isnt_bothered_by_the_Alluvial_distributor_schema()
        {
            RunReadModelDbInitializer();

            var methodName = MethodBase.GetCurrentMethod().Name;

            using (var db = new SchemaTestDbContext())
            {
                db.Tallies.Add(new Tally
                {
                    Name = methodName,
                    Count = 1
                });
                await db.SaveChangesAsync();
            }

            using (var connection = new SqlConnection(SchemaTestDbContext.ConnectionString))
            {
                await SqlBrokeredDistributorDatabase.InitializeSchema(connection);
            }

            RunReadModelDbInitializer();

            using (var db = new SchemaTestDbContext())
            {
                db.Tallies.Should().Contain(t => t.Name == methodName,
                                            "the database should not be rebuilt due to the addition of the Alluvial schema");
            }
        }

        [Test]
        public async Task Alluvial_leases_can_be_initialized_using_an_ItsDomainSql_migration()
        {
            var leasables = LeasableFruits();

            var pool = "fruits";

            using (var db = new SchemaTestDbContext())
            {
                new CreateAndMigrate<SchemaTestDbContext>().InitializeDatabase(db);

                var migrator = new AlluvialDistributorLeaseInitializer<string>(
                    leasables,
                    pool);

                db.EnsureDatabaseIsUpToDate(migrator);
            }

            // assert by running a distributor and seeing that the expected leases can be obtained
            var distributor = new SqlBrokeredDistributor<string>(
                leasables,
                new SqlBrokeredDistributorDatabase(SchemaTestDbContext.ConnectionString),
                pool);

            var received = new ConcurrentBag<string>();
            distributor.OnReceive(async l => received.Add(l.Resource));

            await distributor.Distribute(3).Timeout();

            received.Should().Contain(leasables.Select(l => l.Resource));
        }

        [Test]
        public async Task Different_lease_pools_can_be_initialized_separately()
        {
            using (var db = new SchemaTestDbContext())
            {
                new CreateAndMigrate<SchemaTestDbContext>().InitializeDatabase(db);

                var migrator1 = new AlluvialDistributorLeaseInitializer<string>(
                    LeasableFruits(),
                    "fruits");
                var migrator2 = new AlluvialDistributorLeaseInitializer<string>(
                    LeasableVegetables(),
                    "vegetables");

                db.EnsureDatabaseIsUpToDate(migrator1, migrator2);
            }

            // assert
            var leasesDb = new SqlBrokeredDistributorDatabase(SchemaTestDbContext.ConnectionString);
            var leasables = await leasesDb.GetLeasables();

            leasables.Select(_ => _.ResourceName)
                     .Should()
                     .Contain(LeasableFruits().Select(_ => _.Name))
                     .And
                     .Contain(LeasableVegetables().Select(_ => _.Name));
        }

        private static Leasable<string>[] LeasableFruits()
        {
            return new[] { "apple", "orange", "banana" }
                .Select(s => new Leasable<string>(s, s))
                .ToArray();
        }

        private static Leasable<string>[] LeasableVegetables()
        {
            return new[] { "kale", "cabbage", "broccoli", "eggplant" }
                .Select(s => new Leasable<string>(s, s))
                .ToArray();
        }

        private static void RunReadModelDbInitializer()
        {
            using (var readModelDbContext = new SchemaTestDbContext())
            {
                var initializer = new ReadModelDatabaseInitializer<SchemaTestDbContext>();
                initializer.InitializeDatabase(readModelDbContext);
            }
        }
    }

    public class SchemaTestDbContext : ReadModelDbContext
    {
        public static readonly string ConnectionString =
            @"Data Source=(localdb)\MSSQLLocalDB; Integrated Security=True; MultipleActiveResultSets=False; Initial Catalog=AlluvialStreamsItsDomainSqlSchemaTest";

        public SchemaTestDbContext() : base(ConnectionString)
        {
        }

        public DbSet<Tally> Tallies { get; set; }
    }

    public class Tally
    {
        [Index]
        public int Id { get; set; }

        public string Name { get; set; }

        public int Count { get; set; }
    }
}