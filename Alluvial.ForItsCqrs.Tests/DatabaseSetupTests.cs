using System;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Entity;
using System.Data.SqlClient;
using FluentAssertions;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Alluvial.Distributors.Sql;
using Microsoft.Its.Domain.Sql;
using NUnit.Framework;

namespace Alluvial.ForItsCqrs.Tests
{
    [TestFixture]
    public class DatabaseSetupTests
    {
        public static readonly string connectionString =
            @"Data Source=(localdb)\v11.0; Integrated Security=True; MultipleActiveResultSets=False; Initial Catalog=AlluvialForItsCqrsSchemaTest";

        [Test]
        public async Task The_ItsDomainSql_database_initializer_isnt_bothered_by_the_Alluvial_distributor_schema()
        {
            RunReadModelDbInitializer();

            var methodName = MethodBase.GetCurrentMethod().Name;

            using (var db = new AlluvialForItsCqrsSchemaTestDbContext())
            {
                db.Tallies.Add(new Tally
                {
                    Name = methodName,
                    Count = 1
                });
                await db.SaveChangesAsync();
            }

            var distributorDb = new SqlBrokeredDistributorDatabase(connectionString);
            
            using (var connection = new SqlConnection(connectionString))
            {
                await distributorDb.InitializeSchema(connection);
            }

            RunReadModelDbInitializer();

            using (var db = new AlluvialForItsCqrsSchemaTestDbContext())
            {
                db.Tallies.Should().Contain(t => t.Name == methodName,
                                            "the database should not be rebuilt due to the addition of the Alluvial schema");
            }
        }

        private static void RunReadModelDbInitializer()
        {
            using (var readModelDbContext = new AlluvialForItsCqrsSchemaTestDbContext())
            {
                var initializer = new ReadModelDatabaseInitializer<AlluvialForItsCqrsSchemaTestDbContext>();
                initializer.InitializeDatabase(readModelDbContext);
            }
        }
    }

    public class AlluvialForItsCqrsSchemaTestDbContext : ReadModelDbContext
    {
        public static readonly string ConnectionString =
            @"Data Source=(localdb)\v11.0; Integrated Security=True; MultipleActiveResultSets=False; Initial Catalog=AlluvialForItsCqrsSchemaTest";

        public AlluvialForItsCqrsSchemaTestDbContext() : base(ConnectionString)
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