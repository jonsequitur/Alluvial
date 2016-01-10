using System;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Entity;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Alluvial.Distributors.Sql;
using FluentAssertions;
using Microsoft.Its.Domain.Sql;
using NUnit.Framework;

namespace Alluvial.Streams.ItsDomainSql.Tests
{
    [TestFixture]
    public class DatabaseSetupTests
    {
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

            var distributorDb = new SqlBrokeredDistributorDatabase(SchemaTestDbContext.ConnectionString);
            
            using (var connection = new SqlConnection(SchemaTestDbContext.ConnectionString))
            {
                await distributorDb.InitializeSchema(connection);
            }

            RunReadModelDbInitializer();

            using (var db = new SchemaTestDbContext())
            {
                db.Tallies.Should().Contain(t => t.Name == methodName,
                                            "the database should not be rebuilt due to the addition of the Alluvial schema");
            }
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