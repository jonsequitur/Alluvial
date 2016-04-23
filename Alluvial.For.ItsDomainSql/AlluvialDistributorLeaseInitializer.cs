using System;
using System.Data.Entity;
using System.Threading.Tasks;
using Alluvial.Distributors.Sql;
using Microsoft.Its.Domain.Sql.Migrations;
using Newtonsoft.Json;

namespace Alluvial.For.ItsDomainSql
{
    public class AlluvialDistributorLeaseInitializer<T> : IDbMigrator
    {
        private readonly Leasable<T>[] leasables;
        private readonly string pool;

        public AlluvialDistributorLeaseInitializer(
            Leasable<T>[] leasables,
            string pool)
        {
            if (leasables == null)
            {
                throw new ArgumentNullException(nameof(leasables));
            }
            if (string.IsNullOrWhiteSpace(pool))
            {
                throw new ArgumentException("Pool cannot be null, empty, or consist entirely of whitespace.", nameof(pool));
            }
            this.leasables = leasables;
            this.pool = pool;
        }

        public MigrationResult Migrate(DbContext context)
        {

            Task.Run(async () =>
            {
                var dbConnection = context.Database.Connection;

                await SqlBrokeredDistributorDatabase.InitializeSchema(dbConnection);
                await SqlBrokeredDistributorDatabase.RegisterLeasableResources(
                    leasables, 
                    pool, 
                    dbConnection);
            })
                .Wait();

            return new MigrationResult
            {
                MigrationWasApplied = true,
                Log = $"Initialized leases in pool {pool}:\n\n" + JsonConvert.SerializeObject(leasables)
            };
        }

        public string MigrationScope => $"Leases: {pool}";

        public Version MigrationVersion => new Version("1.0.0");
    }
}