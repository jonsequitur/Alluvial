using System.Data.Entity;
using Microsoft.Its.Domain.Sql;
using Microsoft.Its.Domain.Sql.CommandScheduler;

namespace Alluvial.Streams.ItsDomainSql.Tests
{
    public static class DatabaseSetup
    {
        private static bool databasesInitialized;
        private static readonly object lockObj = new object();

        public static void Run()
        {
            SetConnectionStrings();

            Database.SetInitializer(new CreateAndMigrate<CommandSchedulerDbContext>());

            lock (lockObj)
            {
                if (databasesInitialized)
                {
                    return;
                }

                databasesInitialized = true;
            }
        }

        private static void SetConnectionStrings()
        {
            // local
            EventStoreDbContext.NameOrConnectionString =
                @"Data Source=(localdb)\MSSQLLocalDB; Integrated Security=True; MultipleActiveResultSets=False; Initial Catalog=BankingEventStore";

            CommandSchedulerDbContext.NameOrConnectionString =
                @"Data Source=(localdb)\MSSQLLocalDB; Integrated Security=True; MultipleActiveResultSets=False; Initial Catalog=BankingScheduledCommands";
        }
    }
}