using Microsoft.Its.Domain.Sql;
using Microsoft.Its.Domain.Sql.CommandScheduler;

namespace Alluvial.For.ItsDomainSql.Tests
{
    public static class DatabaseSetup
    {
        private static bool databasesInitialized;
        private static readonly object lockObj = new object();

        public static void Run()
        {
            SetConnectionStrings();

            lock (lockObj)
            {
                if (databasesInitialized)
                {
                    return;
                }

                using (var db = new CommandSchedulerDbContext())
                {
                    new CommandSchedulerDatabaseInitializer().InitializeDatabase(db);
                }
                databasesInitialized = true;
            }
        }

        private static void SetConnectionStrings()
        {
            // local
            EventStoreDbContext.NameOrConnectionString =
                @"Data Source=(localdb)\MSSQLLocalDB; Integrated Security=True; MultipleActiveResultSets=False; Initial Catalog=AlluvialEventStore";

            CommandSchedulerDbContext.NameOrConnectionString =
                @"Data Source=(localdb)\MSSQLLocalDB; Integrated Security=True; MultipleActiveResultSets=False; Initial Catalog=AlluvialScheduledCommands";
        }
    }
}