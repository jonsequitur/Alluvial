using Microsoft.Its.Domain.Sql;
using Microsoft.Its.Domain.Sql.CommandScheduler;

namespace Alluvial.ForItsCqrs.Tests
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

                databasesInitialized = true;
            }
        }

        private static void SetConnectionStrings()
        {
            // local
            EventStoreDbContext.NameOrConnectionString =
                @"Data Source=(localdb)\v11.0; Integrated Security=True; MultipleActiveResultSets=False; Initial Catalog=BankingEventStore";

            CommandSchedulerDbContext.NameOrConnectionString =
                @"Data Source=(localdb)\v11.0; Integrated Security=True; MultipleActiveResultSets=False; Initial Catalog=BankingScheduledCommands";
        }
    }
}