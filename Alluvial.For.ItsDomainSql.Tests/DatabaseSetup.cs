using Microsoft.Its.Domain.Sql;
using Microsoft.Its.Domain.Sql.CommandScheduler;

namespace Alluvial.For.ItsDomainSql.Tests
{
    public static class DatabaseSetup
    {
        public static void Run()
        {
            SetConnectionStrings();
        }

        private static void SetConnectionStrings()
        {
            EventStoreDbContext.NameOrConnectionString =
                @"Data Source=(localdb)\MSSQLLocalDB; Integrated Security=True; MultipleActiveResultSets=False; Initial Catalog=AlluvialEventStore";

            CommandSchedulerDbContext.NameOrConnectionString =
                @"Data Source=(localdb)\MSSQLLocalDB; Integrated Security=True; MultipleActiveResultSets=False; Initial Catalog=AlluvialScheduledCommands";
        }
    }
}