namespace Alluvial.For.ItsDomainSql.Tests
{
    public static class TestDatabases
    {
        public const string EventStoreConnectionString =
            @"Data Source=(localdb)\MSSQLLocalDB; Integrated Security=True; MultipleActiveResultSets=False; Initial Catalog=AlluvialEventStore";

        public const string CommandSchedulerConnectionString =
            @"Data Source=(localdb)\MSSQLLocalDB; Integrated Security=True; MultipleActiveResultSets=False; Initial Catalog=AlluvialScheduledCommands";
    }
}