using System;
using System.Data.SqlClient;
using Alluvial.Distributors.Sql;
using FluentAssertions;
using System.Linq;
using System.Threading.Tasks;
using Alluvial.Tests;
using Microsoft.Its.Domain;
using Microsoft.Its.Domain.Serialization;
using Microsoft.Its.Domain.Sql;
using NUnit.Framework;

namespace Alluvial.For.ItsDomainSql.Tests
{
    [TestFixture]
    public class PerformanceTests
    {
        [SetUp]
        public void SetUp()
        {
            DatabaseSetup.Run();
        }

        [Test]
        [Explicit]
        public async Task Write_events()
        {
            await WritePerfTestEvents();
        }

        [Test]
        [Explicit]
        public async Task EventStream_PerAggregatePartitioned_doesnt_miss_aggregates_when_connections_are_starved()
        {
            var readModelConnectionString = new SqlConnectionStringBuilder(AlluvialSqlTestsDbContext.NameOrConnectionString)
            {
                Pooling = true,
                MaxPoolSize = 128
            }.ConnectionString;

            var stream = EventStream.PerAggregatePartitioned(
                "count-per-aggregate",
                () =>
                {
                    var eventStore = new EventStoreDbContext();
                    return eventStore.Events.Where(e => e.StreamName == "perf-test");
                })
                                    .Trace();

            var sqlBrokeredDistributorDatabase = new SqlBrokeredDistributorDatabase(readModelConnectionString);
            await sqlBrokeredDistributorDatabase.InitializeSchema();

            var distributor = Partition.AllGuids().Among(128)
                                       .CreateSqlBrokeredDistributor(
                                           sqlBrokeredDistributorDatabase,
                                           pool: "perf",
                                           maxDegreesOfParallelism: 128)
                                       .Trace();

            var catchup = stream.CreateDistributedCatchup(distributor, batchSize: 50);

            var aggregator = Aggregator.Create<ProjectionModel, IEvent>((projection, batch) =>
            {
                var count = projection.Body?.FromJsonTo<int>() ?? 0;
                count += batch.Count;
                projection.Body = count.ToJson();
            });

            catchup.Subscribe(aggregator,
                              SqlStorageFor.Projection(
                                  async (db, projectionId) =>
                                  db.Projections.SingleOrDefault(p => p.Id == projectionId),
                                  createNew: id => new ProjectionModel { Id = id },
                                  createDbContext: () => new AlluvialSqlTestsDbContext(readModelConnectionString)),
                              onError: Console.WriteLine);

            using (catchup)
            {
                await catchup.Start();
                await Wait.Until(async () =>
                {
                    using (var eventStore = new EventStoreDbContext())
                    using (var db = new AlluvialSqlTestsDbContext())
                    {
                        var aggregateCount = await eventStore.Database.SqlQuery<int>(
                            @"SELECT count(distinct aggregateid) FROM [eventstore].[Events] where StreamName = 'perf-test'")
                                                             .SingleAsync();

                        var projectionCount = await db.Database.SqlQuery<int>(
                            @"SELECT count(*) FROM [AlluvialSqlTests].[dbo].[ProjectionModels]")
                                                      .SingleAsync();

                        Console.WriteLine($"aggregates: {aggregateCount}    projections: {projectionCount}");

                        return projectionCount == aggregateCount;
                    }
                }, pollInterval: 1.Seconds(), timeout: 2.Minutes());
            }
        }

        private static async Task WritePerfTestEvents()
        {
            var aggregateIds = Enumerable.Range(1, 100)
                                         .Select(_ => Guid.NewGuid())
                                         .ToArray();
            var storableEvents = Enumerable.Range(1, 100)
                                           .SelectMany(i => aggregateIds
                                                                .Select(aggregateId => new StorableEvent
                                                                {
                                                                    AggregateId = aggregateId,
                                                                    StreamName = "perf-test",
                                                                    Type = "event",
                                                                    SequenceNumber = i,
                                                                    Body = "{ }",
                                                                    Timestamp = DateTimeOffset.Now
                                                                }))
                                           .ToArray();

            using (var eventStore = new EventStoreDbContext())
            {
                foreach (var storableEvent in storableEvents)
                {
                    eventStore.Events.Add(storableEvent);
                }

                await eventStore.SaveChangesAsync();
            }

            Console.WriteLine("events written: " + storableEvents.Length);
        }
    }
}