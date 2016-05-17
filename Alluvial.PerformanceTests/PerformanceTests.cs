using System;
using System.Data.Entity;
using System.Data.SqlClient;
using Alluvial.Distributors.Sql;
using FluentAssertions;
using System.Linq;
using System.Threading.Tasks;
using Alluvial.For.ItsDomainSql;
using Alluvial.For.ItsDomainSql.Tests;
using Alluvial.Tests;
using Microsoft.Its.Domain;
using Microsoft.Its.Domain.Serialization;
using Microsoft.Its.Domain.Sql;
using NUnit.Framework;

namespace Alluvial.PerformanceTests
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
        public async Task ConnectionPool_128_Partitions_128_Parallelism_128()
        {
            await ConnectionPool_vs_partitions_vs_parallelism(128, 128, 128);
        }

        [Test]
        [Explicit]
        public async Task ConnectionPool_64_Partitions_64_Parallelism_64()
        {
            await ConnectionPool_vs_partitions_vs_parallelism(64, 64, 64);
        }

        [Test]
        [Explicit]
        public async Task ConnectionPool_1_Partitions_64_Parallelism_1()
        {
            await ConnectionPool_vs_partitions_vs_parallelism(1, 64, 1);
        }

        [Test]
        [Explicit]
        public async Task ConnectionPool_1_Partitions_64_Parallelism_4()
        {
            await ConnectionPool_vs_partitions_vs_parallelism(1, 64, 4);
        }

        [Test]
        [Explicit]
        public async Task ConnectionPool_4_Partitions_64_Parallelism_4()
        {
            await ConnectionPool_vs_partitions_vs_parallelism(4, 64, 4);
        }

        [Test]
        [Explicit]
        public async Task ConnectionPool_4_Partitions_64_Parallelism_8()
        {
            await ConnectionPool_vs_partitions_vs_parallelism(4, 64, 8);
        }

        [Test]
        [Explicit]
        public async Task ConnectionPool_2_Partitions_64_Parallelism_8()
        {
            await ConnectionPool_vs_partitions_vs_parallelism(2, 64, 8);
        }

        [Test]
        [Explicit]
        public async Task ConnectionPool_64_Partitions_64_Parallelism_8()
        {
            await ConnectionPool_vs_partitions_vs_parallelism(64, 64, 8);
        }

        public async Task ConnectionPool_vs_partitions_vs_parallelism(
            int maxPoolSize,
            int numberOfPartitions,
            int maxDegreesOfParallelism)
        {
            var pool = $"{maxPoolSize}-{numberOfPartitions}-{maxDegreesOfParallelism}";

            var readModelConnectionString = new SqlConnectionStringBuilder(AlluvialSqlTestsDbContext.NameOrConnectionString)
            {
                Pooling = true,
                MaxPoolSize = maxPoolSize
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

            var distributor = Partition.AllGuids().Among(numberOfPartitions)
                                       .CreateSqlBrokeredDistributor(
                                           sqlBrokeredDistributorDatabase,
                                           pool,
                                           maxDegreesOfParallelism)
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
                                  getSingle: async (db, projectionId) =>
                                  {
                                      return await db.Projections
                                                     .SingleOrDefaultAsync(p => p.Id == projectionId &&
                                                                                p.Pool == pool);
                                  },
                                  createNew: id => new ProjectionModel
                                  {
                                      Id = id,
                                      Pool = pool
                                  },
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
                            @"SELECT count(distinct aggregateid) 
                              FROM [eventstore].[Events] 
                              WHERE StreamName = 'perf-test'").SingleAsync();

                        var projectionCount = await db.Database.SqlQuery<int>(
                            $@"SELECT count(*) 
                              FROM [AlluvialSqlTests].[dbo].[ProjectionModels]
                              WHERE Pool = '{pool}'").SingleAsync();

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