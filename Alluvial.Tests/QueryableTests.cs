using System;
using System.Data.Entity;
using FluentAssertions;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;

namespace Alluvial.Tests
{
    [TestFixture]
    public class QueryableTests
    {
        [SetUp]
        public void SetUp()
        {
        }

        [Test]
        public async Task WithinPartition_by_range_correctly_partitions_queries_by_guid()
        {
            var id = Guid.NewGuid().ToString();

            using (var db = new AlluvialSqlTestsDbContext())
            {
                for (var i = 1; i <= 100; i++)
                {
                    db.Events.Add(new Event
                    {
                        SequenceNumber = i,
                        Id = id
                    });
                }
                await db.SaveChangesAsync();
            }

            var stream = Stream.PartitionedByRange<Event, int, Guid>(async (q, p) =>
            {
                using (var db = new AlluvialSqlTestsDbContext())
                {
                    return await db.Events.Where(e => e.Id == id)
                                   .WithinPartition(e => e.Guid, p)
                                   .ToArrayAsync();
                }
            });

            var catchup = stream.DistributeAmong(Partition.AllGuids().Among(20));

            var store = new InMemoryProjectionStore<int>();

            catchup.Subscribe(async (count, events) => events.Count, store);

            await catchup.RunSingleBatch();

            store.Sum(e => e).Should().Be(100);
        }
    }
}