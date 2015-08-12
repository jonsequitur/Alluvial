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
        [Test]
        public async Task WithinPartition_by_range_correctly_partitions_queries_by_guid()
        {
            var id = Guid.NewGuid().ToString();

            await WriteEvents(i => new Event
            {
                SequenceNumber = i,
                Id = id
            });

            var stream = Stream.PartitionedByRange<Event, int, Guid>(async (q, p) =>
            {
                using (var db = new AlluvialSqlTestsDbContext())
                {
                    return await db.Events
                                   .Where(e => e.Id == id)
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

        [Test]
        public async Task WithinPartition_by_range_correctly_partitions_queries_by_int()
        {
            var id = Guid.NewGuid().ToString();

            await WriteEvents(i => new Event
            {
                SequenceNumber = i,
                Id = id
            });

            var stream = Stream.PartitionedByRange<Event, int, int>(async (q, p) =>
            {
                using (var db = new AlluvialSqlTestsDbContext())
                {
                    return await db.Events
                                   .Where(e => e.Id == id)
                                   .WithinPartition(e => e.SequenceNumber, p)
                                   .ToArrayAsync();
                }
            });

            var catchup = stream.DistributeAmong(Partition.ByRange(0, 100).Among(5));

            var store = new InMemoryProjectionStore<int>();

            catchup.Subscribe(async (count, events) => events.Count, store);

            await catchup.RunSingleBatch();

            store.Select(x => x).Should().BeEquivalentTo(new[] { 20, 20, 20, 20, 20 });
        }

        [Test]
        public async Task WithinPartition_by_range_correctly_partitions_queries_by_string()
        {
            var guid = Guid.NewGuid();

            var partitions = new[]
            {
                Partition.ByRange("", "mm"),
                Partition.ByRange("mm", "zz")
            };

            Values.AtoZ()
                  .ToList()
                  .ForEach(c =>
                               WriteEvents(i => new Event
                               {
                                   SequenceNumber = i,
                                   Guid = guid,
                                   Id = c + "  " + Guid.NewGuid()
                               }, 10).Wait());

            var stream = Stream.PartitionedByRange<Event, int, string>(async (q, p) =>
            {
                using (var db = new AlluvialSqlTestsDbContext())
                {
                    return await db.Events
                                   .Where(e => e.Guid == guid)
                                   .WithinPartition(e => e.Id, p)
                                   .ToArrayAsync();
                }
            });

            var catchup = stream.DistributeAmong(partitions);

            var store = new InMemoryProjectionStore<int>();

            catchup.Subscribe(async (count, events) => events.Count, store);

            await catchup.RunSingleBatch();

            store.Select(x => x).Should().BeEquivalentTo(new[] { 130, 130 });
        }

        private static async Task WriteEvents(Func<int, Event> createEvent, int howMany = 100)
        {
            using (var db = new AlluvialSqlTestsDbContext())
            {
                for (var i = 1; i <= howMany; i++)
                {
                    db.Events.Add(createEvent(i));
                }
                await db.SaveChangesAsync();
            }
        }
    }
}