using System;
using System.Collections.Generic;
using FluentAssertions;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;

namespace Alluvial.Tests
{
    [TestFixture]
    public abstract class QueryableTests
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
                return Events()
                    .Where(e => e.Id == id)
                    .WithinPartition(e => e.Guid, p)
                    .ToArray();
            });

            var catchup = stream.CreateDistributedCatchup(
                Partition.AllGuids()
                         .Among(20)
                         .CreateInMemoryDistributor());

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
                return Events()
                    .Where(e => e.Id == id)
                    .WithinPartition(e => e.SequenceNumber, p)
                    .ToArray();
            });

            var distributor = Partition
                .ByRange(0, 100)
                .Among(5)
                .CreateInMemoryDistributor();

            var catchup = stream.CreateDistributedCatchup(distributor);

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
                return Events()
                    .Where(e => e.Guid == guid)
                    .WithinPartition(e => e.Id, p)
                    .ToArray();
            });

            var catchup = stream.CreateDistributedCatchup(partitions.CreateInMemoryDistributor());

            var store = new InMemoryProjectionStore<int>();

            catchup.Subscribe(async (count, events) => events.Count, store);

            await catchup.RunSingleBatch();

            store.Select(x => x).Should().BeEquivalentTo(new[] { 130, 130 });
        }

        protected abstract Task WriteEvents(Func<int, Event> createEvent, int howMany = 100);

        protected abstract IQueryable<Event> Events();
    }
}