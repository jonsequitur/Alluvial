using System;
using FluentAssertions;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Its.Domain;
using Microsoft.Its.Domain.Sql;
using NUnit.Framework;

namespace Alluvial.ForItsCqrs.Tests
{
    [TestFixture]
    public class EventStreamTests
    {
        [Test]
        public async Task AllChanges_doesnt_miss_aggregates()
        {
            var aggregateId1 = Guid.NewGuid();
            var aggregateId2 = Guid.NewGuid();
            var aggregateId3 = Guid.NewGuid();
            var aggregateId4 = Guid.NewGuid();

            var storableEvents = CreateStorableEvents(aggregateId1, aggregateId2, aggregateId3, aggregateId4).AsQueryable();
            var allChanges = EventStream.PerAggregate("Snarf", () => storableEvents);

            var aggregator = Aggregator.Create<int, IEvent>((oldCount, batch) =>
            {
                var eventType14s = batch.OfType<AggregateB.EventType14>();
                var newCount = eventType14s.Count();
                return oldCount + newCount;
            }).Trace();

            var catchup = StreamCatchup.All(allChanges);

            var count = 0;
            var store = ProjectionStore.Create<string, int>(async _ => count, async (_, newCount) => count = newCount);
            catchup.Subscribe(aggregator, store);
            catchup.RunUntilCaughtUp().Wait();
            count.Should().Be(2);
        }

        private static StorableEvent[] CreateStorableEvents(
            Guid aggregateId1,
            Guid aggregateId2,
            Guid aggregateId3,
            Guid aggregateId4)
        {
            return new[]
            {
                new StorableEvent
                {
                    StreamName = typeof (AggregateA).Name,
                    Type = typeof (AggregateA.EventType1).Name,
                    AggregateId = aggregateId1,
                    SequenceNumber = 1,
                    Id = 7,
                    Body = "{}"
                },
                new StorableEvent
                {
                    StreamName = typeof (AggregateA).Name,
                    Type = typeof (AggregateA.EventType2).Name,
                    AggregateId = aggregateId1,
                    SequenceNumber = 2,
                    Id = 8,
                    Body = "{}"
                },
                new StorableEvent
                {
                    StreamName = typeof (AggregateA).Name,
                    Type = typeof (AggregateA.EventType3).Name,
                    AggregateId = aggregateId1,
                    SequenceNumber = 3,
                    Id = 9,
                    Body = "{}"
                },
                new StorableEvent
                {
                    StreamName = typeof (AggregateA).Name,
                    Type = typeof (AggregateA.EventType4).Name,
                    AggregateId = aggregateId1,
                    SequenceNumber = 4,
                    Id = 10,
                    Body = "{}"
                },
                new StorableEvent
                {
                    StreamName = typeof (AggregateA).Name,
                    Type = typeof (AggregateA.EventType5).Name,
                    AggregateId = aggregateId1,
                    SequenceNumber = 5,
                    Id = 11,
                    Body = "{}"
                },
                new StorableEvent
                {
                    StreamName = typeof (AggregateA).Name,
                    Type = typeof (AggregateA.EventType6).Name,
                    AggregateId = aggregateId1,
                    SequenceNumber = 6,
                    Id = 12,
                    Body = "{}"
                },
                new StorableEvent
                {
                    StreamName = typeof (AggregateA).Name,
                    Type = typeof (AggregateA.EventType7).Name,
                    AggregateId = aggregateId1,
                    SequenceNumber = 7,
                    Id = 13,
                    Body = "{}"
                },
                new StorableEvent
                {
                    StreamName = typeof (AggregateA).Name,
                    Type = typeof (AggregateA.EventType8).Name,
                    AggregateId = aggregateId1,
                    SequenceNumber = 8,
                    Id = 14,
                    Body = "{}"
                },
                new StorableEvent
                {
                    StreamName = typeof (AggregateA).Name,
                    Type = typeof (AggregateA.EventType6).Name,
                    AggregateId = aggregateId1,
                    SequenceNumber = 9,
                    Id = 15,
                    Body = "{}"
                },
                new StorableEvent
                {
                    StreamName = typeof (AggregateA).Name,
                    Type = typeof (AggregateA.EventType7).Name,
                    AggregateId = aggregateId1,
                    SequenceNumber = 10,
                    Id = 16,
                    Body = "{}"
                },
                new StorableEvent
                {
                    StreamName = typeof (AggregateA).Name,
                    Type = typeof (AggregateA.EventType8).Name,
                    AggregateId = aggregateId1,
                    SequenceNumber = 11,
                    Id = 18,
                    Body = "{}"
                },
                new StorableEvent
                {
                    StreamName = typeof (AggregateA).Name,
                    Type = typeof (AggregateA.EventType6).Name,
                    AggregateId = aggregateId1,
                    SequenceNumber = 12,
                    Id = 19,
                    Body = "{}"
                },
                new StorableEvent
                {
                    StreamName = typeof (AggregateA).Name,
                    Type = typeof (AggregateA.EventType7).Name,
                    AggregateId = aggregateId1,
                    SequenceNumber = 13,
                    Id = 20,
                    Body = "{}"
                },
                new StorableEvent
                {
                    StreamName = typeof (AggregateA).Name,
                    Type = typeof (AggregateA.EventType8).Name,
                    AggregateId = aggregateId1,
                    SequenceNumber = 14,
                    Id = 21,
                    Body = "{}"
                },
                new StorableEvent
                {
                    StreamName = typeof (AggregateA).Name,
                    Type = typeof (AggregateA.EventType6).Name,
                    AggregateId = aggregateId1,
                    SequenceNumber = 15,
                    Id = 22,
                    Body = "{}"
                },
                new StorableEvent
                {
                    StreamName = typeof (AggregateA).Name,
                    Type = typeof (AggregateA.EventType7).Name,
                    AggregateId = aggregateId1,
                    SequenceNumber = 16,
                    Id = 23,
                    Body = "{}"
                },
                new StorableEvent
                {
                    StreamName = typeof (AggregateB).Name,
                    Type = typeof (AggregateB.EventType1).Name,
                    AggregateId = aggregateId2,
                    SequenceNumber = 1,
                    Id = 24,
                    Body = "{}"
                },
                new StorableEvent
                {
                    StreamName = typeof (AggregateB).Name,
                    Type = typeof (AggregateB.EventType9).Name,
                    AggregateId = aggregateId2,
                    SequenceNumber = 2,
                    Id = 25,
                    Body = "{}"
                },
                new StorableEvent
                {
                    StreamName = typeof (AggregateB).Name,
                    Type = typeof (AggregateB.EventType10).Name,
                    AggregateId = aggregateId2,
                    SequenceNumber = 3,
                    Id = 26,
                    Body = "{}"
                },
                new StorableEvent
                {
                    StreamName = typeof (AggregateB).Name,
                    Type = typeof (AggregateB.EventType11).Name,
                    AggregateId = aggregateId2,
                    SequenceNumber = 4,
                    Id = 27,
                    Body = "{}"
                },
                new StorableEvent
                {
                    StreamName = typeof (AggregateB).Name,
                    Type = typeof (AggregateB.EventType12).Name,
                    AggregateId = aggregateId2,
                    SequenceNumber = 5,
                    Id = 28,
                    Body = "{}"
                },
                new StorableEvent
                {
                    StreamName = typeof (AggregateB).Name,
                    Type = typeof (AggregateB.EventType13).Name,
                    AggregateId = aggregateId2,
                    SequenceNumber = 6,
                    Id = 29,
                    Body = "{}"
                },
                new StorableEvent
                {
                    StreamName = typeof (AggregateB).Name,
                    Type = typeof (AggregateB.EventType14).Name,
                    AggregateId = aggregateId2,
                    SequenceNumber = 7,
                    Id = 40,
                    Body = "{}"
                },
                new StorableEvent
                {
                    StreamName = typeof (AggregateB).Name,
                    Type = typeof (AggregateB.EventType1).Name,
                    AggregateId = aggregateId3,
                    SequenceNumber = 1,
                    Id = 1,
                    Body = "{}"
                },
                new StorableEvent
                {
                    StreamName = typeof (AggregateB).Name,
                    Type = typeof (AggregateB.EventType9).Name,
                    AggregateId = aggregateId3,
                    SequenceNumber = 2,
                    Id = 2,
                    Body = "{}"
                },
                new StorableEvent
                {
                    StreamName = typeof (AggregateB).Name,
                    Type = typeof (AggregateB.EventType10).Name,
                    AggregateId = aggregateId3,
                    SequenceNumber = 3,
                    Id = 3,
                    Body = "{}"
                },
                new StorableEvent
                {
                    StreamName = typeof (AggregateB).Name,
                    Type = typeof (AggregateB.EventType11).Name,
                    AggregateId = aggregateId3,
                    SequenceNumber = 4,
                    Id = 4,
                    Body = "{}"
                },
                new StorableEvent
                {
                    StreamName = typeof (AggregateB).Name,
                    Type = typeof (AggregateB.EventType12).Name,
                    AggregateId = aggregateId3,
                    SequenceNumber = 5,
                    Id = 5,
                    Body = "{}"
                },
                new StorableEvent
                {
                    StreamName = typeof (AggregateB).Name,
                    Type = typeof (AggregateB.EventType13).Name,
                    AggregateId = aggregateId3,
                    SequenceNumber = 6,
                    Id = 6,
                    Body = "{}"
                },
                new StorableEvent
                {
                    StreamName = typeof (AggregateB).Name,
                    Type = typeof (AggregateB.EventType14).Name,
                    AggregateId = aggregateId3,
                    SequenceNumber = 7,
                    Id = 17,
                    Body = "{}"
                },
                new StorableEvent
                {
                    StreamName = typeof (AggregateA).Name,
                    Type = typeof (AggregateA.EventType1).Name,
                    AggregateId = aggregateId4,
                    SequenceNumber = 1,
                    Id = 30,
                    Body = "{}"
                },
                new StorableEvent
                {
                    StreamName = typeof (AggregateA).Name,
                    Type = typeof (AggregateA.EventType2).Name,
                    AggregateId = aggregateId4,
                    SequenceNumber = 2,
                    Id = 31,
                    Body = "{}"
                },
                new StorableEvent
                {
                    StreamName = typeof (AggregateA).Name,
                    Type = typeof (AggregateA.EventType3).Name,
                    AggregateId = aggregateId4,
                    SequenceNumber = 3,
                    Id = 32,
                    Body = "{}"
                },
                new StorableEvent
                {
                    StreamName = typeof (AggregateA).Name,
                    Type = typeof (AggregateA.EventType4).Name,
                    AggregateId = aggregateId4,
                    SequenceNumber = 4,
                    Id = 33,
                    Body = "{}"
                },
                new StorableEvent
                {
                    StreamName = typeof (AggregateA).Name,
                    Type = typeof (AggregateA.EventType5).Name,
                    AggregateId = aggregateId4,
                    SequenceNumber = 5,
                    Id = 34,
                    Body = "{}"
                },
                new StorableEvent
                {
                    StreamName = typeof (AggregateA).Name,
                    Type = typeof (AggregateA.EventType6).Name,
                    AggregateId = aggregateId4,
                    SequenceNumber = 6,
                    Id = 35,
                    Body = "{}"
                },
                new StorableEvent
                {
                    StreamName = typeof (AggregateA).Name,
                    Type = typeof (AggregateA.EventType7).Name,
                    AggregateId = aggregateId4,
                    SequenceNumber = 7,
                    Id = 36,
                    Body = "{}"
                },
                new StorableEvent
                {
                    StreamName = typeof (AggregateA).Name,
                    Type = typeof (AggregateA.EventType8).Name,
                    AggregateId = aggregateId4,
                    SequenceNumber = 8,
                    Id = 37,
                    Body = "{}"
                },
                new StorableEvent
                {
                    StreamName = typeof (AggregateA).Name,
                    Type = typeof (AggregateA.EventType6).Name,
                    AggregateId = aggregateId4,
                    SequenceNumber = 9,
                    Id = 38,
                    Body = "{}"
                },
                new StorableEvent
                {
                    StreamName = typeof (AggregateA).Name,
                    Type = typeof (AggregateA.EventType7).Name,
                    AggregateId = aggregateId4,
                    SequenceNumber = 10,
                    Id = 39,
                    Body = "{}"
                },
                new StorableEvent
                {
                    StreamName = typeof (AggregateA).Name,
                    Type = typeof (AggregateA.EventType8).Name,
                    AggregateId = aggregateId4,
                    SequenceNumber = 11,
                    Id = 41,
                    Body = "{}"
                },
                new StorableEvent
                {
                    StreamName = typeof (AggregateA).Name,
                    Type = typeof (AggregateA.EventType6).Name,
                    AggregateId = aggregateId4,
                    SequenceNumber = 12,
                    Id = 42,
                    Body = "{}"
                },
                new StorableEvent
                {
                    StreamName = typeof (AggregateA).Name,
                    Type = typeof (AggregateA.EventType7).Name,
                    AggregateId = aggregateId4,
                    SequenceNumber = 13,
                    Id = 43,
                    Body = "{}"
                },
                new StorableEvent
                {
                    StreamName = typeof (AggregateA).Name,
                    Type = typeof (AggregateA.EventType8).Name,
                    AggregateId = aggregateId4,
                    SequenceNumber = 14,
                    Id = 44,
                    Body = "{}"
                },
                new StorableEvent
                {
                    StreamName = typeof (AggregateA).Name,
                    Type = typeof (AggregateA.EventType6).Name,
                    AggregateId = aggregateId4,
                    SequenceNumber = 15,
                    Id = 45,
                    Body = "{}"
                },
                new StorableEvent
                {
                    StreamName = typeof (AggregateA).Name,
                    Type = typeof (AggregateA.EventType7).Name,
                    AggregateId = aggregateId4,
                    SequenceNumber = 16,
                    Id = 46,
                    Body = "{}"
                },
            };
        }
    }
}