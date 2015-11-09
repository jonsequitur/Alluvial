using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FluentAssertions;
using Microsoft.Its.Domain;
using Microsoft.Its.Domain.Sql;
using NUnit.Framework;

namespace Alluvial.ForItsCqrs.Tests
{
    partial class AggregateA : EventSourcedAggregate<AggregateA>
    {
        public AggregateA(Guid id, IEnumerable<IEvent> eventHistory)
            : base(id, eventHistory)
        {
            
        }
    }
    public partial class AggregateB : EventSourcedAggregate<AggregateB>
    {
        public AggregateB(Guid id, IEnumerable<IEvent> eventHistory)
            : base(id, eventHistory)
        {
            
        }
    }

    public partial class AggregateA { public class EventType1 : Event<AggregateA> {
        public override void Update(AggregateA aggregate) { }
     } }
    public partial class AggregateA { public class EventType2 : Event<AggregateA> {
        public override void Update(AggregateA aggregate) { }
     } }
    public partial class AggregateA { public class EventType3 : Event<AggregateA> {
        public override void Update(AggregateA aggregate) { }
     } }
    public partial class AggregateA { public class EventType4 : Event<AggregateA> {
        public override void Update(AggregateA aggregate) { }
     } }
    public partial class AggregateA { public class EventType5 : Event<AggregateA> {
        public override void Update(AggregateA aggregate) { }
     } }
    public partial class AggregateA { public class EventType6 : Event<AggregateA> {
        public override void Update(AggregateA aggregate) { }
     } }
    public partial class AggregateA { public class EventType7 : Event<AggregateA> {
        public override void Update(AggregateA aggregate) { }
     } }
    public partial class AggregateA { public class EventType8 : Event<AggregateA> {
        public override void Update(AggregateA aggregate) { }
     } }
    public partial class AggregateB { public class EventType1 : Event<AggregateB> {
        public override void Update(AggregateB aggregate) { }
     } }
    public partial class AggregateB { public class EventType9 : Event<AggregateB> {
        public override void Update(AggregateB aggregate) { }
     } }
    public partial class AggregateB { public class EventType10 : Event<AggregateB> {
        public override void Update(AggregateB aggregate) { }
     } }
    public partial class AggregateB { public class EventType11 : Event<AggregateB> {
        public override void Update(AggregateB aggregate) { }
     } }
    public partial class AggregateB { public class EventType12 : Event<AggregateB> {
        public override void Update(AggregateB aggregate) { }
     } }
    public partial class AggregateB { public class EventType13 : Event<AggregateB> {
        public override void Update(AggregateB aggregate) { }
     } }
    public partial class AggregateB { public class EventType14 : Event<AggregateB> {
        public override void Update(AggregateB aggregate) { }
     } }

        [TestFixture]
        public class EventStreamTests
        {
        [Test]
        public async Task AllChanges_doesnt_miss_aggregates()
        {
            var AggregateId1 = Guid.NewGuid();
            var AggregateId2 = Guid.NewGuid();
            var AggregateId3 = Guid.NewGuid();
            var AggregateId4 = Guid.NewGuid();

            var storableEvents = new[]
            {
    new StorableEvent { StreamName = typeof(AggregateA).Name, Type=typeof(AggregateA.EventType1).Name, AggregateId = AggregateId1, SequenceNumber=1	, Id = 7,	Body = "{}" },
    new StorableEvent { StreamName = typeof(AggregateA).Name, Type=typeof(AggregateA.EventType2).Name, AggregateId = AggregateId1, SequenceNumber=2	, Id = 8,	Body = "{}" },
    new StorableEvent { StreamName = typeof(AggregateA).Name, Type=typeof(AggregateA.EventType3).Name, AggregateId = AggregateId1, SequenceNumber=3	, Id = 9,	Body = "{}" },
    new StorableEvent { StreamName = typeof(AggregateA).Name, Type=typeof(AggregateA.EventType4).Name, AggregateId = AggregateId1, SequenceNumber=4	, Id = 10,	Body = "{}" },
    new StorableEvent { StreamName = typeof(AggregateA).Name, Type=typeof(AggregateA.EventType5).Name, AggregateId = AggregateId1, SequenceNumber=5	, Id = 11,	Body = "{}" },
    new StorableEvent { StreamName = typeof(AggregateA).Name, Type=typeof(AggregateA.EventType6).Name, AggregateId = AggregateId1, SequenceNumber=6	, Id = 12,	Body = "{}" },
    new StorableEvent { StreamName = typeof(AggregateA).Name, Type=typeof(AggregateA.EventType7).Name, AggregateId = AggregateId1, SequenceNumber=7	, Id = 13,	Body = "{}" },
    new StorableEvent { StreamName = typeof(AggregateA).Name, Type=typeof(AggregateA.EventType8).Name, AggregateId = AggregateId1, SequenceNumber=8	, Id = 14,	Body = "{}" },
    new StorableEvent { StreamName = typeof(AggregateA).Name, Type=typeof(AggregateA.EventType6).Name, AggregateId = AggregateId1, SequenceNumber=9	, Id = 15,	Body = "{}" },
    new StorableEvent { StreamName = typeof(AggregateA).Name, Type=typeof(AggregateA.EventType7).Name, AggregateId = AggregateId1, SequenceNumber=10, Id = 	16,	Body = "{}" },
    new StorableEvent { StreamName = typeof(AggregateA).Name, Type=typeof(AggregateA.EventType8).Name, AggregateId = AggregateId1, SequenceNumber=11, Id = 	18,	Body = "{}" },
    new StorableEvent { StreamName = typeof(AggregateA).Name, Type=typeof(AggregateA.EventType6).Name, AggregateId = AggregateId1, SequenceNumber=12, Id = 	19,	Body = "{}" },
    new StorableEvent { StreamName = typeof(AggregateA).Name, Type=typeof(AggregateA.EventType7).Name, AggregateId = AggregateId1, SequenceNumber=13, Id = 	20,	Body = "{}" },
    new StorableEvent { StreamName = typeof(AggregateA).Name, Type=typeof(AggregateA.EventType8).Name, AggregateId = AggregateId1, SequenceNumber=14, Id = 	21,	Body = "{}" },
    new StorableEvent { StreamName = typeof(AggregateA).Name, Type=typeof(AggregateA.EventType6).Name, AggregateId = AggregateId1, SequenceNumber=15, Id = 	22,	Body = "{}" },
    new StorableEvent { StreamName = typeof(AggregateA).Name, Type=typeof(AggregateA.EventType7).Name, AggregateId = AggregateId1, SequenceNumber=16, Id = 	23,	Body = "{}" },
    new StorableEvent { StreamName = typeof(AggregateB).Name, Type=typeof(AggregateB.EventType1).Name, AggregateId = AggregateId2, SequenceNumber=1	, Id = 24,	Body = "{}" },
    new StorableEvent { StreamName = typeof(AggregateB).Name, Type=typeof(AggregateB.EventType9).Name, AggregateId = AggregateId2, SequenceNumber=2	, Id = 25,	Body = "{}" },
    new StorableEvent { StreamName = typeof(AggregateB).Name, Type=typeof(AggregateB.EventType10).Name, AggregateId = AggregateId2, SequenceNumber=3	, Id = 26,	Body = "{}" },
    new StorableEvent { StreamName = typeof(AggregateB).Name, Type=typeof(AggregateB.EventType11).Name, AggregateId = AggregateId2, SequenceNumber=4	, Id = 27,	Body = "{}" },
    new StorableEvent { StreamName = typeof(AggregateB).Name, Type=typeof(AggregateB.EventType12).Name, AggregateId = AggregateId2, SequenceNumber=5	, Id = 28,	Body = "{}" },
    new StorableEvent { StreamName = typeof(AggregateB).Name, Type=typeof(AggregateB.EventType13).Name, AggregateId = AggregateId2, SequenceNumber=6	, Id = 29,	Body = "{}" },
    new StorableEvent { StreamName = typeof(AggregateB).Name, Type=typeof(AggregateB.EventType14).Name, AggregateId = AggregateId2, SequenceNumber=7	, Id = 40,	Body = "{}" },
    new StorableEvent { StreamName = typeof(AggregateB).Name, Type=typeof(AggregateB.EventType1).Name, AggregateId = AggregateId3, SequenceNumber=1	, Id = 1,	Body = "{}" },
    new StorableEvent { StreamName = typeof(AggregateB).Name, Type=typeof(AggregateB.EventType9).Name, AggregateId = AggregateId3, SequenceNumber=2	, Id = 2,	Body = "{}" },
    new StorableEvent { StreamName = typeof(AggregateB).Name, Type=typeof(AggregateB.EventType10).Name, AggregateId = AggregateId3, SequenceNumber=3	, Id = 3,	Body = "{}" },
    new StorableEvent { StreamName = typeof(AggregateB).Name, Type=typeof(AggregateB.EventType11).Name, AggregateId = AggregateId3, SequenceNumber=4	, Id = 4,	Body = "{}" },
    new StorableEvent { StreamName = typeof(AggregateB).Name, Type=typeof(AggregateB.EventType12).Name, AggregateId = AggregateId3, SequenceNumber=5	, Id = 5,	Body = "{}" },
    new StorableEvent { StreamName = typeof(AggregateB).Name, Type=typeof(AggregateB.EventType13).Name, AggregateId = AggregateId3, SequenceNumber=6	, Id = 6,	Body = "{}" },
    new StorableEvent { StreamName = typeof(AggregateB).Name, Type=typeof(AggregateB.EventType14).Name, AggregateId = AggregateId3, SequenceNumber=7	, Id = 17,	Body = "{}" },
    new StorableEvent { StreamName = typeof(AggregateA).Name, Type=typeof(AggregateA.EventType1).Name, AggregateId = AggregateId4, SequenceNumber=1	, Id = 30,	Body = "{}" },
    new StorableEvent { StreamName = typeof(AggregateA).Name, Type=typeof(AggregateA.EventType2).Name, AggregateId = AggregateId4, SequenceNumber=2	, Id = 31,	Body = "{}" },
    new StorableEvent { StreamName = typeof(AggregateA).Name, Type=typeof(AggregateA.EventType3).Name, AggregateId = AggregateId4, SequenceNumber=3	, Id = 32,	Body = "{}" },
    new StorableEvent { StreamName = typeof(AggregateA).Name, Type=typeof(AggregateA.EventType4).Name, AggregateId = AggregateId4, SequenceNumber=4	, Id = 33,	Body = "{}" },
    new StorableEvent { StreamName = typeof(AggregateA).Name, Type=typeof(AggregateA.EventType5).Name, AggregateId = AggregateId4, SequenceNumber=5	, Id = 34,	Body = "{}" },
    new StorableEvent { StreamName = typeof(AggregateA).Name, Type=typeof(AggregateA.EventType6).Name, AggregateId = AggregateId4, SequenceNumber=6	, Id = 35,	Body = "{}" },
    new StorableEvent { StreamName = typeof(AggregateA).Name, Type=typeof(AggregateA.EventType7).Name, AggregateId = AggregateId4, SequenceNumber=7	, Id = 36,	Body = "{}" },
    new StorableEvent { StreamName = typeof(AggregateA).Name, Type=typeof(AggregateA.EventType8).Name, AggregateId = AggregateId4, SequenceNumber=8	, Id = 37,	Body = "{}" },
    new StorableEvent { StreamName = typeof(AggregateA).Name, Type=typeof(AggregateA.EventType6).Name, AggregateId = AggregateId4, SequenceNumber=9	, Id = 38,	Body = "{}" },
    new StorableEvent { StreamName = typeof(AggregateA).Name, Type=typeof(AggregateA.EventType7).Name, AggregateId = AggregateId4, SequenceNumber=10, Id = 	39,	Body = "{}" },
    new StorableEvent { StreamName = typeof(AggregateA).Name, Type=typeof(AggregateA.EventType8).Name, AggregateId = AggregateId4, SequenceNumber=11, Id = 	41,	Body = "{}" },
    new StorableEvent { StreamName = typeof(AggregateA).Name, Type=typeof(AggregateA.EventType6).Name, AggregateId = AggregateId4, SequenceNumber=12, Id = 	42,	Body = "{}" },
    new StorableEvent { StreamName = typeof(AggregateA).Name, Type=typeof(AggregateA.EventType7).Name, AggregateId = AggregateId4, SequenceNumber=13, Id = 	43,	Body = "{}" },
    new StorableEvent { StreamName = typeof(AggregateA).Name, Type=typeof(AggregateA.EventType8).Name, AggregateId = AggregateId4, SequenceNumber=14, Id = 	44,	Body = "{}" },
    new StorableEvent { StreamName = typeof(AggregateA).Name, Type=typeof(AggregateA.EventType6).Name, AggregateId = AggregateId4, SequenceNumber=15, Id = 	45,	Body = "{}" },
    new StorableEvent { StreamName = typeof(AggregateA).Name, Type=typeof(AggregateA.EventType7).Name, AggregateId = AggregateId4, SequenceNumber=16, Id = 	46,	Body = "{}" },
            }.AsQueryable();


            {
                var allChanges = EventStream.PerAggregate("Snarf", () => new DisposableQueryable<StorableEvent>(
                    () => { }, storableEvents));

                var aggregator = Aggregator.Create<int, IEvent>((oldCount, batch) =>
                {
                    var eventType14s = batch.OfType<AggregateB.EventType14>();
                    var newCount = eventType14s.Count();
                    return oldCount + newCount;
                }).Trace();

                var catchup = StreamCatchup.All(allChanges);

                var count = 0;
                var store = ProjectionStore.Create<string, int>(async _ =>
                {
                    //await Task.Delay(TimeSpan.FromMilliseconds(1));
                    return count;
                }, async (_, newCount) => count = newCount);
                catchup.Subscribe(aggregator, store);
                catchup.RunUntilCaughtUp().Wait();
                count.Should().Be(2);
            }
        }

        [Test]
        public void Trivial()
        {
            var stream = EventStream.AllChanges("My Stream ID", 
                () => new DisposableQueryable<StorableEvent>(() => { }, 
                new[]
            {
                new StorableEvent {Id = 1}
            }.AsQueryable()));

            var results = stream.CreateQuery().NextBatch().Result;

            results.Should().HaveCount(1);
        }
    }
}