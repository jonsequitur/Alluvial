using System;
using System.Collections.Generic;
using System.Diagnostics;
using FluentAssertions;
using System.Linq;
using System.Threading.Tasks;
using Alluvial.Tests.BankDomain;
using NUnit.Framework;

namespace Alluvial.Tests
{
    [TestFixture]
    public class TracingTests
    {
        private TraceListener traceListener;

        [SetUp]
        public void SetUp()
        {
            traceListener = new TraceListener();
            Trace.Listeners.Add(traceListener);
        }

        [TearDown]
        public void TearDown()
        {
            Trace.Listeners.Remove(traceListener);
        }

        [Test]
        public async Task By_default_Aggregator_Trace_writes_projections_and_batches_to_trace_output()
        {
            var aggregagator = Aggregator.Create<Projection<int, int>, string>((p, es) =>
            {
                p.Value += es.Count;
                return p;
            }).Trace();

            await aggregagator.Aggregate(new Projection<int, int>
            {
                Value = 1
            }, StreamBatch.Create(new[] { "hi", "there" }, Cursor.New(0)));

            traceListener.Messages
                         .Should()
                         .Contain("[Aggregate] Projection(Int32,Int32): 1 @ cursor 0 / batch of 2 starts @ 0");
        }

        [Test]
        public async Task By_default_Aggregator_Trace_writes_exceptions_to_trace_output()
        {
            var aggregagator = Aggregator.Create<Projection<int, int>, string>((p, es) =>
            {
                throw new Exception("OUCH!");
                return p;
            }).Trace();

            try
            {
                await aggregagator.Aggregate(new Projection<int, int>
                {
                    Value = 1
                }, StreamBatch.Create(new[] { "hi", "there" }, Cursor.New(0)));
            }
            catch (Exception)
            {
            }

            traceListener.Messages
                         .Should()
                         .Contain(m => m.Contains("[Aggregate] Exception:"))
                         .And
                         .Contain(m => m.Contains("OUCH!"));
        }

        [Test]
        public async Task By_default_Stream_Trace_writes_events_that_are_read_from_the_stream_to_trace_output()
        {
            var stream = Stream.Create<int>(q => Enumerable.Range(1, 100)
                                                           .Skip(q.Cursor.Position)
                                                           .Take(q.BatchCount ?? 100000))
                               .Trace();

            var iterator = stream.CreateQuery(Cursor.New(15), 10);

            await iterator.NextBatch();

            traceListener.Messages
                         .ShouldBeEquivalentTo(new[]
                         {
                             string.Format("[Query] stream {0} @ cursor 15", stream.Id),
                             string.Format("      [Fetched] stream {0} batch of 10, now @ cursor 25", stream.Id)
                         });
        }

        [Test]
        public async Task By_default_ProjectionStore_Trace_writes_projections_during_Put_to_trace_output()
        {
            var store = new InMemoryProjectionStore<BalanceProjection>();

            await store.Trace().Put("the-stream-id", new BalanceProjection());

            traceListener.Messages
                         .Should()
                         .Contain("[Put] Projection(IDomainEvent,Int32): null @ cursor 0 for stream the-stream-id");
        }

        [Test]
        public async Task By_default_ProjectionStore_Trace_writes_projections_during_Get_to_trace_output()
        {
            var store = new InMemoryProjectionStore<BalanceProjection>();
            await store.Put("the-stream-id", new BalanceProjection());

            await store.Trace().Get("the-stream-id");

            traceListener.Messages
                         .Should()
                         .Contain("[Get] Projection(IDomainEvent,Int32): null @ cursor 0 for stream the-stream-id");
        }

        [Test]
        public async Task By_default_ProjectionStore_Trace_writes_projection_store_misses_during_Get_to_trace_output()
        {
            var store = ProjectionStore.Create<string, BalanceProjection>(
                get: async key => null,
                put: async (key, p) => { });

            await store.Trace().Get("the-stream-id");

            traceListener.Messages
                         .Should()
                         .Contain("[Get] no projection for stream the-stream-id");
        }

        private class TraceListener : System.Diagnostics.TraceListener
        {
            private readonly List<string> messages = new List<string>();

            public override void Write(string message)
            {
                WriteLine(message);
            }

            public override void WriteLine(string message)
            {
                messages.Add(message);
            }

            public List<string> Messages
            {
                get
                {
                    return messages;
                }
            }
        }
    }
}