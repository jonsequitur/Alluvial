using System;
using System.Collections.Generic;
using System.Diagnostics;
using FluentAssertions;
using System.Linq;
using System.Threading.Tasks;
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

        [Test]
        public async Task Aggregator_Trace_writes_projections_and_batches_to_trace_output_by_default()
        {
            var aggregagator = Aggregator.Create<int, string>((p, es) =>
            {
                p += es.Count;
                return p;
            }).Trace();

            await aggregagator.Aggregate(1, StreamBatch.Create(new[] { "hi", "there" }, Cursor.New()));

            traceListener.Messages.Should().Contain("Projection 1 / batch of 2 starts @ 0");
        }

        [Test]
        public async Task Stream_Trace_writes_events_that_are_read_from_the_stream()
        {
            var stream = Stream.Create(q => Enumerable.Range(1, 100)
                                                      .Skip(q.Cursor.As<int>())
                                                      .Take(q.BatchCount ?? int.MaxValue))
                               .Trace();

            var iterator = stream.CreateQuery(Cursor.Create(15), 10);

            await iterator.NextBatch();

            traceListener.Messages.ShouldBeEquivalentTo(new[]
            {
                string.Format("Query: stream {0} @ cursor position 15", stream.Id),
                string.Format("Fetched: stream {0} batch of 10, now @ cursor position 25", stream.Id)
            });

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