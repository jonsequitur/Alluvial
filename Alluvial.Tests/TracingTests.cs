using System;
using System.Collections.Generic;
using System.Diagnostics;
using FluentAssertions;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Alluvial.Tests.BankDomain;
using Alluvial.Tests.Distributors;
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
        public async Task By_default_IStreamAggregator_Trace_writes_projections_and_batches_to_trace_output()
        {
            var aggregator = Aggregator.Create<Projection<int, int>, string>((p, es) =>
            {
                p.Value += es.Count;
                return p;
            }).Trace();

            await aggregator.Aggregate(new Projection<int, int>
            {
                Value = 1
            }, StreamBatch.Create(new[] { "hi", "there" }, Cursor.New(0)));

            traceListener.Messages
                         .Should()
                         .ContainSingle(m => m == "[Aggregate] Projection<Int32,Int32>: 1 @ cursor 0 / batch of 2 starts @ 0");
        }

        [Test]
        public async Task By_default_Aggregate_Trace_writes_projections_and_batches_to_trace_output()
        {
            Aggregate<Projection<int, int>, string> aggregator = new Aggregate<Projection<int, int>, string>((p, es) =>
            {
                p.Value += es.Count;
                return p;
            }).Trace();

            aggregator(new Projection<int, int>
            {
                Value = 1
            }, StreamBatch.Create(new[] { "hi", "there" }, Cursor.New(0)));

            traceListener.Messages
                         .Should()
                         .ContainSingle(m => m == "[Aggregate] Projection<Int32,Int32>: 1 @ cursor 0 / batch of 2 starts @ 0");
        }

        [Test]
        public async Task By_default_AggregateAsync_Trace_writes_projections_and_batches_to_trace_output()
        {
            AggregateAsync<Projection<int, int>, string> aggregator = new AggregateAsync<Projection<int, int>, string>(async (p, es) =>
            {
                p.Value += es.Count;
                return p;
            }).Trace();

            await aggregator(new Projection<int, int>
            {
                Value = 1
            }, StreamBatch.Create(new[] { "hi", "there" }, Cursor.New(0)));

            traceListener.Messages
                         .Should()
                         .ContainSingle(m => m == "[Aggregate] Projection<Int32,Int32>: 1 @ cursor 0 / batch of 2 starts @ 0");
        }

        [Test]
        public async Task By_default_Aggregator_Trace_writes_exceptions_to_trace_output()
        {
            var aggregagator = Aggregator.Create<Projection<int, int>, string>((p, es) =>
            {
                throw new Exception("OUCH!");
#pragma warning disable 162
                return p;
#pragma warning restore 162
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
                         .ContainSingle(m => m.Contains("[Aggregate] Exception:"))
                         .And
                         .ContainSingle(m => m.Contains("OUCH!"));
        }

        [Test]
        public async Task By_default_Stream_Trace_writes_events_that_are_read_from_the_stream_to_trace_output()
        {
            var stream = Stream.Create<int>(q => Enumerable.Range(1, 100)
                                                           .Skip(q.Cursor.Position)
                                                           .Take(q.BatchSize ?? 100000))
                               .Trace();

            var iterator = stream.CreateQuery(Cursor.New(15), 10);

            await iterator.NextBatch();

            traceListener.Messages
                         .ShouldBeEquivalentTo(new[]
                         {
                             $"[Query] stream {stream.Id} @ cursor 15",
                             $"      [Fetched] stream {stream.Id} batch of 10, now @ cursor 25"
                         });
        }

        [Test]
        public async Task By_default_ProjectionStore_Trace_writes_projections_during_Put_to_trace_output()
        {
            var store = new InMemoryProjectionStore<BalanceProjection>();

            await store.Trace().Put("the-stream-id", new BalanceProjection());

            traceListener.Messages
                         .Should()
                         .ContainSingle(m => m == "[Store.Put] Projection<String,Int32>: null @ cursor 0 for stream the-stream-id");
        }

        [Test]
        public async Task By_default_ProjectionStore_Trace_writes_projections_during_Get_to_trace_output()
        {
            var store = new InMemoryProjectionStore<BalanceProjection>();
            await store.Put("the-stream-id", new BalanceProjection());

            await store.Trace().Get("the-stream-id");

            traceListener.Messages
                         .Should()
                         .ContainSingle(m => m == "[Store.Get] Projection<String,Int32>: null @ cursor 0 for stream the-stream-id");
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
                         .ContainSingle(m => m == "[Store.Get] no projection for stream the-stream-id");
        }

        [Test]
        public async Task By_default_Distributor_Trace_writes_start_events_to_trace_output()
        {
            using (var distributor = CreateDistributor().Trace())
            {
                await distributor.Start();

                await Task.Delay(50);

                traceListener.Messages
                             .Should()
                             .ContainSingle(m => m == $"[Distribute] By_default_Distributor_Trace_writes_start_events_to_trace_output: Start");
            }
        }

        [Test]
        public async Task By_default_Distributor_Trace_writes_stop_events_to_trace_output()
        {
            using (var distributor = CreateDistributor().Trace())
            {
                await distributor.Start();

                await distributor.Stop();

                traceListener.Messages
                             .Should()
                             .ContainSingle(m => m == "[Distribute] By_default_Distributor_Trace_writes_stop_events_to_trace_output: Stop");
            }
        }

        [Test]
        public async Task By_default_Distributor_Trace_writes_onReceive_events_to_trace_output()
        {
            Lease<int> lease = null;

            using (var distributor = CreateDistributor(async l => lease = l).Trace())
            {
                await distributor.Distribute(1);

                traceListener.Messages
                             .Should()
                             .ContainSingle(m => m.Contains("[Distribute] By_default_Distributor_Trace_writes_onReceive_events_to_trace_output: OnReceive lease:1"));

                traceListener.Messages
                             .Should()
                             .ContainSingle(m => m.Contains("[Distribute] By_default_Distributor_Trace_writes_onReceive_events_to_trace_output: OnReceive (done) lease:1"));
            }
        }

        [Test]
        public async Task Distributor_Trace_writes_OnReceive_exceptions_to_trace_output()
        {
            var distributor = CreateDistributor().Trace();

            distributor.OnReceive(async lease =>
            {
                throw new Exception("oops!");
            });

            await distributor.Distribute(1);

            traceListener.Messages
                         .Should()
                         .ContainSingle(m => m.Contains("oops!"));
        }

        [Test]
        public async Task DistributorBase_exceptions_on_AcquireLease_can_be_observed()
        {
            Exception caughtException = null;

            var distributor = new TestDistributor<int>(
                Enumerable.Range(1, 10)
                          .Select(i => new Leasable<int>(i, i.ToString()))
                          .ToArray(),
                beforeAcquire: async () => { throw new Exception("dang!"); },
                maxDegreesOfParallelism: 1)
                .Trace(onException: (ex, lease) => caughtException = ex);

            await distributor.Start();
            await Task.Delay(100);
            await distributor.Stop();

            caughtException.Should().NotBeNull();
        }

        [Test]
        public async Task DistributorBase_exceptions_on_ReleaseLease_can_be_observed()
        {
            Exception caughtException = null;
            var distributor = new TestDistributor<int>(
                    Enumerable.Range(1, 10)
                              .Select(i => new Leasable<int>(i, i.ToString()))
                              .ToArray(),
                    beforeRelease: async lease =>
                    {
                        throw new Exception("dang!");
                    })
                .Trace(onException: (ex, lease) =>
                {
                    caughtException = ex;
                })
                .ReleaseLeasesWhenWorkIsDone();

            await distributor.Distribute(1);

            // exception propagation is async, so wait a moment before the assertion
            await Task.Delay(10);

            caughtException.Should().NotBeNull();
        }

        [Test]
        public async Task Distributor_Trace_writes_pool_name_on_all_trace_events()
        {
            var poolName = "this-is-the-pool";
            var distributor = CreateDistributor(pool: poolName).Trace().ReleaseLeasesWhenWorkIsDone();

            await distributor.Distribute(1);

            distributor.OnReceive(async lease => { throw new Exception("oops!"); });

            await distributor.Distribute(1);

            traceListener.Messages
                         .Should()
                         .OnlyContain(m => m.Contains(poolName));
        }

        [Test]
        public async Task Distributor_Trace_default_behavior_can_be_overridden()
        {
            Lease<int> leaseAcquired = null;
            Lease<int> leaseReleased = null;

            var distributor1 = new InMemoryDistributor<int>(new[]
            {
                new Leasable<int>(1, "1")
            }).Trace(
                onLeaseAcquired: l => { leaseAcquired = l; },
                onLeaseWorkDone: l => { leaseReleased = l; });

            distributor1.OnReceive((async _ => { }));
            var distributor = distributor1;

            await distributor.Distribute(1);

            leaseAcquired.Should().NotBeNull();
            leaseReleased.Should().NotBeNull();
        }

        [Test]
        public async Task Aggregator_Trace_default_behavior_can_be_overridden()
        {
            var receivedProjection = 0;
            IStreamBatch<int> receivedBatch = null;

            var aggregator = Aggregator.Create<int, int>((p, b) => { })
                                       .Trace((i, b) =>
                                       {
                                           receivedProjection = i;
                                           receivedBatch = b;
                                       });

            var sentBatch = StreamBatch.Create(Enumerable.Range(1, 10).ToArray(), Cursor.New<int>());
            await aggregator.Aggregate(41, sentBatch);

            receivedProjection.Should().Be(41);
            receivedBatch.Should().BeSameAs(sentBatch);
        }

        [Test]
        public async Task ProjectionStore_Trace_default_behavior_can_be_overridden()
        {
            var receivedGetKey = "";
            var receivedGetProjection = 0;
            var receivedPutKey = "";
            var receivedPutProjection = 0;

            var store = ProjectionStore
                .Create<string, int>(
                    get: async key => 41,
                    put: async (key, count) => { })
                .Trace(
                    get: (key, count) =>
                    {
                        receivedGetKey = key;
                        receivedGetProjection = count;
                    },
                    put: (key, count) =>
                    {
                        receivedPutKey = key;
                        receivedPutProjection = count;
                    }
                );

            await store.Get("any key");

            receivedGetKey.Should().Be("any key");
            receivedGetProjection.Should().Be(41);

            await store.Put("some other key", 57);

            receivedPutKey.Should().Be("some other key");
            receivedPutProjection.Should().Be(57);
        }

        [Test]
        public async Task Stream_Trace_default_behavior_can_be_overridden()
        {
            IStreamQuery<int> receivedSendQuery = null;
            IStreamQuery<int> receivedResultsQuery = null;
            IStreamBatch<int> receivedBatch = null;

            var stream = Enumerable.Range(1, 1000)
                                   .AsStream()
                                   .Trace(onSendQuery: q => { receivedSendQuery = q; },
                                          onResults: (q, b) =>
                                          {
                                              receivedResultsQuery = q;
                                              receivedBatch = b;
                                          });

            var sentQuery = stream.CreateQuery(Cursor.New(15), batchSize: 3);
            await sentQuery.NextBatch();

            receivedSendQuery.Should().BeSameAs(sentQuery);
            receivedResultsQuery.Should().BeSameAs(sentQuery);
            receivedBatch.Count().Should().Be(3);
            receivedBatch.Should().ContainInOrder(16, 17, 18);
        }

        private static IDistributor<int> CreateDistributor(
            Func<Lease<int>, Task> onReceive = null,
            [CallerMemberName] string pool = null,
            TimeSpan? defaultLeaseDuration = null)
        {
            var distributor = new InMemoryDistributor<int>(new[]
            {
                new Leasable<int>(1, "1")
            },
                pool: pool, 
                defaultLeaseDuration: defaultLeaseDuration);

            distributor.OnReceive(onReceive ?? (async _ => { }));

            return distributor;
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
                Console.WriteLine(message);
                messages.Add(message);
            }

            public IEnumerable<string> Messages => messages;
        }
    }
}