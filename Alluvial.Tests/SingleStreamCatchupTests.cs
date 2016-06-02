using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using FluentAssertions;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
using Alluvial.Tests.BankDomain;
using Alluvial.Tests.StreamImplementations.NEventStore;
using NEventStore;
using NUnit.Framework;

namespace Alluvial.Tests
{
    [TestFixture]
    public class SingleStreamCatchupTests
    {
        private IStoreEvents store;
        private string streamId;
        private IStream<IDomainEvent, int> stream;

        [SetUp]
        public void SetUp()
        {
            // populate the event store
            store = TestEventStore.Create();

            streamId = Guid.NewGuid().ToString();

            store.WriteEvents(streamId);

            stream = NEventStoreStream.ByAggregate(store, streamId).DomainEvents();
        }

        [Test]
        public async Task Catchup_can_traverse_all_events()
        {
            var projectionStore = new InMemoryProjectionStore<BalanceProjection>();
            store.WriteEvents(streamId, 999);
            var catchup = StreamCatchup.Create(stream);
            catchup.Subscribe(new BalanceProjector(), projectionStore);
            await catchup.RunSingleBatch();

            projectionStore.Sum(b => b.Balance)
                           .Should()
                           .Be(1000);
            projectionStore.Count()
                           .Should()
                           .Be(1);
        }

        [Test]
        public async Task When_one_batch_is_running_a_second_call_to_RunSingleBatch_will_not_do_anything()
        {
            var projectionStore = new InMemoryProjectionStore<BalanceProjection>();

            var catchup = StreamCatchup.Create(stream, batchSize: 1);
            catchup.Subscribe(new BalanceProjector()
                                  .Pipeline(async (projection, batch, next) =>
                                  {
                                      await Task.Delay(500);
                                      await next(projection, batch);
                                  }), projectionStore);

            await Task.WhenAll(catchup.RunSingleBatch(), catchup.RunSingleBatch());

            projectionStore.Count()
                           .Should()
                           .Be(1);
        }

        [Test]
        public async Task When_one_batch_is_running_then_a_second_caller_to_RunSingleBatch_can_await_the_completion_of_the_same_batch()
        {
            var projectionStore = new InMemoryProjectionStore<BalanceProjection>();

            var cursor = Cursor.New<int>();
            var catchup = StreamCatchup.Create(stream, batchSize: 1, initialCursor: cursor);
            catchup.Subscribe(new BalanceProjector()
                                  .Pipeline(async (projection, batch, next) =>
                                  {
                                      await Task.Delay(500);
                                      await next(projection, batch);
                                  }), projectionStore);

            await Task.WhenAll(catchup.RunSingleBatch(),
                               catchup.RunSingleBatch());

            cursor.Position.Should().Be(1);
        }

        [Test]
        public async Task Catchup_batch_size_can_be_specified()
        {
            var projectionStore = new InMemoryProjectionStore<BalanceProjection>();
            store.WriteEvents(streamId, howMany: 50);
            var catchup = StreamCatchup.Create(stream, batchSize: 20);
            catchup.Subscribe(new BalanceProjector(), projectionStore);

            await catchup.RunSingleBatch();

            projectionStore.Single()
                           .Balance
                           .Should()
                           .Be(20);
        }

        [Test]
        public async Task Catchup_query_cursor_resumes_from_last_position()
        {
            var projectionStore = new InMemoryProjectionStore<BalanceProjection>();

            store.WriteEvents(streamId, howMany: 999);

            var catchup = StreamCatchup.Create(stream, batchSize: 500);
            catchup.Subscribe(new BalanceProjector(), projectionStore);

            await catchup.RunSingleBatch();

            projectionStore.Sum(b => b.Balance)
                           .Should()
                           .Be(500);

            await catchup.RunSingleBatch();

            projectionStore.Sum(b => b.Balance)
                           .Should()
                           .Be(1000);
        }

        [Test]
        public async Task An_initial_cursor_can_be_specified()
        {
            var projectionStore = new InMemoryProjectionStore<BalanceProjection>();

            store.WriteEvents(streamId, howMany: 999);

            var catchup = StreamCatchup.Create(stream,
                                               initialCursor: Cursor.New(800),
                                               batchSize: 1000);
            catchup.Subscribe(new BalanceProjector(), projectionStore);

            await catchup.RunSingleBatch();

            projectionStore.Sum(b => b.Balance)
                           .Should()
                           .Be(200);
        }

        [Test]
        public async Task Catchup_RunUntilCaughtUp_runs_until_the_stream_has_no_more_results()
        {
            var projectionStore = new InMemoryProjectionStore<BalanceProjection>();
            store.WriteEvents(streamId, howMany: 999);
            var catchup = StreamCatchup.Create(stream, batchSize: 10);
            catchup.Subscribe(new BalanceProjector(), projectionStore);

            TaskScheduler.UnobservedTaskException += (sender, args) => Console.WriteLine(args.Exception);

            await catchup.RunUntilCaughtUp();

            projectionStore.Single()
                           .Balance
                           .Should()
                           .Be(1000);
        }

        [Test]
        public async Task When_projections_are_cursors_then_catchup_does_not_replay_previously_seen_events()
        {
            var projectionStore = new InMemoryProjectionStore<BalanceProjection>();
            var eventsAggregated = new List<IDomainEvent>();
            var catchup = StreamCatchup.Create(stream, batchSize: 100);
            catchup.Subscribe(new BalanceProjector().Trace((p, es) => eventsAggregated.AddRange(es)), projectionStore);

            await catchup.RunUntilCaughtUp();

            store.WriteEvents(streamId, howMany: 100);

            await catchup.RunUntilCaughtUp();
            
            var balanceProjection = await projectionStore.Get(streamId);

            balanceProjection.Balance.Should().Be(101);
            balanceProjection.CursorPosition.Should().Be(101);
            eventsAggregated.Count.Should().Be(101);
        }

        [Test]
        public async Task Catchup_Poll_keeps_projections_updated_as_new_events_are_written()
        {
            var projectionStore = new InMemoryProjectionStore<BalanceProjection>();
            var stopwatch = new Stopwatch();
            stopwatch.Start();
            var catchup = StreamCatchup.Create(stream, batchSize: 50);
            catchup.Subscribe(new BalanceProjector().Trace(), projectionStore);

            using (catchup.Poll(TimeSpan.FromMilliseconds(10)))
            {
                using (Background.Loop(_ => store.WriteEvents(streamId, howMany: 2), .1))
                {
                    await Wait.Until(() =>
                    {
                        var sum = projectionStore.Sum(b => b.Balance);
                        Console.WriteLine($"sum is {sum} @ {stopwatch.ElapsedMilliseconds}ms");
                        return sum >= 500;
                    });
                }
            }
        }

        [Test]
        public void RunSingleBatch_throws_when_an_aggregator_throws_an_exception()
        {
            var projectionStore = new InMemoryProjectionStore<BalanceProjection>();
            store.WriteEvents(streamId, 100);

            var projector = new BalanceProjector()
                .Pipeline(async (projection, batch, next) =>
                {
                    Throw();
                    await next(projection, batch);
                });

            var catchup = StreamCatchup.Create(stream, batchSize: 50);
            catchup.Subscribe(projector, projectionStore);

            Action runSingleBatch = () => catchup.RunSingleBatch().Wait();

            runSingleBatch.ShouldThrow<Exception>()
                          .And
                          .Message
                          .Should()
                          .Contain("oops");
        }

        [Test]
        public async Task OnError_Continue_prevents_aggregator_exceptions_from_stopping_catchup()
        {
            var projectionStore = new InMemoryProjectionStore<BalanceProjection>();
            var count = 0;
        
            var projector = new BalanceProjector()
                .Pipeline(async (projection, batch, next) =>
                {
                    Interlocked.Increment(ref count);
                    Console.WriteLine(count);
                    if (count < 49)
                    {
                        Throw();
                    }
                    await next(projection, batch);
                }).Trace();

            var catchup = StreamCatchup.Create(stream.Trace(),
                                               batchSize: 50);

            catchup.Subscribe(projector, 
                projectionStore.AsHandler(), 
                onError: e => e.Continue());

            await catchup.RunUntilCaughtUp();

            projectionStore.Count().Should().Be(1);
            projectionStore.Single().CursorPosition.Should().Be(1);
        }

        [Test]
        public async Task Catchup_can_query_streams_such_that_repeated_data_is_not_queried()
        {
            var queriedEvents = new ConcurrentBag<IDomainEvent>();

            var catchup = StreamCatchup.Create(stream.Trace(onResults: (q, b) =>
            {
                foreach (var e in b)
                {
                    queriedEvents.Add(e);
                }
            }), batchSize: 1);
            catchup.Subscribe(new BalanceProjector());

            store.WriteEvents(streamId);
            await catchup.RunSingleBatch();
            queriedEvents.Select(e => e.StreamRevision)
                         .ShouldBeEquivalentTo(new[] { 1 });

            store.WriteEvents(streamId);
            await catchup.RunSingleBatch();
            queriedEvents.Select(e => e.StreamRevision)
                         .ShouldBeEquivalentTo(new[] { 1, 2 });

            store.WriteEvents(streamId);
            await catchup.RunSingleBatch();
            queriedEvents.Select(e => e.StreamRevision)
                         .ShouldBeEquivalentTo(new[] { 1, 2, 3 });
        }

        [Test]
        public async Task When_multiple_projectors_are_subscribed_then_data_that_both_projections_have_seen_is_not_requeried()
        {
            var queriedEvents = new ConcurrentBag<IDomainEvent>();

            var balanceProjections = new InMemoryProjectionStore<BalanceProjection>();
            await balanceProjections.Put(streamId, new BalanceProjection
            {
                CursorPosition = 2
            });
            var catchup = StreamCatchup.Create(stream.Trace(onResults: (q, b) =>
            {
                foreach (var e in b)
                {
                    queriedEvents.Add(e);
                }
            }), batchSize: 10);
            catchup.Subscribe(new BalanceProjector(), balanceProjections);

            store.WriteEvents(streamId, howMany: 2);

            await catchup.RunSingleBatch();
            queriedEvents.Count
                         .Should()
                         .Be(1,
                             "the first two events should be skipped because of the starting cursor position");
            queriedEvents.Should()
                         .ContainSingle(e => e.StreamRevision == 3,
                                        "only the most recent event should be queried");

            var accountHistoryProjections = new InMemoryProjectionStore<AccountHistoryProjection>();
            await accountHistoryProjections.Put(streamId, new AccountHistoryProjection
            {
                CursorPosition = 2
            });
            catchup.Subscribe(new AccountHistoryProjector(), accountHistoryProjections);

            store.WriteEvents(streamId);

            await catchup.RunSingleBatch();

            queriedEvents.Select(e => e.StreamRevision)
                         .ShouldBeEquivalentTo(new[] { 3, 3, 4 },
                                               "event 3 needs to be repeated because the newly-subscribed aggregator hasn't seen it yet");
        }

        [Test]
        public async Task The_same_projection_is_not_queried_more_than_once_during_a_batch()
        {
            var projection = new BalanceProjection
            {
                CursorPosition = 1
            };

            var getCount = 0;
            var projectionStore = ProjectionStore.Create<string, BalanceProjection>(
                get: async key =>
                {
                    if (key.Contains(streamId))
                    {
                        Console.WriteLine("Get");
                        Interlocked.Increment(ref getCount);
                    }
                    return projection;
                },
                put: async (key, p) =>
                {
                    if (streamId == key)
                    {
                        Console.WriteLine("Put");
                    }
                    projection = p;
                });

            var catchup = StreamCatchup.Create(stream);
            catchup.Subscribe(new BalanceProjector(), projectionStore);

            store.WriteEvents(streamId, howMany: 3);

            await catchup.RunSingleBatch();

            getCount.Should().Be(1);
        }

        [Test]
        public async Task Getting_and_storing_projections_and_cursors_can_operate_transactionally_via_a_closure()
        {
            BalanceProjection finalProjection = null;

            var catchup = StreamCatchup.Create(stream);
            FetchAndSave<BalanceProjection> fetchAndSave = (async (id, callAggregatorPipeline) =>
            {
                using (var transaction = new TransactionScope())
                {
                    // e.g. get the projection / cursor from the store
                    var proj = new BalanceProjection
                    {
                        CursorPosition = 5
                    };

                    proj = await callAggregatorPipeline(proj);

                    finalProjection = proj;

                    // save the projection / cursor back to the store 
                    transaction.Complete();
                }
            });

            catchup.Subscribe(new BalanceProjector(), fetchAndSave);

            store.WriteEvents(streamId, amount: 100m, howMany: 5);

            await catchup.RunSingleBatch();

            finalProjection.Balance.Should().Be(100m);
        }

        [Test]
        public async Task When_advancing_the_cursor_throws_then_an_exception_is_thrown()
        {
            var values = Enumerable.Range(1, 20);
            var stream = Stream.Create<int, int>(
                query: q => values.Skip(q.Cursor.Position)
                                  .Take(q.BatchSize ?? 1000),
                advanceCursor: (q, b) =>
                {
                    Throw();
                });

            var catchup = StreamCatchup.Create(stream);

            catchup.Subscribe<Projection<int, int>, int>(async (sum, batch) =>
            {
                sum.Value += batch.Count;
                return sum;
            });

            Action runSingleBatch = () => catchup.RunSingleBatch().Wait();

            runSingleBatch.ShouldThrow<Exception>()
                          .And
                          .Message
                          .Should()
                          .Contain("oops!");
        }

        [Test]
        public async Task When_the_disposable_returned_by_Poll_is_disposed_then_no_further_polling_happens()
        {
            var pollCount = 0;
            var pollCountAtDispose = 0;
            var stream = Enumerable.Range(1, 1000).AsSequentialStream().Trace();
            var catchup = StreamCatchup.Create(stream, batchSize: 1);
            catchup.Subscribe<int, int>(async (p, b) =>
            {
                pollCount++;
                return p + b.Count;
            });

            using (catchup.Poll(TimeSpan.FromMilliseconds(100)))
            {
                await Task.Delay(50);
                pollCountAtDispose = pollCount;
            }

            await Task.Delay(300);

            Console.WriteLine(new { pollCount });

            pollCount.Should().Be(pollCountAtDispose);
        }

        [Test]
        public async Task A_backoff_can_be_specified_so_that_polling_slows_when_there_is_no_new_data()
        {
           // arrange
            var fetchCount = 0;

            var stream = Stream.Create<string, int>(async q =>
            {
                Interlocked.Increment(ref fetchCount);
                return Enumerable.Empty<string>();
            })
                                   .Trace();

            var catchup = StreamCatchup.Create(stream)
                                       .Backoff(5.Seconds());

            catchup.Subscribe(async (p, b) =>
            {
                p.Value = p.Value ?? new List<string>();
                p.Value.AddRange(b);
                return p;
            }, new InMemoryProjectionStore<Projection<List<string>, int>>());

            // act
            using (catchup.Poll(TimeSpan.FromSeconds(.1)))
            {
                await Task.Delay(1.Seconds());
            }

            // assert
            fetchCount.Should().Be(1);
        }

        [Test]
        public async Task A_backoff_can_be_specified_so_that_distributor_slows_when_there_is_no_new_data()
        {
            // arrange
            var fetchCount = 0;

            var stream = Stream.PartitionedByValue<string, int, string>(
                query: async (query, partition) =>
                {
                    Interlocked.Increment(ref fetchCount);
                    return Enumerable.Empty<string>();
                });

            var partitions = Values.AtoZ()
                                   .Select(Partition.ByValue)
                                   .ToArray();

            var distributor = partitions.CreateInMemoryDistributor(
                waitInterval: TimeSpan.FromSeconds(.1),
                maxDegreesOfParallelism: 30,
                defaultLeaseDuration: 5.Seconds())
                                        .Trace();

            var catchup = stream.CreateDistributedCatchup(distributor)
                                .Backoff(1.Seconds());

            catchup.Subscribe(async (p, b) =>
            {
                p.Value = p.Value ?? new List<string>();
                p.Value.AddRange(b);
                return p;
            }, new InMemoryProjectionStore<Projection<List<string>, int>>());

            // act
            await distributor.Start();
            await Task.Delay(2.Seconds());
            await distributor.Stop();

            // assert
            fetchCount.Should().Be(26);
        }

        private void Throw()
        {
            throw new Exception("oops!");
        }
    }


}