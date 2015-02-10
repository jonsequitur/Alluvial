using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using FluentAssertions;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Alluvial.Tests.BankDomain;
using NEventStore;
using NUnit.Framework;

namespace Alluvial.Tests
{
    [TestFixture]
    public class StreamCatchupTests
    {
        private IStoreEvents store;
        private string streamId;
        private IStream<IDomainEvent> stream;

        [SetUp]
        public void SetUp()
        {
            // populate the event store
            store = TestEventStore.Create();

            streamId = Guid.NewGuid().ToString();

            WriteEvents();

            stream = new NEventStoreStream(store, streamId).DomainEvents();
        }

        [Test]
        public async Task Catchup_can_traverse_all_events()
        {
            var projectionStore = new InMemoryProjectionStore<BalanceProjection>();
            WriteEvents(999);
            var catchup = StreamCatchup.Create(stream);
            catchup.Subscribe(new BalanceProjector(), projectionStore);
            await catchup.RunSingleBatch();

            projectionStore.Sum(b => b.Balance)
                           .Should()
                           .Be(1000);
            projectionStore.Select(b => b.AggregateId)
                           .Distinct()
                           .Count()
                           .Should()
                           .Be(1);
        }

        [Test]
        public async Task When_one_batch_is_running_a_second_call_to_RunSingleBatch_will_not_do_anything()
        {
            var projectionStore = new InMemoryProjectionStore<BalanceProjection>();

            var catchup = StreamCatchup.Create(stream, batchCount: 1);
            catchup.Subscribe(new BalanceProjector()
                                  .Pipeline(async (projection, batch, next) =>
                                  {
                                      await Task.Delay(500);
                                      await next(projection, batch);
                                  }), projectionStore);

            catchup.RunSingleBatch();
            catchup.RunSingleBatch();

            await Task.Delay(1000);

            projectionStore.Count()
                           .Should()
                           .Be(1);
        }

        [Test]
        public async Task Catchup_batch_size_can_be_specified()
        {
            var projectionStore = new InMemoryProjectionStore<BalanceProjection>();
            WriteEvents(howMany: 50);
            var catchup = StreamCatchup.Create(stream, batchCount: 20);
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

            WriteEvents(howMany: 999);

            var catchup = StreamCatchup.Create(stream, batchCount: 500);
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
        public async Task Catchup_RunUntilCaughtUp_runs_until_the_stream_has_no_more_results()
        {
            var projectionStore = new InMemoryProjectionStore<BalanceProjection>();
            WriteEvents(howMany: 999);
            var catchup = StreamCatchup.Create(stream, batchCount: 10);
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
            var catchup = StreamCatchup.Create(stream, batchCount: 100);
            catchup.Subscribe(new BalanceProjector().Trace((p, es) => eventsAggregated.AddRange(es)), projectionStore);

            await catchup.RunUntilCaughtUp();

            WriteEvents(howMany: 100);

            var cursor = await catchup.RunUntilCaughtUp();

            cursor.As<int>()
                  .Should()
                  .Be(101);

            var balanceProjection = await projectionStore.Get(streamId);

            balanceProjection.Balance.Should().Be(101);
            balanceProjection.CursorPosition.Should().Be(101);
            eventsAggregated.Count.Should().Be(101);
        }

        [Test]
        public async Task Catchup_Poll_keeps_projections_updated_as_new_events_are_written()
        {
            var projectionStore = new InMemoryProjectionStore<BalanceProjection>();

            var catchup = StreamCatchup.Create(stream, batchCount: 50);
            catchup.Subscribe(new BalanceProjector(), projectionStore);

            using (catchup.Poll(TimeSpan.FromMilliseconds(10)))
            using (Background.Loop(_ => WriteEvents(howMany: 2), .1))
            {
                await Wait.Until(() =>
                {
                    var sum = projectionStore.Sum(b => b.Balance);
                    Console.WriteLine("sum is " + sum);
                    return sum >= 500;
                });
            }
        }

        [Test]
        public async Task RunSingleBatch_throws_when_an_aggregator_throws_an_exception()
        {
            var projectionStore = new InMemoryProjectionStore<BalanceProjection>();
            WriteEvents(100);

            var projector = new BalanceProjector()
                .Pipeline(async (projection, batch, next) =>
                {
                    Throw();
                    await next(projection, batch);
                });

            var catchup = StreamCatchup.Create(stream, batchCount: 50);
            catchup.Subscribe(projector, projectionStore);

            Action runSingleBatch = () => catchup.RunSingleBatch().Wait();

            runSingleBatch.ShouldThrow<Exception>()
                          .And
                          .Message
                          .Should()
                          .Contain("oops");
        }

        private void Throw()
        {
            throw new Exception("oops");
        }

        [Test]
        public async Task Catch_allows_aggregator_exceptions_to_be_prevented_from_stopping_catchup()
        {
            var projectionStore = new InMemoryProjectionStore<BalanceProjection>();

            var projector = new BalanceProjector()
                .Pipeline(async (projection, batch, next) =>
                {
                    Throw();
                    await next(projection, batch);
                })
                .Catch(continueIf: (projection, batch, next) => true);

            var catchup = StreamCatchup.Create(stream, batchCount: 50);
            catchup.Subscribe(projector, projectionStore);

            await catchup.RunSingleBatch();

            projectionStore.Count().Should().Be(1);
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
            }),
                                               stream.NewCursor(),
                                               batchCount: 1);
            catchup.Subscribe(new BalanceProjector());

            WriteEvents();
            await catchup.RunSingleBatch();
            queriedEvents.Select(e => e.StreamRevision)
                         .ShouldBeEquivalentTo(new[] { 1 });

            WriteEvents();
            await catchup.RunSingleBatch();
            queriedEvents.Select(e => e.StreamRevision)
                         .ShouldBeEquivalentTo(new[] { 1, 2 });

            WriteEvents();
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
                AggregateId = streamId,
                CursorPosition = 2
            });
            var catchup = StreamCatchup.Create(stream.Trace(onResults: (q, b) =>
            {
                foreach (var e in b)
                {
                    queriedEvents.Add(e);
                }
            }), batchCount: 10);
            catchup.Subscribe(new BalanceProjector(), balanceProjections);

            WriteEvents(howMany: 2);

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
                AggregateId = streamId,
                CursorPosition = 2
            });
            catchup.Subscribe(new AccountHistoryProjector(), accountHistoryProjections);

            WriteEvents();

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
                AggregateId = streamId,
                CursorPosition = 1
            };

            var getCount = 0;
            var projectionStore = ProjectionStore.Create<string, BalanceProjection>(
                get: async key =>
                {
                    if (streamId == key)
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

            WriteEvents(howMany: 3);

            await catchup.RunSingleBatch();

            getCount.Should().Be(1);
        }

        private void WriteEvents(decimal amount = 1, int howMany = 1, string streamId = null)
        {
            streamId = streamId ?? this.streamId;

            for (int i = 0; i < howMany; i++)
            {
                using (var eventStream = store.OpenStream(streamId, 0))
                {
                    if (amount > 0)
                    {
                        eventStream.Add(new EventMessage
                        {
                            Body = new FundsDeposited
                            {
                                AggregateId = streamId,
                                Amount = amount
                            }
                        });
                    }
                    else
                    {
                        eventStream.Add(new EventMessage
                        {
                            Body = new FundsWithdrawn
                            {
                                AggregateId = streamId,
                                Amount = amount
                            }
                        });
                    }

                    eventStream.CommitChanges(Guid.NewGuid());
                }
            }
        }
    }
}