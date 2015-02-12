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
    public class StreamOfStreamsCatchupTests
    {
        private IStoreEvents store;
        private string[] streamIds;
        private NEventStoreStreamSource streamSource;

        [SetUp]
        public void SetUp()
        {
            // populate the event store
            store = TestEventStore.Create();

            streamIds = Enumerable.Range(1, 1000)
                                  .Select(_ => Guid.NewGuid().ToString())
                                  .ToArray();

            foreach (var streamId in streamIds)
            {
                WriteEvent(streamId, 1m);
            }

            streamSource = new NEventStoreStreamSource(store);
        }

        private void WriteEvent(string streamId, decimal amount = 1)
        {
            using (var stream = store.OpenStream(streamId, 0))
            {
                if (amount > 0)
                {
                    stream.Add(new EventMessage
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
                    stream.Add(new EventMessage
                    {
                        Body = new FundsWithdrawn
                        {
                            AggregateId = streamId,
                            Amount = amount
                        }
                    });
                }

                stream.CommitChanges(Guid.NewGuid());
            }
        }

        [Test]
        public async Task Catchup_can_use_a_sequence_of_keys_to_traverse_all_aggregates()
        {
            var projectionStore = new InMemoryProjectionStore<BalanceProjection>();

            var catchup = StreamCatchup.Create(streamSource.UpdatedStreams().Trace());
            catchup.Subscribe(new BalanceProjector().Trace(), projectionStore);
            await catchup.RunSingleBatch();

            projectionStore.Sum(b => b.Balance)
                           .Should()
                           .Be(1000);
            projectionStore.Select(b => b.AggregateId)
                           .Distinct()
                           .Count()
                           .Should()
                           .Be(1000);
        }

        [Test]
        public async Task When_one_batch_is_running_a_second_call_to_RunSingleBatch_will_not_do_anything()
        {
            var projectionStore = new InMemoryProjectionStore<BalanceProjection>();

            var catchup = StreamCatchup.Create(streamSource.UpdatedStreams(), batchCount: 1);
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
        public async Task Catchup_upstream_batch_size_can_be_specified()
        {
            var projectionStore = new InMemoryProjectionStore<BalanceProjection>();

            var catchup = StreamCatchup.Create(streamSource.UpdatedStreams(), batchCount: 20);
            catchup.Subscribe(new BalanceProjector(), projectionStore);

            await catchup.RunSingleBatch();

            projectionStore.Count()
                           .Should()
                           .Be(20);
        }

        [Test]
        public async Task Catchup_upstream_cursor_can_be_specified()
        {
            var catchup = StreamCatchup.Create(streamSource.UpdatedStreams(), batchCount: 500);
            catchup.Subscribe(new BalanceProjector(), new InMemoryProjectionStore<BalanceProjection>());

            var cursor = await catchup.RunSingleBatch();

            var projectionStore = new InMemoryProjectionStore<BalanceProjection>();
            catchup = StreamCatchup.Create(streamSource.UpdatedStreams(), cursor);
            catchup.Subscribe(new BalanceProjector(), projectionStore);

            await catchup.RunSingleBatch();

            projectionStore.Count()
                           .Should()
                           .Be(500);
        }

        [Test]
        public async Task Catchup_query_cursor_resumes_from_last_position()
        {
            var projectionStore = new InMemoryProjectionStore<BalanceProjection>();

            var catchup = StreamCatchup.Create(streamSource.UpdatedStreams(), batchCount: 500);
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

            var catchup = StreamCatchup.Create(streamSource.UpdatedStreams(), batchCount: 10);
            catchup.Subscribe(new BalanceProjector(), projectionStore);

            TaskScheduler.UnobservedTaskException += (sender, args) => Console.WriteLine(args.Exception);

            await catchup.RunUntilCaughtUp();

            projectionStore.Sum(b => b.Balance)
                           .Should()
                           .Be(1000);
            projectionStore.Select(b => b.AggregateId)
                           .Distinct()
                           .Count()
                           .Should()
                           .Be(1000);
        }

        [Test]
        public async Task When_projections_are_cursors_then_catchup_does_not_replay_previously_seen_events()
        {
            var projectionStore = new InMemoryProjectionStore<BalanceProjection>();
            var eventsAggregated = new List<IDomainEvent>();
            var catchup = StreamCatchup.Create(streamSource.UpdatedStreams(), batchCount: 1000);
            catchup.Subscribe(new BalanceProjector()
                                  .Trace((p, es) => eventsAggregated.AddRange(es)), projectionStore);

            await catchup.RunUntilCaughtUp();

            var streamId = streamIds.First();

            WriteEvent(streamId, 100m);

            var cursor = await catchup.RunUntilCaughtUp();

            cursor.As<string>()
                  .Should()
                  .Be("1001");

            var balanceProjection = await projectionStore.Get(streamId);

            balanceProjection.Balance.Should().Be(101);
            balanceProjection.CursorPosition.Should().Be(2);
            eventsAggregated.Count.Should().Be(1001);
        }

        [Test]
        public async Task Catchup_Poll_keeps_projections_updated_as_new_event_streams_are_created()
        {
            var projectionStore = new InMemoryProjectionStore<BalanceProjection>();

            var catchup = StreamCatchup.Create(streamSource.UpdatedStreams(), batchCount: 50);
            catchup.Subscribe(new BalanceProjector(), projectionStore);

            using (catchup.Poll(TimeSpan.FromMilliseconds(10)))
            {
                // write more events
                Task.Run(async () =>
                {
                    foreach (var streamId in Enumerable.Range(1, 200).Select(_=>Guid.NewGuid().ToString() ))
                    {
                        WriteEvent(streamId);
                        await Task.Delay(1);
                    }
                    Console.WriteLine("wrote 200 more events");
                });

                await Wait.Until(() =>
                {
                    var sum = projectionStore.Sum(b => b.Balance);
                    Console.WriteLine("sum is " + sum);
                    return sum >= 1200;
                });
            }
        }

        [Test]
        public async Task Catchup_Poll_keeps_projections_updated_as_new_events_are_written_to_existing_streams()
        {
            var projectionStore = new InMemoryProjectionStore<BalanceProjection>();

            var catchup = StreamCatchup.Create(streamSource.UpdatedStreams(), batchCount: 50);
            catchup.Subscribe(new BalanceProjector(), projectionStore);

            using (catchup.Poll(TimeSpan.FromMilliseconds(10)))
            {
                // write more events
                Task.Run(async () =>
                {
                    foreach (var streamId in streamIds.Take(200))
                    {
                        WriteEvent(streamId);
                        await Task.Delay(1);
                    }
                    Console.WriteLine("wrote 200 more events");
                });

                await Wait.Until(() =>
                {
                    var sum = projectionStore.Sum(b => b.Balance);
                    Console.WriteLine("sum is " + sum);
                    return sum >= 1200;
                });
            }
        }

        [Test]
        public async Task RunSingleBatch_throws_when_an_aggregator_throws_an_exception()
        {
            var projectionStore = new InMemoryProjectionStore<BalanceProjection>();

            var projector = new BalanceProjector()
                .Pipeline(async (projection, batch, next) =>
                {
                    if (projectionStore.Count() >= 30)
                    {
                        throw new Exception("oops");
                    }
                    await next(projection, batch);
                });

            var catchup = StreamCatchup.Create(streamSource.UpdatedStreams(), batchCount: 50);
            catchup.Subscribe(projector, projectionStore);

            Action runSingleBatch = () => catchup.RunSingleBatch().Wait();

            runSingleBatch.ShouldThrow<Exception>()
                          .And
                          .Message
                          .Should()
                          .Contain("oops");
        }

        [Test]
        public async Task Catch_allows_aggregator_exceptions_to_be_prevented_from_stopping_catchup()
        {
            var projectionStore = new InMemoryProjectionStore<BalanceProjection>();

            var projector = new BalanceProjector()
                .Pipeline(async (projection, batch, next) =>
                {
                    if (projectionStore.Count() == 30)
                    {
                        throw new Exception("oops");
                    }
                    await next(projection, batch);
                })
                .Catch(continueIf: (projection, batch, next) => true);

            var catchup = StreamCatchup.Create(streamSource.UpdatedStreams(), batchCount: 50);
            catchup.Subscribe(projector, projectionStore);

            await catchup.RunSingleBatch();

            projectionStore.Count().Should().Be(50);
        }

        [Test]
        public async Task Catchup_cursor_storage_can_be_specified_using_catchup_configuration()
        {
            ICursor storedCursor = null;

            var catchup = StreamCatchup.Create(streamSource.UpdatedStreams(),
                                               batchCount: 1,
                                               configure: configure => configure.StoreCursor(async (id, c) => storedCursor = c));
            catchup.Subscribe(new BalanceProjector());

            var cursor = await catchup.RunSingleBatch();

            storedCursor.Should().BeSameAs(cursor);
        }

        [Test]
        public async Task Catchup_cursor_retrieval_can_be_specified_using_catchup_configuration()
        {
            ICursor storedCursor = Cursor.Create("3");

            var catchup = StreamCatchup.Create(streamSource.UpdatedStreams(),
                                               batchCount: 1,
                                               configure: configure => configure.GetCursor(async id => storedCursor));
            catchup.Subscribe(new BalanceProjector());

            var cursor = await catchup.RunSingleBatch();

            cursor.Should().BeSameAs(storedCursor);
            cursor.As<string>().Should().Be("4");
        }

        [Test]
        public async Task Catchup_can_query_downstream_streams_such_that_repeated_data_is_not_queried()
        {
            var streamId = Guid.NewGuid().ToString();
            var queriedEvents = new ConcurrentBag<IDomainEvent>();

            var catchup = StreamCatchup.Create(streamSource.UpdatedStreams()
                                                           .Map(ss => ss.Select(s => s.Trace(onResults: (q, b) =>
                                                           {
                                                               foreach (var e in b)
                                                               {
                                                                   queriedEvents.Add(e);
                                                               }
                                                           }))),
                                               Cursor.Create("1000"),
                                               batchCount: 1);
            catchup.Subscribe(new BalanceProjector());

            WriteEvent(streamId);
            await catchup.RunSingleBatch();
            queriedEvents.Select(e => e.StreamRevision)
                         .ShouldBeEquivalentTo(new[] { 1 });

            WriteEvent(streamId);
            await catchup.RunSingleBatch();
            queriedEvents.Select(e => e.StreamRevision)
                         .ShouldBeEquivalentTo(new[] { 1, 2 });

            WriteEvent(streamId);
            await catchup.RunSingleBatch();
            queriedEvents.Select(e => e.StreamRevision)
                         .ShouldBeEquivalentTo(new[] { 1, 2, 3 });
        }

        [Test]
        public async Task When_multiple_projectors_are_subscribed_then_data_that_both_projections_have_seen_is_not_requeried()
        {
            var streamId = Guid.NewGuid().ToString();
            var queriedEvents = new ConcurrentBag<IDomainEvent>();

            var balanceProjections = new InMemoryProjectionStore<BalanceProjection>();
            await balanceProjections.Put(streamId, new BalanceProjection
            {
                AggregateId = streamId,
                CursorPosition = 2
            });
            var catchup = StreamCatchup.Create(streamSource.UpdatedStreams()
                                                           .Map(ss => ss.Select(s => s.Trace(onResults: (q, b) =>
                                                           {
                                                               foreach (var e in b)
                                                               {
                                                                   queriedEvents.Add(e);
                                                               }
                                                           }))),
                                               Cursor.Create("1000"),
                                               batchCount: 1);
            catchup.Subscribe(new BalanceProjector(), balanceProjections);

            WriteEvent(streamId);
            WriteEvent(streamId);
            WriteEvent(streamId);

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

            WriteEvent(streamId);
            await catchup.RunSingleBatch();

            queriedEvents.Select(e => e.StreamRevision)
                         .ShouldBeEquivalentTo(new[] { 3, 3, 4 },
                                               "event 3 needs to be repeated because the newly-subscribed aggregator hasn't seen it yet");
        }

        [Test]
        public async Task The_same_projection_is_not_queried_more_than_once_during_a_batch()
        {
            var streamId = "hello";
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

            var catchup = StreamCatchup.Create(streamSource.UpdatedStreams());
            catchup.Subscribe(new BalanceProjector(), projectionStore);

            WriteEvent(streamId);
            WriteEvent(streamId);
            WriteEvent(streamId);

            await catchup.RunSingleBatch();

            getCount.Should().Be(1);
        }
    }
}