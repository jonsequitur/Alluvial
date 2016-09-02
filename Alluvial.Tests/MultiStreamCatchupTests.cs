using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using FluentAssertions;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Alluvial.Tests.BankDomain;
using Alluvial.Tests.StreamImplementations.NEventStore;
using Its.Log.Instrumentation;
using NEventStore;
using NUnit.Framework;

#pragma warning disable 4014

namespace Alluvial.Tests
{
    [TestFixture]
    public class MultiStreamCatchupTests
    {
        private IStoreEvents store;
        private string[] streamIds;
        private NEventStoreStreamSource streamSource;

        [SetUp]
        public void SetUp()
        {
            // populate the event store
            store = TestEventStore.Create();

            streamIds = Enumerable.Range(1, 100)
                                  .Select(_ => Guid.NewGuid().ToString())
                                  .ToArray();

            foreach (var streamId in streamIds)
            {
                store.WriteEvent(streamId);
            }

            streamSource = new NEventStoreStreamSource(store);
        }

        [Test]
        public async Task Catchup_can_use_a_sequence_of_keys_to_traverse_all_aggregates()
        {
            var projectionStore = new InMemoryProjectionStore<BalanceProjection>();

            var catchup = StreamCatchup.All(streamSource.StreamPerAggregate());

            using (catchup.Subscribe(new BalanceProjector(), projectionStore))
            {
                await catchup.RunSingleBatch();

                projectionStore.Sum(b => b.Balance)
                               .Should()
                               .Be(100);
                projectionStore.Count()
                               .Should()
                               .Be(100);
            }
        }

        [Test]
        public async Task When_one_batch_is_running_a_second_call_to_RunSingleBatch_will_not_do_anything()
        {
            var projectionStore = new InMemoryProjectionStore<BalanceProjection>();

            var catchup = StreamCatchup.All(streamSource.StreamPerAggregate(), batchSize: 1);
            using (catchup.Subscribe(new BalanceProjector()
                                         .Pipeline(async (projection, batch, next) =>
                                         {
                                             await Task.Delay(500);
                                             await next(projection, batch);
                                         }), projectionStore))
            {
                await Task.WhenAll(catchup.RunSingleBatch(), catchup.RunSingleBatch());

                projectionStore.Count()
                               .Should()
                               .Be(1);
            }
        }

        [Test]
        public async Task Catchup_upstream_batch_size_can_be_specified()
        {
            var projectionStore = new InMemoryProjectionStore<BalanceProjection>();

            var catchup = StreamCatchup.All(streamSource.StreamPerAggregate(), batchSize: 20);

            using (catchup.Subscribe(new BalanceProjector(), projectionStore))
            {
                await catchup.RunSingleBatch();

                projectionStore.Count()
                               .Should()
                               .Be(20);
            }
        }

        [Test]
        public async Task Catchup_starting_cursor_can_be_specified()
        {
            var projectionStore = new InMemoryProjectionStore<BalanceProjection>();
            var updatedStreams = streamSource.StreamPerAggregate();
            var cursor = updatedStreams.NewCursor();
            cursor.AdvanceTo("50");
            var catchup = StreamCatchup.All(updatedStreams, cursor);

            using (catchup.Subscribe(new BalanceProjector(), projectionStore))
            {
                await catchup.RunSingleBatch();
            }

            projectionStore.Count()
                           .Should()
                           .Be(50);
        }

        [Test]
        public async Task Stream_traversal_can_continue_from_upstream_cursor()
        {
            var projectionStore = new InMemoryProjectionStore<BalanceProjection>();
            var catchup = StreamCatchup.All(streamSource.StreamPerAggregate(), Cursor.New("50"));

            using (catchup.Subscribe(new BalanceProjector(), projectionStore))
            {
                await catchup.RunSingleBatch();
            }
            projectionStore.Count()
                           .Should()
                           .Be(50);
        }

        [Test]
        public async Task Catchup_query_cursor_resumes_from_last_position()
        {
            var projectionStore = new InMemoryProjectionStore<BalanceProjection>();

            var catchup = StreamCatchup.All(streamSource.StreamPerAggregate().Trace(), batchSize: 50);
            using (catchup.Subscribe(new BalanceProjector(), projectionStore))
            {
                await catchup.RunSingleBatch();

                projectionStore.Sum(b => b.Balance)
                               .Should()
                               .Be(50);

                await catchup.RunSingleBatch();

                projectionStore.Sum(b => b.Balance)
                               .Should()
                               .Be(100);
            }
        }

        [Test]
        public async Task Catchup_RunUntilCaughtUp_runs_until_the_stream_has_no_more_results()
        {
            var projectionStore = new InMemoryProjectionStore<BalanceProjection>();

            var catchup = StreamCatchup.All(streamSource.StreamPerAggregate(), batchSize: 10);

            using (catchup.Subscribe(new BalanceProjector(), projectionStore))
            {
                TaskScheduler.UnobservedTaskException += (sender, args) => Console.WriteLine(args.Exception);

                await catchup.RunUntilCaughtUp();
            }

            Console.WriteLine(projectionStore
                .Select(p => p.Value)
                .ToLogString());

            projectionStore.Sum(b => b.Balance)
                           .Should()
                           .Be(100);
            projectionStore.Count()
                           .Should()
                           .Be(100);
        }

        [Test]
        public async Task When_projections_are_cursors_then_catchup_does_not_replay_previously_seen_events()
        {
            var projectionStore = new InMemoryProjectionStore<BalanceProjection>();
            var eventsAggregated = new ConcurrentBag<IDomainEvent>();
            var catchup = StreamCatchup.All(streamSource.StreamPerAggregate(), batchSize: 100);

            using (catchup.Subscribe(new BalanceProjector()
                                         .Trace((p, es) =>
                                         {
                                             foreach (var e in es)
                                             {
                                                 eventsAggregated.Add(e);
                                             }
                                         }), projectionStore))
            {
                await catchup.RunUntilCaughtUp();

                var streamId = streamIds.First();

                store.WriteEvent(streamId, amount: 100m);

                await catchup.RunUntilCaughtUp();

                var balanceProjection = await projectionStore.Get(streamId);

                balanceProjection.Balance.Should().Be(101);
                balanceProjection.CursorPosition.Should().Be(2);
                eventsAggregated.Count.Should().Be(101);
            }
        }

        [Test]
        public async Task Catchup_Poll_keeps_projections_updated_as_new_event_streams_are_created()
        {
            var projectionStore = new InMemoryProjectionStore<BalanceProjection>();

            var catchup = StreamCatchup.All(streamSource.StreamPerAggregate(),
                                            batchSize: 5);

            using (catchup.Subscribe(new BalanceProjector(), projectionStore))
            using (catchup.Poll(TimeSpan.FromMilliseconds(10)))
            {
                // write more events
                Task.Run(async () =>
                {
                    foreach (var streamId in Enumerable.Range(1, 20).Select(_ => Guid.NewGuid().ToString()))
                    {
                        store.WriteEvent(streamId);
                        await Task.Delay(1);
                    }
                    Console.WriteLine("wrote 200 more events");
                });

                await Wait.Until(() =>
                {
                    var sum = projectionStore.Sum(b => b.Balance);
                    Console.WriteLine("sum is " + sum);
                    return sum >= 120;
                });
            }
        }

        [Test]
        public async Task Catchup_Poll_keeps_projections_updated_as_new_events_are_written_to_existing_streams()
        {
            var projectionStore = new InMemoryProjectionStore<BalanceProjection>();
            var done = false;
            var catchup = StreamCatchup.All(streamSource.StreamPerAggregate(), batchSize: 5);

            using (catchup.Subscribe(new BalanceProjector().Trace(), projectionStore.Trace()))
            using (catchup.Poll(TimeSpan.FromMilliseconds(10)))
            {
                // write more events
                Task.Run(async () =>
                {
                    Console.WriteLine($"there are {streamIds.Length} stream ids");

                    foreach (var streamId in streamIds)
                    {
                        await Task.Delay(2);
                        if (!done)
                        {
                            store.WriteEvent(streamId);
                        }
                    }
                });

                await Wait.Until(() =>
                {
                    var sum = projectionStore.Sum(b => b.Balance);
                    Console.WriteLine("sum is " + sum);
                    done = sum >= 120;
                    return done;
                }, pollInterval: 20.Milliseconds());
            }
        }

        [Test]
        public void RunSingleBatch_throws_when_an_aggregator_throws_an_exception()
        {
            var projectionStore = new InMemoryProjectionStore<BalanceProjection>();
            var count = 0;
            var projector = new BalanceProjector()
                .Pipeline(async (projection, batch, next) =>
                {
                    Interlocked.Increment(ref count);

                    if (count >= 30)
                    {
                        throw new Exception("oops");
                    }

                    await next(projection, batch);
                });

            var catchup = StreamCatchup.All(streamSource.StreamPerAggregate(), batchSize: 50);

            using (catchup.Subscribe(projector, projectionStore))
            {
                Action runSingleBatch = () => catchup.RunSingleBatch().Wait();

                runSingleBatch.ShouldThrow<Exception>()
                              .And
                              .Message
                              .Should()
                              .Contain("oops");
            }
        }

        [Test]
        public async Task OnError_Continue_prevents_aggregator_exceptions_from_stopping_catchup()
        {
            var count = 0;
            var projectionStore = new InMemoryProjectionStore<BalanceProjection>();

            var projector = new BalanceProjector()
                .Pipeline(async (projection, batch, next) =>
                {
                    Interlocked.Increment(ref count);
                    if (count < 20)
                    {
                        throw new Exception("oops");
                    }
                    await next(projection, batch);
                }).Trace();

            var catchup = StreamCatchup.All(streamSource.StreamPerAggregate().Trace(),
                                            batchSize: 50);

            using (catchup.Subscribe(projector,
                                     projectionStore.AsHandler(),
                                     onError: e => e.Continue()))
            {
                await catchup.RunSingleBatch();
            }

            projectionStore.Count().Should().Be(31);
        }

        [Test]
        public async Task Catchup_cursor_storage_can_be_specified_using_catchup_configuration()
        {
            ICursor<string> storedCursor = null;

            var catchup = StreamCatchup.All(streamSource.StreamPerAggregate(),
                                            batchSize: 10,
                                            manageUpstreamCursor: async (id, use) =>
                                            {
                                                var c = streamSource.StreamPerAggregate().NewCursor();
                                                await use(c);
                                                storedCursor = c;
                                            });
            using (catchup.Subscribe(new BalanceProjector()))
            {
                await catchup.RunSingleBatch();

                storedCursor.Position.Should().Be("10");
            }
        }

        [Test]
        public async Task Catchup_cursor_retrieval_can_be_specified_using_catchup_configuration()
        {
            ICursor<string> storedCursor = Cursor.New("3");

            var catchup = StreamCatchup.All(streamSource.StreamPerAggregate(),
                                            batchSize: 1,
                                            manageUpstreamCursor: async (id, use) => { await use(storedCursor); });
            using (catchup.Subscribe(new BalanceProjector()))
            {
                await catchup.RunSingleBatch();

                storedCursor.Position.Should().Be("4");
            }
        }

        [Test]
        public async Task Catchup_can_query_downstream_streams_such_that_repeated_data_is_not_queried()
        {
            var store = this.store;
            var streamId = Guid.NewGuid().ToString();
            var queriedEvents = new ConcurrentBag<IDomainEvent>();

            var catchup = StreamCatchup.All(streamSource.StreamPerAggregate()
                                                        .Map(ss => ss.Select(s => s.Trace(onResults: (q, b) =>
                                                        {
                                                            foreach (var e in b)
                                                            {
                                                                queriedEvents.Add(e);
                                                            }
                                                        }))),
                                            Cursor.New("100"),
                                            batchSize: 1);

            using (catchup.Subscribe(new BalanceProjector()))
            {
                store.WriteEvent(streamId);
                await catchup.RunSingleBatch();
                queriedEvents.Select(e => e.StreamRevision)
                             .ShouldBeEquivalentTo(new[] { 1 });

                store.WriteEvent(streamId);
                await catchup.RunSingleBatch();
                queriedEvents.Select(e => e.StreamRevision)
                             .ShouldBeEquivalentTo(new[] { 1, 2 });

                store.WriteEvent(streamId);
                await catchup.RunSingleBatch();
                queriedEvents.Select(e => e.StreamRevision)
                             .ShouldBeEquivalentTo(new[] { 1, 2, 3 });
            }
        }

        [Test]
        public async Task When_multiple_projectors_are_subscribed_then_data_that_both_projections_have_seen_is_not_requeried()
        {
            var streamId = Guid.NewGuid().ToString();
            var queriedEvents = new ConcurrentBag<IDomainEvent>();

            var balanceProjections = new InMemoryProjectionStore<BalanceProjection>().Trace();
            await balanceProjections.Put(streamId, new BalanceProjection
            {
                CursorPosition = 2
            });
            var catchup = StreamCatchup.All(streamSource.StreamPerAggregate()
                                                        .Trace()
                                                        .Map(ss => ss.Select(s => s.Trace(onResults: (q, b) =>
                                                        {
                                                            foreach (var e in b)
                                                            {
                                                                queriedEvents.Add(e);
                                                            }
                                                        }))),
                                            Cursor.New("100"),
                                            batchSize: 1);

            using (catchup.Subscribe(new BalanceProjector(), balanceProjections))
            {
                store.WriteEvent(streamId, amount: 1); // "101" - 1
                store.WriteEvent(streamId, amount: 2); // "102" - 2
                store.WriteEvent(streamId, amount: 3); // "103" - 3

                await catchup.RunSingleBatch();

                queriedEvents.Count
                             .Should()
                             .Be(1,
                                 "the first two events should be skipped because of the starting cursor position");
                queriedEvents.Should()
                             .ContainSingle(e => e.StreamRevision == 3,
                                            "only the most recent event should be queried");

                var accountHistoryProjections = new InMemoryProjectionStore<AccountHistoryProjection>().Trace();
                await accountHistoryProjections.Put(streamId, new AccountHistoryProjection
                {
                    CursorPosition = 2
                });

                using (catchup.Subscribe(new AccountHistoryProjector(), accountHistoryProjections))
                {
                    store.WriteEvent(streamId, amount: 4);

                    await catchup.RunSingleBatch();

                    queriedEvents.Select(e => e.StreamRevision)
                                 .ShouldBeEquivalentTo(new[] { 3, 3, 4 },
                                                       "event 3 needs to be repeated because the newly-subscribed aggregator hasn't seen it yet");
                }
            }
        }

        [Test]
        public async Task The_same_projection_is_not_queried_more_than_once_during_a_batch()
        {
            var streamId = "hello";
            var projection = new BalanceProjection
            {
                CursorPosition = 1
            };

            var getCount = 0;
            var projectionStore = ProjectionStore.Create<string, BalanceProjection>(
                get: key =>
                {
                    if (key.Contains(streamId))
                    {
                        Interlocked.Increment(ref getCount);
                    }
                    return projection;
                },
                put: (key, p) => projection = p);

            var catchup = StreamCatchup.All(streamSource.StreamPerAggregate());
            using (catchup.Subscribe(new BalanceProjector(), projectionStore))
            {
                store.WriteEvent(streamId);
                store.WriteEvent(streamId);
                store.WriteEvent(streamId);

                await catchup.RunSingleBatch();
            }

            getCount.Should().Be(1);
        }

        [Test]
        public async Task A_backoff_can_be_specified_so_that_polling_slows_when_there_is_no_new_data()
        {
            // arrange
            var fetchCount = 0;

            var streams = Stream.Create<string, int>(q =>
            {
                Interlocked.Increment(ref fetchCount);
                return Enumerable.Empty<string>();
            }) .Trace()
                                .IntoMany((item, cursor, toCursor) => Enumerable.Empty<string>().AsSequentialStream());

            var catchup = StreamCatchup.All(streams)
                                       .Backoff(5.Seconds());

            catchup.Subscribe((p, b) =>
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

            var streams = Stream.PartitionedByValue<string, int, string>(
                query: async (query, partition) =>
                {
                    Interlocked.Increment(ref fetchCount);
                    return Enumerable.Empty<string>();
                })
                                .IntoMany((a, b, c, d) => Enumerable.Empty<string>().AsSequentialStream())
                                .Trace();

            var partitions = Values.AtoZ()
                                   .Select(Partition.ByValue)
                                   .ToArray();

            var distributor = partitions.CreateInMemoryDistributor(
                maxDegreesOfParallelism: 30,
                defaultLeaseDuration: 5.Seconds())
                                        .Trace();

            var catchup = streams.CreateDistributedCatchup(distributor)
                                 .Backoff(5.Seconds());

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
    }
}