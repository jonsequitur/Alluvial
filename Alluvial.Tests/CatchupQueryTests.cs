using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Alluvial.Tests.BankDomain;
using FluentAssertions;
using NEventStore;
using NUnit.Framework;

namespace Alluvial.Tests
{
    [TestFixture]
    public class CatchupQueryTests
    {
        private IStoreEvents store;
        private string[] streamIds;
        private IDataStreamSource<string, IDomainEvent> streamStore;
        private IDataStream<IDataStream<IDomainEvent>> streams;

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

            streamStore = DataStreamSource.Create<string, IDomainEvent>(
                id => new NEventStoreDataStream(store, id)
                          .Map(es => es.Select(e => e.Body).OfType<IDomainEvent>()));

            streams = DataStream.Create<NEventStoreStreamUpdate>(
                query: q => store.Advanced
                                 .GetFrom(q.Cursor.As<string>())
                                 .Select(c => new NEventStoreStreamUpdate
                                 {
                                     StreamId = c.StreamId,
                                     CheckpointToken = c.CheckpointToken
                                 })
                                 .Take(q.BatchCount ?? int.MaxValue),
                advanceCursor: (query, batch) =>
                {
                    var last = batch.LastOrDefault();
                    if (last != null)
                    {
                        query.Cursor.AdvanceTo(last.CheckpointToken);
                    }
                })
                                .Requery(k => streamStore.Open(k.StreamId));
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

            await Catchup.Create(streams)
                         .Subscribe(new BalanceProjector(), projectionStore)
                         .RunSingleBatch();

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
            var barrier = new Barrier(2);

            var catchup = Catchup.Create(streams, batchCount: 1)
                                 .Subscribe(new BalanceProjector()
                                                .After((projection, events) => barrier.SignalAndWait(1000)), projectionStore);

            catchup.RunSingleBatch();
            catchup.RunSingleBatch();

            barrier.SignalAndWait(1000);

            Thread.Sleep(10);

            projectionStore.Count()
                           .Should()
                           .Be(1);
        }

        [Test]
        public async Task Catchup_outer_batch_size_can_be_specified()
        {
            var projectionStore = new InMemoryProjectionStore<BalanceProjection>();

            var catchup = Catchup.Create(streams, batchCount: 20)
                                 .Subscribe(new BalanceProjector(), projectionStore);

            await catchup.RunSingleBatch();

            projectionStore.Count()
                           .Should()
                           .Be(20);
        }

        [Test]
        public async Task Catchup_cursor_can_be_specified()
        {
            var catchup = Catchup.Create(streams, batchCount: 500)
                                 .Subscribe(new BalanceProjector(), new InMemoryProjectionStore<BalanceProjection>());

            var query = await catchup.RunSingleBatch();

            var projectionStore = new InMemoryProjectionStore<BalanceProjection>();
            catchup = Catchup.Create(streams, query.Cursor)
                             .Subscribe(new BalanceProjector(), projectionStore);

            await catchup.RunSingleBatch();

            projectionStore.Count()
                           .Should()
                           .Be(500);
        }

        [Test]
        public async Task Catchup_query_cursor_resumes_from_last_position()
        {
            var projectionStore = new InMemoryProjectionStore<BalanceProjection>();

            var catchup = Catchup.Create(streams, batchCount: 500)
                                 .Subscribe(new BalanceProjector(), projectionStore);

            await catchup.RunSingleBatch();
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
        public async Task Catchup_RunUntilCaughtUp_runs_until_the_stream_has_no_more_results()
        {
            var projectionStore = new InMemoryProjectionStore<BalanceProjection>();

            var catchup = Catchup.Create(streams, batchCount: 10)
                                 .Subscribe(new BalanceProjector(), projectionStore);

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
        public async Task Catchup_incrementally_updates_projections()
        {
            var projectionStore = new InMemoryProjectionStore<BalanceProjection>();

            var catchup = Catchup.Create(streams, batchCount: 1000)
                                 .Subscribe(new BalanceProjector(), projectionStore);

            TaskScheduler.UnobservedTaskException += (sender, args) => Console.WriteLine(args.Exception);

            await catchup.RunUntilCaughtUp();

            var streamId = streamIds.First();

            WriteEvent(streamId, 100m);

            var query = await catchup.RunUntilCaughtUp();

            query.Cursor
                 .As<string>()
                 .Should()
                 .Be("1001");

            var balanceProjection = await projectionStore.Get(streamId);

            balanceProjection.Balance.Should().Be(101);
            balanceProjection.CursorPosition.Should().Be(2);
        }
    }
}