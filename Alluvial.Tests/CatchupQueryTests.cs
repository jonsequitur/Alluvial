using System;
using System.Linq;
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
            store = TestEventStore.Create();

            streamIds = Enumerable.Range(1, 1000)
                                  .Select(_ => Guid.NewGuid().ToString())
                                  .ToArray();

            foreach (var streamId in streamIds)
            {
                using (var stream = store.OpenStream(streamId, 0))
                {
                    stream.Add(new EventMessage
                    {
                        Body = new FundsDeposited
                        {
                            AggregateId = streamId,
                            Amount = 1m
                        }
                    });

                    stream.CommitChanges(Guid.NewGuid());
                }
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
                                 }),
                advanceCursor: (query, batch) => query.Cursor.AdvanceTo(batch.Last().CheckpointToken))
                                .Requery(k => streamStore.Open(k.StreamId));
        }

        [Test]
        public async Task Catchup_can_use_a_sequence_of_keys_to_traverse_all_aggregates()
        {
            var projectionStore = new InMemoryProjectionStore<BalanceProjection>();

            Catchup.Create(streams)
                   .Subscribe(new BalanceProjector(), projectionStore)
                   .RunUntilCaughtUp()
                   .Wait(2000);

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
        public async Task Catchup_query_cursor_resumes_from_last_position()
        {
            var projectionStore = new InMemoryProjectionStore<BalanceProjection>();

            var catchup = Catchup.Create(streams)
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
        public async Task When_one_batch_is_running_a_second_call_to_RunSingleBatch_will_not_do_anything()
        {
            var projectionStore = new InMemoryProjectionStore<BalanceProjection>();

            var catchup = Catchup.Create(streams)
                                 .Subscribe(new BalanceProjector(), projectionStore);

            var tasks = new Task[]
            {
                catchup.RunSingleBatch(),
                catchup.RunSingleBatch()
            };

            await Task.WhenAll(tasks);

            projectionStore.Sum(b => b.Balance)
                           .Should()
                           .Be(1000);
            projectionStore.Select(b => b.AggregateId)
                           .Distinct()
                           .Count()
                           .Should()
                           .Be(1000);
        }
    }
}