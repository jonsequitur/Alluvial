using System;
using System.Collections.Concurrent;
using FluentAssertions;
using System.Linq;
using System.Threading.Tasks;
using Alluvial.Tests.BankDomain;
using NEventStore;
using NUnit.Framework;

namespace Alluvial.Tests
{
    [TestFixture]
    public class StreamMapTests
    {
        private string streamId;
        private IStoreEvents store;
        private IStream<EventMessage, int> stream;

        [SetUp]
        public void SetUp()
        {
            streamId = Guid.NewGuid().ToString();
            store = TestEventStore.Create();
            store.Populate(streamId);
            stream = NEventStoreStream.ByAggregate(store, streamId);
        }

        [Test]
        public async Task A_stream_can_be_mapped_at_query_time()
        {
            var domainEvents = stream.Map(es => es.Select(e => e.Body).OfType<IDomainEvent>());

            var query = domainEvents.CreateQuery();

            var batch = await domainEvents.Fetch(query);

            batch.Count()
                 .Should()
                 .Be(4);
        }

        [Test]
        public async Task A_partitioned_stream_can_be_mapped_at_query_time()
        {
            for (var i = 1; i <= 9; i++)
            {
                store.WriteEvents(
                    streamId: Guid.NewGuid().ToString(),
                    howMany: 10,
                    bucketId: i.ToString());
            }

            var partitionedStream = Stream.Partitioned<EventMessage, int, string>(async (q, p) =>
            {
                await Task.Delay(10); // work around for possible NEventStore consistency lag
                var bucketId = ((IStreamQueryValuePartition<string>) p).Value;
                var streamsToSnapshot = store.Advanced.GetStreamsToSnapshot(bucketId, 0);
                var streamId = streamsToSnapshot.Select(s => s.StreamId).Single();
                var stream = NEventStoreStream.ByAggregate(store, streamId, bucketId);
                var batch = await stream.CreateQuery(q.Cursor, q.BatchSize).NextBatch();
                return batch;
            }).Trace();

            var domainEvents = partitionedStream.Map(es => es.Select(e => e.Body).OfType<IDomainEvent>());

            // catch up
            var catchup = domainEvents.CreateDistributedCatchup(batchSize: 2)
                                      .DistributeInMemoryAmong(
                                          Enumerable.Range(1, 10)
                                                    .Select(i => Partition.ByValue(i.ToString())));

            var receivedEvents = new ConcurrentBag<IDomainEvent>();

            catchup.Subscribe(async b =>
            {
                foreach (var e in b)
                {
                    receivedEvents.Add(e);
                }
            });

            await catchup.RunUntilCaughtUp().Timeout();

            receivedEvents.Count().Should().Be(90);
        }

        [Test]
        public async Task A_mapped_data_stream_can_be_traversed_using_the_outer_query_cursor()
        {
            using (var nEventStoreStream = store.OpenStream(streamId))
            {
                for (var i = 0; i < 5; i++)
                {
                    nEventStoreStream.Add(new EventMessage
                    {
                        Body = new FundsWithdrawn
                        {
                            AggregateId = streamId,
                            Amount = 1m
                        }
                    });
                }
                nEventStoreStream.CommitChanges(Guid.NewGuid());
            }

            var domainEvents = stream.Map(es => es.Select(e => e.Body)
                                                  .OfType<FundsWithdrawn>());

            var query = domainEvents.CreateQuery();

            var batch = await query.NextBatch();

            batch.Count()
                 .Should()
                 .Be(5);
            query.Cursor
                 .Position
                 .Should()
                 .Be(9);
        }
    }
}