using System;
using System.Collections.Generic;
using FluentAssertions;
using System.Linq;
using System.Threading.Tasks;
using Its.Log.Instrumentation;
using NUnit.Framework;

namespace Alluvial.Tests
{
    [TestFixture]
    public class StreamQueryDistributionTests
    {
        private List<string> words;
        private IPartitionedStream<string, int, string> partitionedStream;
        private IEnumerable<IStreamQueryPartition<string>> partitions;

        [SetUp]
        public void SetUp()
        {
            words = Values.AtoZ().SelectMany(c => Enumerable.Range(1, 100).Select(i => c + i)).ToList();

            partitions = Values.AtoZ().Select(c => Partition.Where<string>(s => s.StartsWith(c), named: c));

            partitionedStream = Stream
                .Partitioned<string, int, string>(
                    query: async (q, partition) =>
                    {
                        var wordsInPartition = words
                            .Skip(q.Cursor.Position)
                            .Where(partition.Contains);

                        var b = wordsInPartition
                            .Take(q.BatchSize.Value);

                        return b;
                    },
                    advanceCursor: (query, batch) =>
                    {
                        var last = batch.LastOrDefault();
                        if (last != null)
                        {
                            query.Cursor.AdvanceTo(words.IndexOf(last) + 1);
                        }
                    });

            Formatter.ListExpansionLimit = 100;
            Formatter<Projection<HashSet<int>, int>>.RegisterForAllMembers();
        }

        [Test]
        public async Task Competing_catchups_can_lease_a_partition_using_a_distributor_catchup()
        {
            var store = new InMemoryProjectionStore<Projection<HashSet<string>, int>>();

            var aggregator = Aggregator.Create<Projection<HashSet<string>, int>, string>((p, xs) =>
            {
                if (p.Value == null)
                {
                    p.Value = new HashSet<string>();
                }

                foreach (var x in xs)
                {
                    p.Value.Add(x);
                }
            }).Trace();

            var catchup = partitionedStream.Trace()
                                           .CreateDistributedCatchup(batchSize: 15)
                                           .DistributeInMemoryAmong(partitions);

            catchup.Subscribe(aggregator, store.Trace());

            await catchup.RunUntilCaughtUp();

            partitions.ToList()
                      .ForEach(partition =>
                                   store.Should()
                                        .ContainSingle(projection =>
                                                           projection.Value.Count == 100 &&
                                                           projection.Value.All(p => p.IsWithinPartition(partition))));
        }

        [Test]
        public async Task Distributed_single_stream_catchups_can_store_a_cursor_per_partition()
        {
            var cursorStore = new InMemoryProjectionStore<ICursor<int>>(_ => Cursor.New<int>());

            var aggregator = Aggregator.Create<Projection<HashSet<string>, int>, string>((p, xs) =>
            {
                if (p.Value == null)
                {
                    p.Value = new HashSet<string>();
                }

                foreach (var x in xs)
                {
                    p.Value.Add(x);
                }
            }).Trace();

            var catchup = partitionedStream
                .CreateDistributedCatchup(
                    batchSize: 73,
                    fetchAndSavePartitionCursor: cursorStore.Trace().AsHandler())
                .DistributeInMemoryAmong(partitions);

            catchup.Subscribe(aggregator);

            await catchup.RunUntilCaughtUp();

            cursorStore.Count().Should().Be(26);
            Enumerable.Range(1, 26).ToList().ForEach(i => { cursorStore.Should().Contain(c => c.Position == i*100); });
        }

        [Test]
        public async Task Distributed_multi_stream_catchups_can_store_a_cursor_per_partition()
        {
            var cursorStore = new InMemoryProjectionStore<ICursor<int>>(_ => Cursor.New<int>());

            var aggregator = Aggregator.Create<Projection<HashSet<string>, int>, string>((p, xs) =>
            {
                if (p.Value == null)
                {
                    p.Value = new HashSet<string>();
                }

                foreach (var x in xs)
                {
                    p.Value.Add(x);
                }
            }).Trace();

            var catchup = partitionedStream
                .IntoMany(async (word, b, c, p) => new[] { word }.AsStream())
                .CreateDistributedCatchup(batchSize: 10,
                                          fetchAndSavePartitionCursor: cursorStore.Trace().AsHandler())
                .DistributeInMemoryAmong(new[]
                {
                    Partition.ByRange("a", "f"),
                    Partition.ByRange("f", "j"),
                    Partition.ByRange("k", "z")
                });

            catchup.Subscribe(aggregator);

            await catchup.RunSingleBatch();

            cursorStore.Count().Should().Be(3);
        }

        [Test]
        public async Task When_a_distributed_catchup_has_not_been_subscribed_to_a_distributor_then_RunSingleBatch_throws()
        {
            var catchup = partitionedStream.CreateDistributedCatchup();

            Action runSingleBatch = () => catchup.RunSingleBatch().Wait();

            runSingleBatch.ShouldThrow<InvalidOperationException>()
                .And
                .Message
                .Should()
                .Contain("You must subscribe the catchup to a distributor before calling RunSingleBatch.");
        }
    }
}