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
                                           .CreateDistributedCatchup(
                                               partitions.CreateInMemoryDistributor().AutoReleaseLeases(),
                                               batchSize: 15);

            catchup.Subscribe(aggregator, store.Trace());

            await catchup.RunUntilCaughtUp().Timeout();

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
                    partitions.CreateInMemoryDistributor().AutoReleaseLeases(),
                    batchSize: 73,
                    fetchAndSavePartitionCursor: cursorStore.Trace().AsHandler());

            catchup.Subscribe(aggregator);

            await catchup.RunUntilCaughtUp().Timeout();

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

            var distributor = new[]
            {
                Partition.ByRange("a", "f"),
                Partition.ByRange("f", "j"),
                Partition.ByRange("k", "z")
            }.CreateInMemoryDistributor();

            var catchup = partitionedStream
                .IntoMany((word, b, c, p) => new[] { word }.AsStream())
                .CreateDistributedCatchup(
                    distributor,
                    batchSize: 10,
                    fetchAndSavePartitionCursor: cursorStore.Trace().AsHandler());

            catchup.Subscribe(aggregator);

            await catchup.RunSingleBatch().Timeout();

            cursorStore.Count().Should().Be(3);
        }
    }
}