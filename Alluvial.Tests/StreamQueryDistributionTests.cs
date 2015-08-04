using System;
using System.Collections.Generic;
using FluentAssertions;
using Its.Log.Instrumentation;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;

namespace Alluvial.Tests
{
    [Ignore("Test not finished")]
    [TestFixture]
    public class StreamQueryDistributionTests
    {
        private List<string> words;
        private IPartitionedStream<string, int, string> partitionedStream;
        private IEnumerable<IStreamQueryPartition<string>> partitions;

        [SetUp]
        public void SetUp()
        {
            words = AtoZ().SelectMany(c => Enumerable.Range(1, 100).Select(i => c + i)).ToList();

            partitions = AtoZ().Select(c => Partition.Where<string>(s => s.StartsWith(c), named: c));

            partitionedStream = Stream
                .PartitionByRanges<string, int, string>(
                    query: async (q, partition) =>
                    {
                        var wordsInPartition = words
                            .Skip(q.Cursor.Position)
                            .Where(partition.Contains);

                        var b = wordsInPartition
                            .Take(q.BatchSize.Value);

                        return b;
                    },
                    advanceCursor: (query, batch) => { query.Cursor.AdvanceTo(words.IndexOf(batch.Last()) + 1); });

            Formatter.ListExpansionLimit = 100;
            Formatter<Projection<HashSet<int>, int>>.RegisterForAllMembers();
        }

        private IEnumerable<string> AtoZ()
        {
            for (var c = 'a'; c <= 'z'; c++)
            {
                yield return new string(c, 1);
            }
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

            //            // set up 10 competing catchups
            //            for (var i = 0; i < 10; i++)
            //            {
            //                var distributor = partitions.DistributeQueriesInProcess().Trace();
            //
            //                distributor.OnReceive(async lease =>
            //                {
            //                    var catchup = StreamCatchup.Create(await partitionedStream.GetStream(lease.Resource));
            //                    catchup.Subscribe(aggregator, store.Trace());
            //                    await catchup.RunSingleBatch();
            //                });
            //
            //                distributor.Start();
            //
            //                disposables.Add(distributor);
            //            }

            var catchup = partitionedStream
                .Trace()
                .DistributeAmong(partitions, batchSize: 15);

            catchup.Subscribe(aggregator, store.Trace());

            await catchup.RunUntilCaughtUp();

            Console.WriteLine(new { store }.ToLogString());

            partitions.ToList()
                      .ForEach(partition =>
                                   store.Should()
                                        .ContainSingle(projection =>
                                                           projection.Value.Count() == 100 &&
                                                           projection.Value.All(p => p.IsWithinPartition(partition))));
        }

        [Ignore("Test not finished")]
        [Test]
        public async Task Distributed_catchups_can_store_a_cursor_per_partition()
        {
            var store = new InMemoryProjectionStore<Projection<HashSet<string>, IStreamQueryPartition<string>>>();

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

            var catchup = partitionedStream.DistributeAmong(partitions, batchSize: 15);

            catchup.Subscribe(aggregator);

            await catchup.RunUntilCaughtUp();

            Console.WriteLine(new { store }.ToLogString());

            partitions.ToList()
                      .ForEach(partition =>
                                   store.Should()
                                        .ContainSingle(projection =>
                                                           projection.Value.Count() == 100 &&
                                                           projection.Value.All(p => p.IsWithinPartition(partition))));
        }
    }
}