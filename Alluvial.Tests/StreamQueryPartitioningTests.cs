using System;
using System.Collections.Generic;
using FluentAssertions;
using Its.Log.Instrumentation;
using System.Linq;
using System.Reactive.Disposables;
using System.Threading.Tasks;
using NUnit.Framework;

namespace Alluvial.Tests
{
    [TestFixture]
    public class StreamQueryPartitioningTests
    {
        private int[] ints;
        private IPartitionedStream<int, int, int> partitionedStream;
        private CompositeDisposable disposables;

        [SetUp]
        public void SetUp()
        {
            ints = Enumerable.Range(1, 1000).ToArray();
            disposables = new CompositeDisposable();
            partitionedStream = Stream
                .Partitioned<int, int, int>(
                    query: async (q, p) =>
                    {
                        return ints
                            .Where(i => i.IsWithinPartition(p))
                            .Skip(q.Cursor.Position)
                            .Take(q.BatchSize.Value);
                    },
                    advanceCursor: (query, batch) =>
                    {
                        // putting the cursor and the partition on the same field is a little weird because a batch of zero doesn't necessarily signify the end of the batch
                        if (batch.Any())
                        {
                            query.Cursor.AdvanceTo(batch.Last());
                        }
                    });

            Formatter.ListExpansionLimit = 100;
            Formatter<Projection<HashSet<int>, int>>.RegisterForAllMembers();
        }

        [TearDown]
        public void TearDown()
        {
            disposables.Dispose();
        }

        [Test]
        public async Task A_stream_can_be_partitioned_through_query_parameterization()
        {
            var partition = Partition.ByRange(0, 100);
            var stream = await partitionedStream.GetStream(partition);

            var aggregator = Aggregator.Create<Projection<int, int>, int>((p, i) => p.Value += i.Sum());

            var projection = await stream.Aggregate(aggregator, new Projection<int, int>());

            projection.Value
                      .Should()
                      .Be(Enumerable.Range(1, 100).Sum());
        }

        [Test]
        public async Task When_a_partition_is_queried_then_the_cursor_is_updated()
        {
            var partitions = Partition.ByRange(0, 1000).Among(2);

            var store = new InMemoryProjectionStore<Projection<int, int>>();
            var aggregator = Aggregator.Create<Projection<int, int>, int>((p, i) => p.Value += i.Sum());

            await Task.WhenAll(partitions.Select(async partition =>
            {
                var stream = await partitionedStream.GetStream(partition);

                Console.WriteLine(stream);

                var catchup = StreamCatchup.Create(stream);
                catchup.Subscribe(aggregator, store);
                await catchup.RunSingleBatch();
            }));

            store.Should()
                 .ContainSingle(p => p.CursorPosition == 500)
                 .And
                 .ContainSingle(p => p.CursorPosition == 1000);
        }

        [Test]
        public async Task GuidQueryPartitioner_partitions_guids_fairly()
        {
            var totalNumberOfGuids = 1000;
            var numberOfPartitions = 50;

            var partitions = Partition.AllGuids()
                                      .Among(numberOfPartitions);

            var guids = Enumerable.Range(1, totalNumberOfGuids).Select(_ => Guid.NewGuid()).ToArray();

            var partitioned = Stream.Partitioned<Guid, int, Guid>(
                async (q, p) =>
                    guids.Where(g => g.IsWithinPartition(p)),
                advanceCursor: (q, b) => q.Cursor.AdvanceTo(totalNumberOfGuids));

            var aggregator = Aggregator.Create<Projection<HashSet<Guid>, int>, Guid>((p, b) =>
            {
                if (p.Value == null)
                {
                    p.Value = new HashSet<Guid>();
                }
                foreach (var guid in b)
                {
                    p.Value.Add(guid);
                }
            });
            var store = new InMemoryProjectionStore<Projection<HashSet<Guid>, int>>();

            await Task.WhenAll(partitions.Select(async partition =>
            {
                var stream = await partitioned.GetStream(partition);

                var catchup = StreamCatchup.Create(stream, batchSize: int.MaxValue);
                catchup.Subscribe(aggregator, store);
                await catchup.RunSingleBatch();

                var projection = await store.Get(stream.Id);
                Console.WriteLine(partition + ": " + projection.Value.Count);
            }));

            var approximateGuidsPerPartition = totalNumberOfGuids/numberOfPartitions;
            var tolerance = (int) (totalNumberOfGuids*.12);

            store.Sum(p => p.Value.Count).Should().Be(totalNumberOfGuids);

            store.ToList().ForEach(projection =>
            {
                projection.Value
                          .Count
                          .Should()
                          .BeInRange(approximateGuidsPerPartition - tolerance,
                                     approximateGuidsPerPartition + tolerance);
            });
        }

        [Test]
        public async Task IntPartitionBuilder_creates_the_even_partitions_when_possible()
        {
            var partitions = Partition.ByRange(
                lowerBoundExclusive: 0,
                upperBoundInclusive: 100)
                                        .Among(10)
                                        .ToArray();

            partitions[0].LowerBoundExclusive.Should().Be(0);
            partitions[0].UpperBoundInclusive.Should().Be(10);

            partitions[1].LowerBoundExclusive.Should().Be(10);
            partitions[1].UpperBoundInclusive.Should().Be(20);

            partitions[2].LowerBoundExclusive.Should().Be(20);
            partitions[2].UpperBoundInclusive.Should().Be(30);

            partitions[3].LowerBoundExclusive.Should().Be(30);
            partitions[3].UpperBoundInclusive.Should().Be(40);

            partitions[4].LowerBoundExclusive.Should().Be(40);
            partitions[4].UpperBoundInclusive.Should().Be(50);

            partitions[5].LowerBoundExclusive.Should().Be(50);
            partitions[5].UpperBoundInclusive.Should().Be(60);

            partitions[6].LowerBoundExclusive.Should().Be(60);
            partitions[6].UpperBoundInclusive.Should().Be(70);

            partitions[7].LowerBoundExclusive.Should().Be(70);
            partitions[7].UpperBoundInclusive.Should().Be(80);

            partitions[8].LowerBoundExclusive.Should().Be(80);
            partitions[8].UpperBoundInclusive.Should().Be(90);

            partitions[9].LowerBoundExclusive.Should().Be(90);
            partitions[9].UpperBoundInclusive.Should().Be(100);
        }

        [Test]
        public async Task When_even_partitions_are_not_possible_among_int_range_partitions_then_Among_creates_gapless_and_non_overlapping_partitions()
        {
            var partitions = Partition.ByRange(
                lowerBoundExclusive: 0,
                upperBoundInclusive: 11)
                                        .Among(3)
                                        .ToArray();

            partitions[0].LowerBoundExclusive.Should().Be(0);
            partitions[0].UpperBoundInclusive.Should().Be(3);

            partitions[1].LowerBoundExclusive.Should().Be(3);
            partitions[1].UpperBoundInclusive.Should().Be(6);

            partitions[2].LowerBoundExclusive.Should().Be(6);
            partitions[2].UpperBoundInclusive.Should().Be(11);
        }

        [Test]
        public async Task String_range_partitions_correctly_evaluate_values()
        {
            var AtoJ = Partition.ByRange(
                lowerBoundExclusive: "",
                upperBoundInclusive: "j");

            var KtoZ = Partition.ByRange(
                lowerBoundExclusive: "j",
                upperBoundInclusive: "z");

            "a".IsWithinPartition(AtoJ).Should().BeTrue();
            "b".IsWithinPartition(AtoJ).Should().BeTrue();
            "j".IsWithinPartition(AtoJ).Should().BeTrue();
            "k".IsWithinPartition(AtoJ).Should().BeFalse();
            "z".IsWithinPartition(AtoJ).Should().BeFalse();
            
            "a".IsWithinPartition(KtoZ).Should().BeFalse();
            "j".IsWithinPartition(KtoZ).Should().BeFalse();
            "k".IsWithinPartition(KtoZ).Should().BeTrue();
            "z".IsWithinPartition(KtoZ).Should().BeTrue();
        }

        [Test]
        public async Task String_value_partitions_correctly_evaluate_values()
        {
            var thisPartition = Partition.ByValue("this");

            "this".IsWithinPartition(thisPartition).Should().BeTrue();
            "that".IsWithinPartition(thisPartition).Should().BeFalse();
        }
    }
}