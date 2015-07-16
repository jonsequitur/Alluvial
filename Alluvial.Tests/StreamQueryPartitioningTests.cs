using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using FluentAssertions;
using System.Linq;
using System.Reactive.Disposables;
using System.Threading.Tasks;
using Alluvial.Distributors;
using NUnit.Framework;

namespace Alluvial.Tests
{
    [TestFixture]
    public class StreamQueryPartitioningTests
    {
        private int[] ints;
        private IStreamQueryPartitioner<int, int, int> partitioner;
        private CompositeDisposable disposables;

        [SetUp]
        public void SetUp()
        {
            ints = Enumerable.Range(1, 1000).ToArray();
            disposables = new CompositeDisposable();
            partitioner = Stream
                .Partition<int, int, int>(async (q, p) => ints
                                              .Where(i => i > p.LowerBoundExclusive &&
                                                          i <= p.UpperBoundInclusive)
                                              .Skip(q.Cursor.Position)
                                              .Take(q.BatchCount.Value),
                                          advanceCursor: (query, batch) => { query.Cursor.AdvanceTo(batch.Last()); });
        }

        [TearDown]
        public void TearDown()
        {
            disposables.Dispose();
        }

        [Test]
        public async Task A_stream_can_be_partitioned_through_query_parameterization()
        {
            var partition = StreamQuery.Partition(0, 100);
            var stream = await partitioner.GetStream(partition);

            var aggregator = Aggregator.CreateFor<int, int>((p, i) => p.Value += i.Sum());

            var projection = await stream.Aggregate(aggregator, new Projection<int, int>());

            projection.Value
                      .Should()
                      .Be(Enumerable.Range(1, 100).Sum());
        }

        [Test]
        public async Task When_a_partition_is_queried_then_the_cursor_is_updated()
        {
            var partitions = new[]
            {
                StreamQuery.Partition(0, 500),
                StreamQuery.Partition(500, 1000)
            };

            var store = new InMemoryProjectionStore<Projection<int, int>>();
            var aggregator = Aggregator.CreateFor<int, int>((p, i) => p.Value += i.Sum());

            await Task.WhenAll(partitions.Select(async partition =>
            {
                var stream = await partitioner.GetStream(partition);

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
        public async Task Competing_catchups_can_lease_a_partition_using_a_distributor()
        {
            var partitions = Enumerable.Range(0, 9)
                                       .Select(i => StreamQuery.Partition(i*100, (i + 1)*100))
                                       .ToArray();

            var store = new InMemoryProjectionStore<Projection<HashSet<int>, int>>();

            var aggregator = Aggregator.CreateFor<HashSet<int>, int>((p, xs) =>
            {
                if (p.Value == null)
                {
                    p.Value = new HashSet<int>();
                }

                foreach (var x in xs)
                {
                    p.Value.Add(x);
                }
            }).Trace();

            // set up 10 competing catchups
            for (var i = 0; i < 10; i++)
            {
                var distributor = new InMemoryStreamQueryDistributor(
                    partitions.Select(p => new LeasableResource(p.ToString(), TimeSpan.FromSeconds(10))).ToArray(), "")
                    .Trace();

                distributor.OnReceive(async lease =>
                {
                    var partition = partitions.Single(p => p.ToString() == lease.LeasableResource.Name);
                    var catchup = StreamCatchup.Create(await partitioner.GetStream(partition));
                    catchup.Subscribe(aggregator, store.Trace());
                    await catchup.RunSingleBatch();
                });

                distributor.Start();

                disposables.Add(distributor);
            }

            partitions.ToList()
                      .ForEach(partition =>
                                   store.Should()
                                        .ContainSingle(p =>
                                                           p.Value.Count() == 100 &&
                                                           p.Value.OrderBy(i => i).Last() == partition.UpperBoundInclusive));
        }

        [Test]
        public async Task GuidQueryPartitioner_partitions_guids_fairly()
        {
            var totalNumberOfGuids = 1000;
            var numberOfPartitions = 50;

            var partitions = StreamQuery.Partition(
                Guid.Empty,
                Guid.Parse("ffffffff-ffff-ffff-ffff-ffffffffffff"))
                                        .Among(numberOfPartitions);

            var guids = Enumerable.Range(1, totalNumberOfGuids).Select(_ => Guid.NewGuid()).ToArray();

            var partitioner = Stream.Partition<Guid, int, Guid>(
                async (q, p) =>
                    guids.Where(g => new SqlGuid(g).CompareTo(new SqlGuid(p.LowerBoundExclusive)) > 0 &&
                                     new SqlGuid(g).CompareTo(new SqlGuid(p.UpperBoundInclusive)) <= 0),
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
                var stream = await partitioner.GetStream(partition);

                var catchup = StreamCatchup.Create(stream, batchCount: int.MaxValue);
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
            var partitions = StreamQuery.Partition(
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
        public async Task When_even_partitions_are_not_possible_among_int_partitions_then_Among_creates_gapless_and_non_overlapping_partitions()
        {
            var partitions = StreamQuery.Partition(
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
    }
}