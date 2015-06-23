using System;
using FluentAssertions;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;

namespace Alluvial.Tests
{
    [TestFixture]
    public class StreamQueryPartitioningTests
    {
        private int[] ints;
        private IStreamPartitionGroup<int, int, int> partitions;

        [SetUp]
        public void SetUp()
        {
            ints = Enumerable.Range(1, 1000).ToArray();

            partitions = Stream
                .Partition<int, int, int>(async (q, p) => ints
                                              .Where(i => i >= p.LowerBoundInclusive &&
                                                          i < p.UpperBoundExclusive)
                                              .Skip(q.Cursor.Position)
                                              .Take(q.BatchCount.Value));
        }

        [Test]
        public async Task A_stream_can_be_partitioned_through_query_parameterization()
        {
            var partition = StreamQuery.Partition(1, 100);
            var stream = await partitions.GetStream(partition);

            var aggregator = Aggregator.CreateFor<int, int>((p, i) => p.Value += i.Sum());

            var projection = await stream.Aggregate(aggregator, new Projection<int, int>());

            projection.Value
                      .Should()
                      .Be(Enumerable.Range(1, 99).Sum());
        }

        [Ignore("Test not finished")]
        [Test]
        public async Task When_a_partion_is_queried_then_the_cursor_is_updated()
        {






            // FIX (testname) write test
            Assert.Fail("Test not written yet.");
        }
    }
}