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
        [Test]
        public async Task A_stream_can_be_partitioned_through_query_parameterization()
        {
            var ints = Enumerable.Range(1, 1000).ToArray();

            var partitions = Stream
                .Partition<int, int, int>(async (q, p) => ints
                                              .Where(i => i >= p.LowerBoundInclusive &&
                                                          i < p.UpperBoundExclusive)
                                              .Skip(q.Cursor.Position)
                                              .Take(q.BatchCount.Value));

            var stream = await partitions.GetStream(StreamQuery.Partition(1, 100));

            var projection = await stream.Aggregate(Aggregator.CreateFor<int, int>((p, i) => p.Value += i.Sum()), new Projection<int, int>());

            Console.WriteLine(projection);

            projection.Value.Should().Be(Enumerable.Range(1, 99).Sum());
        }
    }
}