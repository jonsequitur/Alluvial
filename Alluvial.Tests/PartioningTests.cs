using System;
using FluentAssertions;
using NUnit.Framework;

namespace Alluvial.Tests
{
    [TestFixture]
    public class PartioningTests
    {
        [Test]
        public void Single_value_range_partitions_are_not_allowed()
        {
            Action createPartition = () => Partition.ByRange(1, 1);

            createPartition.ShouldThrow<ArgumentException>()
                .And
                .Message
                .Should()
                .Be("The lower bound (1) must be less than the upper bound (1).");
        }

        [Test]
        public void Range_partitions_cannot_have_their_upper_bound_less_than_their_lower_bound()
        {
            Action createPartition = () => Partition.ByRange(2, 1);

            createPartition.ShouldThrow<ArgumentException>()
                .And
                .Message
                .Should()
                .Be("The lower bound (2) must be less than the upper bound (1).");
        }
    }
}