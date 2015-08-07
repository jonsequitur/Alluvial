using System;
using Alluvial.PartitionBuilders;
using FluentAssertions;
using Its.Log.Instrumentation;
using System.Linq;
using System.Numerics;
using System.Threading.Tasks;
using NUnit.Framework;

namespace Alluvial.Tests
{
    public class GuidPartioningTests
    {
        private readonly Guid[] guidsSortedLikeSqlServerSortsThem =
        {
            Guid.Parse("01000000-0000-0000-0000-000000000000"),
            Guid.Parse("10000000-0000-0000-0000-000000000000"),
            Guid.Parse("00010000-0000-0000-0000-000000000000"),
            Guid.Parse("00100000-0000-0000-0000-000000000000"),
            Guid.Parse("00000100-0000-0000-0000-000000000000"),
            Guid.Parse("00001000-0000-0000-0000-000000000000"),
            Guid.Parse("00000001-0000-0000-0000-000000000000"),
            Guid.Parse("00000010-0000-0000-0000-000000000000"),
            Guid.Parse("00000000-0100-0000-0000-000000000000"),
            Guid.Parse("00000000-1000-0000-0000-000000000000"),
            Guid.Parse("00000000-0001-0000-0000-000000000000"),
            Guid.Parse("00000000-0010-0000-0000-000000000000"),
            Guid.Parse("00000000-0000-0100-0000-000000000000"),
            Guid.Parse("00000000-0000-1000-0000-000000000000"),
            Guid.Parse("00000000-0000-0001-0000-000000000000"),
            Guid.Parse("00000000-0000-0010-0000-000000000000"),
            Guid.Parse("00000000-0000-0000-0001-000000000000"),
            Guid.Parse("00000000-0000-0000-0010-000000000000"),
            Guid.Parse("00000000-0000-0000-0100-000000000000"),
            Guid.Parse("00000000-0000-0000-1000-000000000000"),
            Guid.Parse("00000000-0000-0000-0000-000000000001"),
            Guid.Parse("00000000-0000-0000-0000-000000000010"),
            Guid.Parse("00000000-0000-0000-0000-000000000100"),
            Guid.Parse("00000000-0000-0000-0000-000000001000"),
            Guid.Parse("00000000-0000-0000-0000-000000010000"),
            Guid.Parse("00000000-0000-0000-0000-000000100000"),
            Guid.Parse("00000000-0000-0000-0000-000001000000"),
            Guid.Parse("00000000-0000-0000-0000-000010000000"),
            Guid.Parse("00000000-0000-0000-0000-000100000000"),
            Guid.Parse("00000000-0000-0000-0000-001000000000"),
            Guid.Parse("00000000-0000-0000-0000-010000000000"),
            Guid.Parse("00000000-0000-0000-0000-100000000000")
        };

        [SetUp]
        public void SetUp()
        {
            Formatter.ListExpansionLimit = 1000;
        }

        [Test]
        public async Task SqlGuidPartitioner_can_round_trip_random_guids_correctly()
        {
            var list = Enumerable.Range(1, 10000).Select(_ => Guid.NewGuid()).ToList();

            var roundTripped = list.Select(guid =>
            {
                var bigInteger = guid.ToBigInteger();
                var guidAgain = bigInteger.ToGuid();
                return guidAgain;
            }).ToList();

            var missingGuids = list.Where(guid => !roundTripped.Contains(guid)).ToList();

            Console.WriteLine(missingGuids.ToLogString());

            missingGuids.Count.Should().Be(0);
        }

        [Test]
        public async Task SqlGuidPartitioner_can_round_trip_interesting_guids_correctly()
        {
            guidsSortedLikeSqlServerSortsThem
                .ToList()
                .ForEach(TestGuidRoundTrip);

            guidsSortedLikeSqlServerSortsThem
                .Select(g => Guid.Parse(g.ToString().Replace("01", "ff")))
                .ToList()
                .ForEach(TestGuidRoundTrip);

            guidsSortedLikeSqlServerSortsThem
                .Select(g => Guid.Parse(g.ToString().Replace("00", "ff")))
                .ToList()
                .ForEach(TestGuidRoundTrip);
        }

        [Test]
        public async Task SqlGuidPartitioner_can_round_trip_max_guid_correctly()
        {
            var maxGuid = Guid.Parse("ffffffff-ffff-ffff-ffff-ffffffffffff");
            TestGuidRoundTrip(maxGuid);
        }

        [Test]
        public async Task SqlGuidPartitioner_can_round_trip_empty_guid_correctly()
        {
            TestGuidRoundTrip(Guid.Empty);
        }

        [Test]
        public async Task OrderBySqlServer_sort_order_matches_SQL_Server_sort_order()
        {
            var sqlGuidsRandomlyReordered = guidsSortedLikeSqlServerSortsThem
                .OrderBy(_ => Guid.NewGuid());

            sqlGuidsRandomlyReordered.OrderBySqlServer()
                                     .Should()
                                     .ContainInOrder(guidsSortedLikeSqlServerSortsThem);

            sqlGuidsRandomlyReordered.OrderBySqlServer()
                                     .Select(g => g.ToBigInteger())
                                     .Should()
                                     .BeInAscendingOrder();
        }

        [Test]
        public async Task OrderBySqlServer_sort_order_converted_to_integers_is_ascending()
        {
            var sqlGuidsRandomlyReordered = guidsSortedLikeSqlServerSortsThem
                .OrderBy(_ => Guid.NewGuid());

            sqlGuidsRandomlyReordered.OrderBySqlServer()
                                     .Select(g => g.ToBigInteger())
                                     .Should()
                                     .BeInAscendingOrder();
        }

        [Test]
        public async Task BigInteger_representation_should_preserve_SQL_sort_order()
        {
            guidsSortedLikeSqlServerSortsThem.Select(g => g.ToBigInteger())
                                             .Should()
                                             .BeInAscendingOrder();
        }

        [Test]
        public async Task OrderBySqlServer_sort_order_converted_to_integers_is_ascending_for_random_integers()
        {
            var random = new Random();

            var integers = Enumerable.Range(1, 1000)
                                     .Select(_ => random.Next(int.MinValue, int.MaxValue));

            integers.Select(i => new BigInteger(i).ToGuid())
                    .OrderBySqlServer()
                    .Select(g => g.ToBigInteger())
                    .Should()
                    .BeInAscendingOrder();
        }

        [Test]
        public async Task ShiftNegativeToUnsigned_and_ShiftUnsignedToNegative_are_complementary()
        {
            var bigInts = new[]
            {
                new BigInteger(0),
                new BigInteger(1),
                new BigInteger(1000),
                new BigInteger(-1),
                new BigInteger(-1000),
                SqlGuidPartitionBuilder.MaxSigned128BitBigInt - 1,
                SqlGuidPartitionBuilder.MaxSigned128BitBigInt
            };

            foreach (var bigInteger in bigInts)
            {
                bigInteger.ShiftNegativeToUnsigned()
                          .ShiftUnsignedToNegative()
                          .Should()
                          .Be(bigInteger);
            }
        }

        [Test]
        public async Task ShiftUnsignedToNegative_shifts_max_guid_to_negative_1()
        {
            var maxGuid = Guid.Parse("ffffffff-ffff-ffff-ffff-ffffffffffff");
            maxGuid.ToBigInteger().ShiftUnsignedToNegative().Should().Be(-1);
        }

        [Test]
        public async Task ShiftNegativeToUnsigned_treats_negative_1_as_the_max_unsigned_value()
        {
            var bigInteger = new BigInteger(-1)
                .ShiftNegativeToUnsigned();

            bigInteger
                .Should()
                .Be(SqlGuidPartitionBuilder.MaxUnsigned128BitBigInt);
        }

        [Test]
        public async Task Max_unsigned_integer_guidifies_as_max_guid()
        {
            SqlGuidPartitionBuilder.MaxUnsigned128BitBigInt
                                   .ToGuid()
                                   .Should()
                                   .Be(Guid.Parse("ffffffff-ffff-ffff-ffff-ffffffffffff"));
        }

        [Test]
        public async Task ShiftNegativeToUnsigned_treats_negative_max_unsigned_as_the_max_signed_value_plus_1()
        {
            (new BigInteger(0) - SqlGuidPartitionBuilder.MaxSigned128BitBigInt)
                .ShiftNegativeToUnsigned()
                .Should()
                .Be(SqlGuidPartitionBuilder.MaxSigned128BitBigInt + 1);
        }

        [Test]
        public async Task Guid_partitions_as_integers_are_gapless()
        {
            int numberOfPartitions = 1000;

            var partitions = Partition.ByRange(
                Guid.Empty,
                Guid.Parse("ffffffff-ffff-ffff-ffff-ffffffffffff"))
                                        .Among(numberOfPartitions)
                                        .ToArray();

            for (var i = 0; i < numberOfPartitions - 1; i++)
            {
                var firstPartition = partitions[i];
                var secondPartition = partitions[i + 1];

                firstPartition.LowerBoundExclusive.Should().NotBe(firstPartition.UpperBoundInclusive);

                firstPartition.UpperBoundInclusive
                              .Should()
                              .Be(secondPartition.LowerBoundExclusive);

                firstPartition.UpperBoundInclusive.ToBigInteger()
                              .Should()
                              .Be(secondPartition.LowerBoundExclusive.ToBigInteger());
            }
        }

        private static void TestGuidRoundTrip(Guid guid)
        {
            Console.WriteLine(guid);
            guid.ToBigInteger().ToGuid().Should().Be(guid);
        }
    }
}