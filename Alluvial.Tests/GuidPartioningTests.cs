using System;
using System.Collections.Generic;
using FluentAssertions;
using System.Linq;
using System.Numerics;
using System.Threading.Tasks;
using Its.Log.Instrumentation;
using NUnit.Framework;

namespace Alluvial.Tests
{
    public class GuidPartioningTests
    {
        private readonly Guid[] sqlServerSortedGuids =
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
        public async Task GuidConverter_can_round_trip_random_guids_correctly()
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
        public async Task GuidConverter_can_round_trip_interesting_guids_correctly()
        {
            sqlServerSortedGuids
                .ToList()
                .ForEach(TestGuidRoundTrip);

            sqlServerSortedGuids
                .Select(g => Guid.Parse(g.ToString().Replace("01", "ff")))
                .ToList()
                .ForEach(TestGuidRoundTrip);

            sqlServerSortedGuids
                .Select(g => Guid.Parse(g.ToString().Replace("00", "ff")))
                .ToList()
                .ForEach(TestGuidRoundTrip);
        }

        [Test]
        public async Task GuidConverter_can_round_trip_max_guid_correctly()
        {
            var maxGuid = Guid.Parse("ffffffff-ffff-ffff-ffff-ffffffffffff");
            TestGuidRoundTrip(maxGuid);
        }

        [Test]
        public async Task GuidConverter_can_round_trip_empty_guid_correctly()
        {
            TestGuidRoundTrip(Guid.Empty);
        }

        [Test]
        public async Task OrderBySqlServer_sort_order_matches_SQL_Server_sort_order()
        {
            var sqlGuidsRandomlyReordered = sqlServerSortedGuids
                .OrderBy(_ => Guid.NewGuid());

            sqlGuidsRandomlyReordered.OrderBySqlServer()
                                     .Should()
                                     .ContainInOrder(sqlServerSortedGuids);

            sqlGuidsRandomlyReordered.OrderBySqlServer()
                                     .Select(g => g.ToBigInteger())
                                     .Should()
                                     .BeInAscendingOrder();
        }

        [Test]
        public async Task OrderBySqlServer_sort_order_converted_to_integers_is_ascending()
        {
            var sqlGuidsRandomlyReordered = sqlServerSortedGuids
                .OrderBy(_ => Guid.NewGuid());

            sqlGuidsRandomlyReordered.OrderBySqlServer()
                                     .Select(g => g.ToBigInteger())
                                     .Should()
                                     .BeInAscendingOrder();
        }

        [Test]
        public async Task BigInteger_representation_should_preserve_SQL_sort_order()
        {
            sqlServerSortedGuids.Select(g => g.ToBigInteger())
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
        public async Task Sortify_and_Unsortify_are_complementary()
        {
            var bigInts = new[]
            {
                new BigInteger(0),
                new BigInteger(1),
                new BigInteger(1000),
                new BigInteger(-1),
                new BigInteger(-1000),
                SqlGuidPartitioner.MaxSigned128BitBigInt - 1,
                SqlGuidPartitioner.MaxSigned128BitBigInt
            };

            foreach (var bigInteger in bigInts)
            {
                bigInteger.Sortify().Unsortify().Should().Be(bigInteger);
            }
        }

        [Test]
        public async Task Unsortify_shifts_max_guid_to_negative_1()
        {
            var maxGuid = Guid.Parse("ffffffff-ffff-ffff-ffff-ffffffffffff");
            maxGuid.ToBigInteger().Unsortify().Should().Be(-1);
        }

        [Test]
        public async Task Sortify_treats_negative_1_as_the_max_unsigned_value()
        {
            var bigInteger = new BigInteger(-1)
                .Sortify();

            bigInteger
                .Should()
                .Be(SqlGuidPartitioner.MaxUnsigned128BitBigInt);
        }



        [Test]
        public async Task Max_unsigned_integer_guidifies_as_max_guid()
        {
            SqlGuidPartitioner.MaxUnsigned128BitBigInt
                              .ToGuid()
                              .Should()
                              .Be(Guid.Parse("ffffffff-ffff-ffff-ffff-ffffffffffff"));
        }

        [Test]
        public async Task Sortify_treats_negative_max_unsigned_as_the_max_signed_value_plus_1()
        {
            (new BigInteger(0) - SqlGuidPartitioner.MaxSigned128BitBigInt)
                .Sortify()
                .Should()
                .Be(SqlGuidPartitioner.MaxSigned128BitBigInt + 1);
        }

        [Test]
        public async Task Guid_partitions_as_integers_are_gapless()
        {
            int numberOfPartitions = 1000;

            var partitions = StreamQuery.Partition(
                Guid.Empty,
                Guid.Parse("ffffffff-ffff-ffff-ffff-ffffffffffff"))
                                        .Among(numberOfPartitions)
                                        .ToArray();

            for (int i = 0; i < numberOfPartitions - 1; i++)
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