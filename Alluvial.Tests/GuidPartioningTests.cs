using System;
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
            Enumerable.Range(1, 100000)
                      .ToList()
                      .ForEach(_ =>
                      {
                          Guid guid = Guid.NewGuid();
                          BigInteger bigInteger;
                          bigInteger = guid.ToBigInteger();
                          try
                          {
                              bigInteger.ToGuid().Should().Be(guid);
                          }
                          catch (Exception exception)
                          {
                              Console.WriteLine(exception);
                              Console.WriteLine(new
                              {
                                  bigInteger,
                                  diff = bigInteger - SqlGuidPartitioner.Max128BitBigInt
                              });
                              throw;
                          }
                      });
        }

        [Test]
        public async Task GuidConverter_can_round_trip_interesting_guids_correctly()
        {
            sqlServerSortedGuids
                .Reverse()
                .ToList()
                .ForEach(TestGuid);

            sqlServerSortedGuids
                .Select(g => Guid.Parse(g.ToString().Replace("01", "ff")))
                .Reverse()
                .ToList()
                .ForEach(TestGuid);

            sqlServerSortedGuids
                .Select(g => Guid.Parse(g.ToString().Replace("00", "ff")))
                .Reverse()
                .ToList()
                .ForEach(TestGuid);

            TestGuid(Guid.Parse("ffffffff-ffff-ffff-ffff-ffffffffffff"));

            TestGuid(Guid.Empty);
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

        private static void TestGuid(Guid guid)
        {
            Console.WriteLine(guid);
            guid.ToBigInteger().ToGuid().Should().Be(guid);
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
            };

            foreach (var bigInteger in bigInts)
            {
                bigInteger.Sortify().Unsortify().Should().Be(bigInteger);
            }
        }

        [Test]
        public async Task Find_max_guid()
        {
            Console.WriteLine(SqlGuidPartitioner.Max128BitBigInt.ToGuid());

            var big = SqlGuidPartitioner.Max128BitBigInt + 1;

            Console.WriteLine(big.ToGuid());

            // FIX (Find_max_guid) write test
            Assert.Fail("Test not written yet.");
        }

        [Test]
        public async Task find_max_int()
        {
            var guids = new[]
            {
                Guid.Empty,
                Guid.Parse("ffffffff-ffff-ffff-ffff-ffffffffffff"),
                Guid.Parse("00ffffff-ffff-ffff-ffff-ffffffffffff"),
                Guid.Parse("ffffffff-ffff-ffff-ffff-ffffffffff00"),
                Guid.Parse("ffffffff-ffff-ffff-ffff-fffffffffff0"),
                Guid.Parse("ffffffff-ffff-ffff-ffff-ffffffffff01"),
                Guid.Parse("ffffffff-ffff-ffff-ffff-ffffffffff0f"),
                Guid.Parse("ffffffff-ffff-ffff-ffff-fffffffffffe"),
                Guid.Parse("00000000-ffff-ffff-ffff-ffffffffffff"),
                Guid.Parse("ffffffff-0000-ffff-ffff-ffffffffffff"),
                Guid.Parse("ffffffff-ffff-0000-ffff-ffffffffffff"),
                Guid.Parse("ffffffff-ffff-ffff-0000-ffffffffffff"),
                Guid.Parse("ffffffff-ffff-ffff-ffff-000000000000")
            }.OrderBySqlServer();

            var ints = guids
                .Select(g => g.ToBigInteger())
                .ToArray();

            foreach (var bigInt in ints)
            {
                Console.WriteLine(new { bigInt, guid = bigInt.ToGuid() });
            }

            var start = BigInteger.Parse("308276084001730439550074881");
            var realBig = BigInteger.Pow(2, 127) - 1;

            Console.WriteLine("long.MaxValue = " + long.MaxValue);

            Console.WriteLine(new
            {
                realBig,
                bytes = realBig.ToByteArray().Length
            }
                );

            var guid = Guid.Empty;

            for (var i = 0; i < 100; i++)
            {
                var value = (start + i);

                try
                {
                    guid = value.ToGuid();
                    Console.WriteLine(new { value, guid });
                }
                catch (Exception exception)
                {
                    Console.WriteLine(exception);
                    Console.WriteLine(new { value, guid });
                }
            }
        }
    }
}