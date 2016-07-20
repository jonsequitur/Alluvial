using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Linq;
using System.Numerics;

namespace Alluvial.PartitionBuilders
{
    internal static class SqlGuidPartitionBuilder
    {
        // this is the byte order in which SQL guids are evaluated for sorting
        private static readonly byte[] byteOrder =
        {
            // group 1
            3,
            2,
            1,
            0,
            // group 2
            5,
            4,
            // group 3
            7,
            6,
            // group 4
            9,
            8,
            // group 5
            15,
            14,
            13,
            12,
            11,
            10
        };

        // the max signed int that will fit into a guid
        internal static readonly BigInteger MaxSigned128BitBigInt;

        // the max unsigned int that will fit into a guid
        internal static readonly BigInteger MaxUnsigned128BitBigInt;

        static SqlGuidPartitionBuilder()
        {
            MaxSigned128BitBigInt = BigInteger.Parse("170141183460469231731687303715884105727");
            MaxUnsigned128BitBigInt = MaxSigned128BitBigInt*2;
        }

        public static IEnumerable<Guid> OrderBySqlServer(this IEnumerable<Guid> source) =>
            source.OrderBy(g => new SqlGuid(g));

        /// <summary>
        /// Transforms a Guid into a naturally-sortable unsigned integer according to SQL server sort order
        /// </summary>
        internal static BigInteger ToBigInteger(this Guid guid)
        {
            var bytes = guid.ToByteArray();

            var value = new BigInteger(
                new[]
                {
                    bytes[byteOrder[0]],
                    bytes[byteOrder[1]],
                    bytes[byteOrder[2]],
                    bytes[byteOrder[3]],
                    bytes[byteOrder[4]],
                    bytes[byteOrder[5]],
                    bytes[byteOrder[6]],
                    bytes[byteOrder[7]],
                    bytes[byteOrder[8]],
                    bytes[byteOrder[9]],
                    bytes[byteOrder[10]],
                    bytes[byteOrder[11]],
                    bytes[byteOrder[12]],
                    bytes[byteOrder[13]],
                    bytes[byteOrder[14]],
                    bytes[byteOrder[15]]
                });

            value = ShiftNegativeToUnsigned(value);

            return value;
        }

        /// <summary>
        /// Transforms an unsigned integer into a Guid with a corresponding sort order per SQL Server's Guid sorting algorithm.
        /// </summary>
        internal static Guid ToGuid(this BigInteger value)
        {
            var bytes = value.ShiftUnsignedToNegative().ToByteArray();

            if (bytes.Length > 16)
            {
                throw new ArgumentException($"{value} takes more than 128 bits to represent and cannot be stored in a guid");
            }

            if (value == 0)
            {
                return Guid.Empty;
            }

            var extreme = value <= MaxSigned128BitBigInt ? byte.MinValue : byte.MaxValue;

            bytes = new[]
            {
                // group 1
                bytes.Length > byteOrder[0] ? bytes[byteOrder[0]] : extreme,
                bytes.Length > byteOrder[1] ? bytes[byteOrder[1]] : extreme,
                bytes.Length > byteOrder[2] ? bytes[byteOrder[2]] : extreme,
                bytes[byteOrder[3]],

                // group 2
                bytes.Length > byteOrder[4] ? bytes[byteOrder[4]] : extreme,
                bytes.Length > byteOrder[5] ? bytes[byteOrder[5]] : extreme,

                // group 3
                bytes.Length > byteOrder[6] ? bytes[byteOrder[6]] : extreme,
                bytes.Length > byteOrder[7] ? bytes[byteOrder[7]] : extreme,

                // group 4
                bytes.Length > byteOrder[8] ? bytes[byteOrder[8]] : extreme,
                bytes.Length > byteOrder[9] ? bytes[byteOrder[9]] : extreme,

                // group 5
                bytes.Length > byteOrder[10] ? bytes[byteOrder[10]] : extreme,
                bytes.Length > byteOrder[11] ? bytes[byteOrder[11]] : extreme,
                bytes.Length > byteOrder[12] ? bytes[byteOrder[12]] : extreme,
                bytes.Length > byteOrder[13] ? bytes[byteOrder[13]] : extreme,
                bytes.Length > byteOrder[14] ? bytes[byteOrder[14]] : extreme,
                bytes.Length > byteOrder[15] ? bytes[byteOrder[15]] : extreme,
            };

            return new Guid(bytes);
        }

        /// <summary>
        /// Shifts a negative number to an unsigned number of higher value than <see cref="MaxSigned128BitBigInt" />, such that -1 becomes <see cref="MaxUnsigned128BitBigInt" />.
        /// </summary>
        internal static BigInteger ShiftNegativeToUnsigned(this BigInteger value)
        {
            if (value < 0)
            {
                value = MaxSigned128BitBigInt*2 + value + 1;
            }

            return value;
        }

        /// <summary>
        /// Shifts an unsigned value higher than <see cref="MaxSigned128BitBigInt" /> into its negative complement.
        /// </summary>
        internal static BigInteger ShiftUnsignedToNegative(this BigInteger value)
        {
            if (value > MaxSigned128BitBigInt)
            {
                value = value - 1 - MaxSigned128BitBigInt*2;
            }

            return value;
        }

        public static IEnumerable<IStreamQueryRangePartition<Guid>> ByRange(
            Guid lowerBoundExclusive,
            Guid upperBoundInclusive,
            int numberOfPartitions)
        {
            var upperBigIntInclusive = upperBoundInclusive.ToBigInteger();
            var lowerBigIntExclusive = lowerBoundExclusive.ToBigInteger();
            var space = upperBigIntInclusive - lowerBigIntExclusive;

            foreach (var i in Enumerable.Range(0, numberOfPartitions))
            {
                var lower = lowerBigIntExclusive + i*(space/numberOfPartitions);

                var upper = lowerBigIntExclusive + (i + 1)*(space/numberOfPartitions);

                if (i == numberOfPartitions - 1)
                {
                    upper = upperBigIntInclusive;
                }

                yield return new SqlGuidRangePartition
                {
                    LowerBoundExclusive = lower.ToGuid(),
                    UpperBoundInclusive = upper.ToGuid()
                };
            }
        }
    }
}