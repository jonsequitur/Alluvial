using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Linq;
using System.Numerics;

namespace Alluvial
{
    public static class SqlGuidPartitioner
    {
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
            10,
        };

        public static IEnumerable<Guid> OrderBySqlServer(this IEnumerable<Guid> source)
        {
            return source.OrderBy(g => new SqlGuid(g));
        }

        public static BigInteger ToBigInteger(this Guid guid)
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
                    bytes[byteOrder[15]],
                });

            value = Sortify(value);

            return value;
        }

        public static Guid ToGuid(this BigInteger value)
        {
            var bytes = value.Unsortify().ToByteArray();

            if (bytes.Length > 16)
            {
                throw new ArgumentException(string.Format("{0} takes more than 128 bits to represent and cannot be stored in a guid", value));
            }

            if (value == 0)
            {
                return Guid.Empty;
            }

            var extreme = value > 0 ? byte.MinValue : byte.MaxValue;

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

        public static BigInteger Sortify(this BigInteger value)
        {
            if (value < 0)
            {
                value = (Max128BitBigInt*2) + 1 + value;
                return value;
            }

            return value;
        }

        public static BigInteger Unsortify(this BigInteger value)
        {
            if (value > Max128BitBigInt)
            {
                value = (value) - 1 - (Max128BitBigInt*2);
                return value;
            }

            return value;
        }

        public static readonly BigInteger Max128BitBigInt = BigInteger.Parse("170141183460469231731687303715884105727");
    }
}