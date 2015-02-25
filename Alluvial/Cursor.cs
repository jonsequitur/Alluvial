using System;
using System.Collections.Generic;
using System.Linq;

namespace Alluvial
{
    /// <summary>
    /// Methods for working with cursors.
    /// </summary>
    public static class Cursor
    {
        private static readonly StartingPosition startOfStream = new StartingPosition();

        /// <summary>
        /// Gets an immutable object that represents the start of any stream.
        /// </summary>
        public static StartingPosition StartOfStream
        {
            get
            {
                return startOfStream;
            }
        }

        /// <summary>
        /// Creates a new cursor.
        /// </summary>
        public static ICursor<TCursor> New<TCursor>(TCursor position = default(TCursor))
        {
            return new Cursor<TCursor>(position);
        }

        /// <summary>
        /// Creates a non-movable cursor based on the position of another cursor.
        /// </summary>
        public static ICursor<T> ReadOnly<T>(this ICursor<T> cursor)
        {
            return new ReadOnlyCursor<T>(cursor);
        }

        internal static ICursor<TCursor> Minimum<TCursor>(this IEnumerable<ICursor<TCursor>> cursors)
        {
            var cursorArray = cursors.ToArray();

            var startingPosition = cursorArray.FirstOrDefault(c => c.Position == null ||
                                                                   c.Position is StartingPosition);

            if (startingPosition != null)
            {
                return startingPosition.Clone();
            }

            var firstOrDefault = cursorArray
                .Where(c => c != null && c.Position != null)
                .OrderBy(c => (object) c.Position)
                .Select(c => c.Clone())
                .FirstOrDefault();

            return firstOrDefault ?? New<TCursor>();
        }

        internal static ICursor<TCursor> Clone<TCursor>(this ICursor<TCursor> cursor)
        {
            return New(cursor.Position);
        }

        /// <summary>
        /// Determines whether the specified comparison has reached a specific point.
        /// </summary>
        /// <param name="comparison">The result of a call to <see cref="IComparable.CompareTo" />, comparing the cursor to .</param>
        /// <param name="ascending"></param>
        /// <returns></returns>
        public static bool HasReached(int comparison, bool ascending)
        {
            return ascending
                ? comparison >= 0
                : comparison <= 0;
        }

        public struct StartingPosition :
            IComparable<DateTime>,
            IComparable<DateTimeOffset>,
            IComparable<int>,
            IComparable<long>
        {
            public static bool operator >(StartingPosition start, object value)
            {
                return false;
            }

            public static bool operator <(StartingPosition start, object value)
            {
                return true;
            }

            public static bool operator >(StartingPosition start, DateTime value)
            {
                return false;
            }

            public static bool operator <(StartingPosition start, DateTime value)
            {
                return true;
            }

            public static bool operator >(StartingPosition start, DateTimeOffset value)
            {
                return false;
            }

            public static bool operator <(StartingPosition start, DateTimeOffset value)
            {
                return true;
            }

            public static implicit operator int(StartingPosition start)
            {
                return int.MinValue;
            }

            public static implicit operator long(StartingPosition start)
            {
                return long.MinValue;
            }

            public static implicit operator DateTime(StartingPosition start)
            {
                return DateTime.MinValue;
            }

            public static implicit operator DateTimeOffset(StartingPosition start)
            {
                return DateTimeOffset.MinValue;
            }

            public int CompareTo(DateTime other)
            {
                return -1;
            }

            public int CompareTo(DateTimeOffset other)
            {
                return -1;
            }

            public int CompareTo(int other)
            {
                return -1;
            }

            public int CompareTo(long other)
            {
                return -1;
            }
        }
    }
}