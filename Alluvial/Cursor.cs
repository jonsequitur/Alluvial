using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

namespace Alluvial
{
    /// <summary>
    /// Methods for working with cursors.
    /// </summary>
    public static class Cursor
    {
        private static readonly StartingPosition startOfStream = new StartingPosition();

        static Cursor()
        {
            By<int>.Create = () => new SequentialCursor<int>();
            By<long>.Create = () => new SequentialCursor<long>();
            By<DateTime>.Create = () => new ChronologicalCursor();
            By<DateTimeOffset>.Create = () => new ChronologicalCursor();
            By<string>.Create = () => new AlphabeticalCursor();
        }

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
        /// Returns the cursor position as a specified type.
        /// </summary>
        public static T As<T>(this ICursor cursor)
        {
            var cursorWrapper = cursor as CursorWrapper;
            if (cursorWrapper != null && !cursorWrapper.IsInitialized)
            {
                cursorWrapper.Wrap(By<T>.Create());
            }

            return (T) cursor.Position;
        }

        internal static class By<T>
        {
            public static Func<ICursor> Create = () =>
            {
                throw new InvalidOperationException(string.Format("No ICursor class is mapped for {0}", typeof (T)));
            };
        }

        /// <summary>
        /// Creates a sequential cursor.
        /// </summary>
        public static ICursor Create(int startAt, bool ascending = true)
        {
            return new SequentialCursor<int>(startAt, ascending);
        }

        /// <summary>
        /// Creates a sequential cursor.
        /// </summary>
        public static ICursor Create(long startAt, bool ascending = true)
        {
            return new SequentialCursor<long>(startAt, ascending);
        }

        /// <summary>
        /// Creates a chronological cursor.
        /// </summary>
        public static ICursor Create(DateTimeOffset startAt, bool ascending = true)
        {
            return new ChronologicalCursor(startAt, ascending);
        }

        /// <summary>
        /// Creates an alphabetical cursor.
        /// </summary>
        public static ICursor Create(string startAt, bool ascending = true)
        {
            return new AlphabeticalCursor(startAt, ascending);
        }

        internal static ICursor Create(StartingPosition startAt, bool ascending = true)
        {
            return new CursorWrapper();
        }

        /// <summary>
        /// Creates a new cursor.
        /// </summary>
        internal static ICursor New()
        {
            return new CursorWrapper();
        }

        /// <summary>
        /// Creates a non-movable cursor based on the position of another cursor.
        /// </summary>
        public static ICursor ReadOnly(this ICursor cursor)
        {
            return new ReadOnlyCursor(cursor);
        }

        internal static ICursor Minimum(this IEnumerable<ICursor> cursors)
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

            return firstOrDefault ?? New();
        }

        internal static ICursor Clone(this ICursor cursor)
        {
            return Create(cursor.Position);
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