using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

namespace Alluvial
{
    /// <summary>
    /// Methods for working with cursors.
    /// </summary>
    [DebuggerStepThrough]
    public static class Cursor
    {
        static Cursor()
        {
            By<int>.Create = () => new SequentialCursor<int>();
            By<long>.Create = () => new SequentialCursor<long>();
            By<DateTime>.Create = () => new ChronologicalCursor();
            By<DateTimeOffset>.Create = () => new ChronologicalCursor();
            By<string>.Create = () => new AlphabeticalCursor();
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
            public static Func<ICursor> Create = () => { throw new InvalidOperationException(string.Format("No ICursor class is mapped for {0}", typeof (T))); };
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

        /// <summary>
        /// Creates a new cursor.
        /// </summary>
        public static ICursor New()
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
            return cursors.OrderBy(c => (object) c.Position)
                          .Select(c => c.Clone())
                          .FirstOrDefault() ??
                   New();
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
    }
}