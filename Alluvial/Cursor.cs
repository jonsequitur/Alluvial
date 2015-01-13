using System;
using System.Diagnostics;

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
            By<int>.Create = () => new SequentialCursor();
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

            return cursor.Position;
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
            return new SequentialCursor(startAt, ascending);
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
        public static ICursor ReadOnly(ICursor cursor)
        {
            return new ReadOnlyCursor(cursor);
        }

        /// <summary>
        /// Determines whether the specified comparison has reached a specific point.
        /// </summary>
        /// <param name="comparison">The result of a call to <see cref="IComparable.CompareTo" />.</param>
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