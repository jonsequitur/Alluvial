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
        /// <summary>
        /// Creates a new cursor.
        /// </summary>
        public static ICursor<TCursor> New<TCursor>(TCursor position = default(TCursor)) =>
            new Cursor<TCursor>(position);

        /// <summary>
        /// Creates a non-movable cursor based on the position of another cursor.
        /// </summary>
        public static ICursor<T> ReadOnly<T>(this ICursor<T> cursor) =>
            new ReadOnlyCursor<T>(cursor);

        internal static ICursor<TCursor> MinOrDefault<TCursor>(this IEnumerable<ICursor<TCursor>> cursors)
        {
            var cursorArray = cursors.ToArray();

            var startingPosition = cursorArray.FirstOrDefault(c => c.Position == null);

            if (startingPosition != null)
            {
                return startingPosition.Clone();
            }

            var firstOrDefault = cursorArray
                .Where(c => c != null && c.Position != null)
                .OrderBy(c => (object) c.Position)
                .Select(c => c.Clone())
                .FirstOrDefault();

            return firstOrDefault;
        }

        internal static ICursor<TCursor> Clone<TCursor>(this ICursor<TCursor> cursor) =>
            New(cursor.Position);

        /// <summary>
        /// Determines whether the specified comparison has reached a specific point.
        /// </summary>
        /// <param name="comparison">The result of a call to <see cref="IComparable.CompareTo" />, comparing the cursor to .</param>
        /// <param name="ascending"></param>
        /// <returns></returns>
        public static bool HasReached(int comparison, bool ascending) =>
            @ascending
                ? comparison >= 0
                : comparison <= 0;
    }
}