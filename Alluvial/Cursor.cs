using System;
using System.Diagnostics;

namespace Alluvial
{
    [DebuggerStepThrough]
    public static class Cursor
    {
        static Cursor()
        {
            By<int>.Create = () => new SequentialCursor();
            By<DateTime>.Create = () => new ChronologicalCursor();
            By<DateTimeOffset>.Create = () => new ChronologicalCursor();
            By<string>.Create = () => new StringCursor();
        }

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

        public static ICursor Create(int startAt, bool ascending = true)
        {
            return new SequentialCursor(startAt, ascending);
        }

        public static ICursor Create(DateTimeOffset startAt, bool ascending = true)
        {
            return new ChronologicalCursor(startAt, ascending);
        }

        public static ICursor Create(string startAt, bool ascending = true)
        {
            return new StringCursor(startAt, ascending);
        }

        public static ICursor New()
        {
            return new CursorWrapper();
        }

        public static ICursor ReadOnly(ICursor cursor)
        {
            return new ReadOnlyCursor(cursor);
        }

        public static bool HasReached<TData>(this ICursor cursor, TData point)
        {
            int comparison = Compare<TData>((dynamic) cursor, (dynamic) point);

            if (cursor.Ascending)
            {
                return comparison >= 0;
            }

            return comparison <= 0;
        }

        private static int Compare<TData>(IComparable<TData> comparable, TData point)
        {
            return comparable.CompareTo(point);
        }

        private static int Compare<TData>(ICursor cursor1, ICursor cursor2)
        {
            return cursor1.Position.CompareTo(cursor2.Position);
        }

        private static int Compare<TData>(ICursor cursor, IComparable<TData> point)
        {
            return -(point.CompareTo(cursor.Position));
        }

        private static int Compare<TData>(object cursor, object point)
        {
            return cursor.Equals(point) ? 0 : 1;
        }
    }
}