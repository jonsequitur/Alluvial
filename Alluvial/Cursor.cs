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
            By<string>.Create = () => new AlphabeticalCursor();
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
            return new AlphabeticalCursor(startAt, ascending);
        }

        public static ICursor New()
        {
            return new CursorWrapper();
        }

        public static ICursor ReadOnly(ICursor cursor)
        {
            return new ReadOnlyCursor(cursor);
        }

        public static bool HasReached(int comparison, bool ascending)
        {
            return ascending
                ? comparison >= 0
                : comparison <= 0;
        }
    }
}