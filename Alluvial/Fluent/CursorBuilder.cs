using System;

namespace Alluvial.Fluent
{
    public class CursorBuilder
    {
        internal CursorBuilder()
        {
        }

        public CursorBuilder<TCursor> By<TCursor>() =>
            new CursorBuilder<TCursor>
            {
                NewCursor = () => Cursor.New<TCursor>()
            };

        public CursorBuilder<TCursor> StartsAt<TCursor>(Func<ICursor<TCursor>> @new) =>
            new CursorBuilder<TCursor>
            {
                NewCursor = @new
            };

        public CursorBuilder<TCursor> StartsAt<TCursor>(TCursor position) =>
            new CursorBuilder<TCursor>
            {
                NewCursor = () => Cursor.New(position)
            };
    }
}