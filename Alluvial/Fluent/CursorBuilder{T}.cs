using System;

namespace Alluvial.Fluent
{
    public class CursorBuilder<TCursor>
    {
        internal CursorBuilder()
        {
        }

        internal Func<ICursor<TCursor>> NewCursor { get; set; }
    }
}