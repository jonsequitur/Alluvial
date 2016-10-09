using System;

namespace Alluvial.Fluent
{
    /// <summary>
    /// Defines cursor behaviors for a stream.
    /// </summary>
    public class CursorBuilder<TCursor>
    {
        internal CursorBuilder()
        {
        }

        internal Func<ICursor<TCursor>> NewCursor { get; set; }
    }
}