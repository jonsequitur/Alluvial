using System;
using System.Diagnostics;

namespace Alluvial
{
    [DebuggerStepThrough]
    [DebuggerDisplay("Position: {Position}")]
    internal class ReadOnlyCursor<T> : Cursor<T>
    {
        public ReadOnlyCursor(ICursor<T> innerCursor)
        {
            Position = innerCursor.Position;
        }

        public override void AdvanceTo(T position)
        {
            throw new InvalidOperationException("Cursor is read-only");
        }
    }
}