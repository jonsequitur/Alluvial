using System;
using System.Diagnostics;

namespace Alluvial
{
    [DebuggerStepThrough]
    [DebuggerDisplay("Position: {Position}")]
    internal class ReadOnlyCursor : CursorWrapper
    {
        public ReadOnlyCursor(ICursor innerCursor)
        {
            Wrap(innerCursor);
        }

        public override void AdvanceTo(dynamic position)
        {
            throw new InvalidOperationException("Cursor is read-only");
        }

        public override void AdvanceBy(dynamic amount)
        {
            throw new InvalidOperationException("Cursor is read-only");
        }
    }
}