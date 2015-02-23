using System;
using System.Diagnostics;

namespace Alluvial
{
    [DebuggerDisplay("Projection: {ToString()} @ cursor {CursorPosition}")]
    public class Projection<TValue, TCursorPosition> :
        Projection<TValue>,
        ICursor,
        ICursor<TCursorPosition>
    {
        private static readonly string typeName = string.Format("Projection<{0},{1}>", typeof (TValue).Name, typeof (TCursorPosition).Name);

        public Projection()
        {
            Ascending = true;
        }

        public TCursorPosition CursorPosition { get; set; }

        dynamic ICursor.Position
        {
            get
            {
                return CursorPosition;
            }
        }

        public bool Ascending { get; private set; }

        public void AdvanceTo(dynamic position)
        {
            CursorPosition = position;
        }

        public bool HasReached(dynamic point)
        {
            return Cursor.HasReached(((IComparable) CursorPosition).CompareTo(point), Ascending);
        }

        public void AdvanceCursorTo(TCursorPosition point)
        {
            CursorPosition = point;
        }

        public virtual bool HasCursorReached(TCursorPosition point)
        {
            return Cursor.HasReached(((IComparable<TCursorPosition>) CursorPosition).CompareTo(point),
                                     Ascending);
        }

        protected virtual string TypeName
        {
            get
            {
                return typeName;
            }
        }
    }
}