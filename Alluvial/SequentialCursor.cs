using System;
using System.Diagnostics;

namespace Alluvial
{
    [DebuggerStepThrough]
    [DebuggerDisplay("Position: {Position}")]
    internal class SequentialCursor<T> : IIncrementableCursor
        where T : IComparable<T>
    {
        private T position;

        public SequentialCursor(T position = default(T),
                                bool ascending = true)
        {
            Ascending = ascending;
            this.position = position;
        }

        public bool Ascending { get; private set; }

        public dynamic Position
        {
            get
            {
                return position;
            }
        }

        public void AdvanceBy(dynamic amount)
        {
            if (Ascending)
            {
                position += amount;
            }
            else
            {
                position -= amount;
            }
        }

        public void AdvanceTo(dynamic sequenceNumber)
        {
            position = sequenceNumber;
        }

        public virtual bool HasReached(dynamic point)
        {
            return Cursor.HasReached(
                position.CompareTo((T) point),
                Ascending);
        }
    }
}