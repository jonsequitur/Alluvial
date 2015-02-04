using System;
using System.Diagnostics;

namespace Alluvial
{
    [DebuggerStepThrough]
    [DebuggerDisplay("Position: {Position}")]
    internal class CursorWrapper : IIncrementableCursor
    {
        private ICursor innerCursor = new SequentialCursor<long>();

        internal void Wrap(ICursor cursor)
        {
            if (IsInitialized)
            {
                throw new InvalidOperationException("Cursor is already initialized");
            }
            IsInitialized = true;
            innerCursor = cursor;
        }

        public bool Ascending
        {
            get
            {
                return innerCursor.Ascending;
            }
        }

        public dynamic Position
        {
            get
            {
                return innerCursor.Position;
            }
        }

        public bool IsInitialized { get; private set; }

        public virtual void AdvanceBy(dynamic amount)
        {
            IsInitialized = true;

            var incrementalCursor = innerCursor as IIncrementableCursor;
            if (incrementalCursor != null)
            {
                incrementalCursor.AdvanceBy(amount);
            }
            else
            {
                throw new NotImplementedException(string.Format("{0} does not implement IIncrementalCursor", innerCursor.GetType()));
            }
        }

        public virtual void AdvanceTo(dynamic position)
        {
            IsInitialized = true;

            innerCursor.AdvanceTo(position);
        }

        public bool HasReached(dynamic point)
        {
            return innerCursor.HasReached(point);
        }
    }
}