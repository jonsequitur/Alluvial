using System;
using System.Diagnostics;

namespace Alluvial
{
    [DebuggerStepThrough]
    internal class CursorWrapper : IIncrementalCursor
    {
        private ICursor innerCursor = new SequentialCursor();

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

            var incrementalCursor = innerCursor as IIncrementalCursor;
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
    }
}