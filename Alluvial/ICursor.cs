using System;

namespace Alluvial
{
    public interface ICursor
    {
        dynamic Position { get; }
        bool Ascending { get; }
        void AdvanceTo(dynamic position);
    }

    internal class ChronologicalCursor : ICursor, IIncrementalCursor
    {
        private DateTimeOffset position;

        public ChronologicalCursor(
            DateTimeOffset position = default(DateTimeOffset),
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
                position = position.Add((TimeSpan) amount);
            }
            else
            {
                position = position.Subtract((TimeSpan) amount);
            }
        }

        public void AdvanceTo(dynamic position)
        {
            this.position = position;
        }
    }

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