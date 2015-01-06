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
}