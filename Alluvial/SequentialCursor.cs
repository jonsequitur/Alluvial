using System.Diagnostics;

namespace Alluvial
{
    [DebuggerStepThrough]
    internal class SequentialCursor : ICursor, IIncrementalCursor
    {
        private int position;

        public SequentialCursor(int position = 0,
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

        public void AdvanceTo(dynamic position)
        {
            this.position = position;
        }
    }
}