using System;
using System.Diagnostics;

namespace Alluvial
{
    [DebuggerStepThrough]
    [DebuggerDisplay("Position: \"{Position}\"")]
    internal class AlphabeticalCursor : ICursor
    {
        private string position;

        public AlphabeticalCursor(string position = "",
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

        public void AdvanceTo(dynamic position)
        {
            this.position = position;
        }

        public virtual bool HasReached(dynamic point)
        {
            int comparison = StringComparer.OrdinalIgnoreCase.Compare(position, point);

            if (Ascending)
            {
                return comparison >= 0;
            }

            return comparison <= 0;
        }
    }
}