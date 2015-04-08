using System;
using System.Diagnostics;

namespace Alluvial
{
    [DebuggerDisplay("{ToString()}")]
    public class Projection<TValue, TCursor> :
        Projection<TValue>,
        ICursor<TCursor>,
        ITrackCursorPosition
    {
        private static readonly string projectionName = typeof (Projection<TValue, TCursor>).ReadableName();

        public TCursor CursorPosition { get; set; }

        void ICursor<TCursor>.AdvanceTo(TCursor point)
        {
            CursorPosition = point;
            CursorWasAdvanced = true;
        }

        TCursor ICursor<TCursor>.Position
        {
            get
            {
                return CursorPosition;
            }
        }

        public bool CursorWasAdvanced { get; private set; }

        bool ICursor<TCursor>.HasReached(TCursor point)
        {
            return Cursor.HasReached(((IComparable<TCursor>) CursorPosition).CompareTo(point),
                                     true);
        }

        protected override string ProjectionName
        {
            get
            {
                return projectionName;
            }
        }
    
       public override string ToString()
        {
            string valueString;

            var v = Value;
            if (v != null)
            {
                valueString = v.ToString();
            }
            else
            {
                valueString = "null";
            }

            return string.Format("{0}: {1} @ cursor {2}", ProjectionName, valueString, CursorPosition);
        }
    }

    internal interface ITrackCursorPosition
    {
        bool CursorWasAdvanced { get;}
    }
}