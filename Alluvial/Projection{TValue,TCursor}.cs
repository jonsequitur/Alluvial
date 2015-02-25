using System;
using System.Diagnostics;

namespace Alluvial
{
    [DebuggerDisplay("Projection: {ProjectionName} @ cursor {CursorPosition}")]
    public class Projection<TValue, TCursor> :
        Projection<TValue>,
        ICursor<TCursor>
    {
        private static readonly string projectionName;

        static Projection()
        {
            projectionName = string.Format("Projection<{0},{1}>", typeof (TValue).Name, typeof (TCursor).Name);
        }

        public TCursor CursorPosition { get; set; }

        void ICursor<TCursor>.AdvanceTo(TCursor point)
        {
            CursorPosition = point;
        }

        TCursor ICursor<TCursor>.Position
        {
            get
            {
                return CursorPosition;
            }
        }

        bool ICursor<TCursor>.HasReached(TCursor point)
        {
            return Cursor.HasReached(((IComparable<TCursor>) CursorPosition).CompareTo(point),
                                     true);
        }

        protected virtual string ProjectionName
        {
            get
            {
                return projectionName;
            }
        }
    }
}