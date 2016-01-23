using System;
using System.Diagnostics;

namespace Alluvial
{
    /// <summary>
    /// A projection that is also a cursor, allowing it to track its own position in its source stream.
    /// </summary>
    /// <typeparam name="TValue">The type of the projection value.</typeparam>
    /// <typeparam name="TCursor">The type of the cursor.</typeparam>
    [DebuggerDisplay("{ToString()}")]
    public class Projection<TValue, TCursor> :
        Projection<TValue>,
        ICursor<TCursor>,
        ITrackCursorPosition
    {
        private static readonly string projectionName = typeof (Projection<TValue, TCursor>).ReadableName();

        /// <summary>
        /// Gets or sets the cursor position.
        /// </summary>
        /// <value>
        /// The cursor position.
        /// </value>
        public TCursor CursorPosition { get; set; }

        /// <summary>
        /// Advances the cursor to the specified position.
        /// </summary>
        /// <param name="point"></param>
        void ICursor<TCursor>.AdvanceTo(TCursor point)
        {
            CursorPosition = point;
            CursorWasAdvanced = true;
        }

        /// <summary>
        /// Gets the position of the cursor.
        /// </summary>
        TCursor ICursor<TCursor>.Position => CursorPosition;

        public bool CursorWasAdvanced { get; set; }

        bool ICursor<TCursor>.HasReached(TCursor point) =>
            Cursor.HasReached(((IComparable<TCursor>) CursorPosition).CompareTo(point),
                              true);

        protected override string ProjectionName => projectionName;

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

            return $"{ProjectionName}: {valueString} @ cursor {CursorPosition}";
        }
    }
}