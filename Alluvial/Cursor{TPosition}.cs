using System;
using System.Diagnostics;

namespace Alluvial
{
    [DebuggerDisplay("{ToString()}")]
    public class Cursor<T> : ICursor<T>, ITrackCursorPosition
    {
        private static readonly Func<Cursor<T>, T, bool> hasCursorReached;
        private readonly T originalPosition;

        static Cursor()
        {
            var  positionIsComparable = typeof (IComparable<T>).IsAssignableFrom(typeof (T));

            if (positionIsComparable)
            {
                hasCursorReached = (cursor, point) =>
                {
                    if (cursor.Position == null)
                    {
                        return false;
                    }

                    var comparablePosition = (IComparable<T>)cursor.Position;
                    return comparablePosition.CompareTo(point) >= 0;
                };
            }
            else
            {
                hasCursorReached = (cursor, point) =>
                {
                    throw new InvalidOperationException("Cursor position cannot be compared to " + typeof(T));
                };
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Cursor{T}"/> class.
        /// </summary>
        public Cursor() : this(default(T))
        {
        }

        public Cursor(T position)
        {
            Position = position;
            originalPosition = position;
        }

        public virtual bool HasReached(T point) => hasCursorReached(this, point);

        public virtual void AdvanceTo(T point) => Position = point;

        public virtual T Position { get; protected set; }

        public override string ToString() => $"{Position}";

        public bool CursorWasAdvanced => originalPosition.Equals(Position);
    }
}