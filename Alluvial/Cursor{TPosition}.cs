using System;
using System.Diagnostics;

namespace Alluvial
{
    /// <summary>
    /// Records a position within a stream.
    /// </summary>
    /// <typeparam name="T">The type of the cursor's position.</typeparam>
    /// <seealso cref="Alluvial.ICursor{T}" />
    /// <seealso cref="Alluvial.ITrackCursorPosition" />
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

        /// <summary>
        /// Initializes a new instance of the <see cref="Cursor{T}"/> class.
        /// </summary>
        /// <param name="position">The position of the cursor.</param>
        public Cursor(T position)
        {
            Position = position;
            originalPosition = position;
        }

        /// <summary>
        /// Determines whether the specified point in the stream has been reached.
        /// </summary>
        public virtual bool HasReached(T point) => hasCursorReached(this, point);

        /// <summary>
        /// Advances the cursor to the specified position.
        /// </summary>
        public virtual void AdvanceTo(T point) => Position = point;

        /// <summary>
        /// Gets the position of the cursor.
        /// </summary>
        public virtual T Position { get; protected set; }

        /// <summary>
        /// Returns a <see cref="System.String" /> that represents this instance.
        /// </summary>
        /// <returns>
        /// A <see cref="System.String" /> that represents this instance.
        /// </returns>
        public override string ToString() => $"{Position}";

        /// <summary>
        /// Gets a value indicating whether the cursor was advanced from its initial position at the time the instance was created.
        /// </summary>
        /// <value>
        ///   <c>true</c> if the cursor was advanced; otherwise, <c>false</c>.
        /// </value>
        public bool CursorWasAdvanced => originalPosition.Equals(Position);
    }
}