using System;

namespace Alluvial
{
    /// <summary>
    /// Records a position within a stream.
    /// </summary>
    public interface ICursor<TPosition>
    {
        /// <summary>
        /// Gets the position of the cursor.
        /// </summary>
        TPosition Position { get; }

        /// <summary>
        /// Advances the cursor to the specified position.
        /// </summary>
        void AdvanceTo(TPosition point);

        /// <summary>
        /// Determines whether the specified point in the stream has been reached.
        /// </summary>
        bool HasReached(TPosition point);
    }
}