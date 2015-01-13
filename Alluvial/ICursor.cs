using System;

namespace Alluvial
{
    /// <summary>
    /// Records a position within a data stream.
    /// </summary>
    public interface ICursor
    {
        /// <summary>
        /// Gets the position of the cursor.
        /// </summary>
        dynamic Position { get; }

        /// <summary>
        /// Gets a value indicating whether this <see cref="ICursor"/> is ascending relative to the data stream's intrinsic ordering.
        /// </summary>
        bool Ascending { get; }

        /// <summary>
        /// Advances the cursor to the specified position.
        /// </summary>
        void AdvanceTo(dynamic position);

        /// <summary>
        /// Determines whether the specified point in the data stream has been reached.
        /// </summary>
        bool HasReached(dynamic point);
    }
}