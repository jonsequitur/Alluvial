using System;

namespace Alluvial
{
    /// <summary>
    /// A cursor that supports being advanced incrementally.
    /// </summary>
    public interface IIncrementableCursor : ICursor
    {
        /// <summary>
        /// Advances the cursor by the specified amount.
        /// </summary>
        /// <param name="amount">The amount by which to advance the cursor.</param>
        void AdvanceBy(dynamic amount);
    }
}