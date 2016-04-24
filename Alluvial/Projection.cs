using System;

namespace Alluvial
{
    /// <summary>
    /// Methods for working with projections.
    /// </summary>
    public static class Projection
    {
        /// <summary>
        /// Creates a cursored projection with the given value and cursor position.
        /// </summary>
        /// <typeparam name="TValue">The type of the projection's data.</typeparam>
        /// <typeparam name="TCursor">The type of the cursor.</typeparam>
        /// <param name="value">The value.</param>
        /// <param name="cursorPosition">The cursor position.</param>
        /// <returns></returns>
        public static Projection<TValue, TCursor> Create<TValue, TCursor>(TValue value, TCursor cursorPosition) =>
            new Projection<TValue, TCursor>
            {
                Value = value,
                CursorPosition = cursorPosition
            };
    }
}