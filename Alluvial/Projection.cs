using System;

namespace Alluvial
{
    public static class Projection
    {
        public static Projection<TValue, TCursorPosition> Create<TValue, TCursorPosition>(TValue value, TCursorPosition cursorPosition)
        {
            return new Projection<TValue, TCursorPosition>
            {
                Value = value,
                CursorPosition = cursorPosition
            };
        }
    }
}