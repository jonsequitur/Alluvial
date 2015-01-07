using System;

namespace Alluvial
{
    public interface IIncrementableCursor : ICursor
    {
        void AdvanceBy(dynamic amount);
    }
}