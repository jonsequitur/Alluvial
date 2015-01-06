using System;

namespace Alluvial
{
    public interface ICursor
    {
        dynamic Position { get; }
        bool Ascending { get; }
        void AdvanceTo(dynamic position);
    }
}