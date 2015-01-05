namespace Alluvial
{
    public interface IIncrementalCursor : ICursor
    {
        void AdvanceBy(dynamic amount);
    }
}