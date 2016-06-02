namespace Alluvial.Fluent
{
    public class StreamBuilder<TData> : StreamBuilder
    {
        internal StreamBuilder(string streamId = null)
        {
            StreamId = streamId;
        }
    }
}