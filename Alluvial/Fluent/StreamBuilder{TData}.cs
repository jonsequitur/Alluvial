namespace Alluvial.Fluent
{
    public class StreamBuilder<TData> : StreamBuilder
    {
        public StreamBuilder(string streamId = null)
        {
            StreamId = streamId;
        }
    }
}