namespace Alluvial.Fluent
{
    /// <summary>
    /// Defines the structure and behavior of a stream.
    /// </summary>
    /// <typeparam name="TData">The type of the data in the stream.</typeparam>
    /// <seealso cref="Alluvial.Fluent.StreamBuilder" />
    public class StreamBuilder<TData> : StreamBuilder
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="StreamBuilder{TData}"/> class.
        /// </summary>
        /// <param name="streamId">The stream identifier.</param>
        internal StreamBuilder(string streamId = null)
        {
            StreamId = streamId;
        }
    }
}