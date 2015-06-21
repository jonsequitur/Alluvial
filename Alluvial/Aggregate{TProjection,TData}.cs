namespace Alluvial
{
    /// <summary>
    /// Updates the state of a projection by aggregating a batch of stream data.
    /// </summary>
    /// <typeparam name="TProjection">The type of the projection.</typeparam>
    /// <typeparam name="TData">The type of the stream's data.</typeparam>
    /// <param name="initial">The initial state of the projection.</param>
    /// <param name="batch">A batch of data from a stream.</param>
    /// <returns>The updated state of the projection.</returns>
    public delegate TProjection Aggregate<TProjection, in TData>(
        TProjection initial,
        IStreamBatch<TData> batch);
}