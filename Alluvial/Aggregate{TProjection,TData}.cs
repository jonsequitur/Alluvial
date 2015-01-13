namespace Alluvial
{
    /// <summary>
    /// Applies a batch data to a projection. 
    /// </summary>
    /// <typeparam name="TProjection">The type of the projection.</typeparam>
    /// <typeparam name="TData">The type of the data.</typeparam>
    /// <param name="initial">The initial state of the projection prior to the data being applied.</param>
    /// <param name="batch">The data to be applied to the projection.</param>
    /// <returns>The updated projection.</returns>
    public delegate TProjection Aggregate<TProjection, in TData>(
        TProjection initial,
        IStreamQueryBatch<TData> batch);
}