namespace Alluvial
{
    /// <summary>
    /// Handles exceptions that occur during catchup and tells the catchup how to react.
    /// </summary>
    /// <typeparam name="TProjection">The type of the projection.</typeparam>
    /// <param name="error">The error.</param>
    public delegate void HandleAggregatorError<TProjection>(StreamCatchupError<TProjection> error);
}