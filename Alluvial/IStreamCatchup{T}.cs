using System;
using System.Threading.Tasks;

namespace Alluvial
{
    /// <summary>
    /// An persistent query over a stream of data, which updates one or more stream aggregators.
    /// </summary>
    /// <typeparam name="TData">The type of the data that the catchup pushes to the aggregators.</typeparam>
    public interface IStreamCatchup<out TData>
    {
        /// <summary>
        /// Subscribes an aggregator to the catchup.
        /// </summary>
        /// <typeparam name="TProjection">The type of the aggregated projection.</typeparam>
        /// <param name="aggregator">The aggregator.</param>
        /// <param name="fetchAndSave">The fetch and save function that is called when the catchup has data.</param>
        /// <param name="onError">A delegate that is called when an aggregator encounters an error.</param>
        /// <returns>
        /// An <see cref="IDisposable" /> that, when disposed, unsubscribed the aggregator from the catchup.
        /// </returns>
        IDisposable SubscribeAggregator<TProjection>(
            IStreamAggregator<TProjection, TData> aggregator,
            FetchAndSave<TProjection> fetchAndSave,
            HandleAggregatorError<TProjection> onError);

        /// <summary>
        /// Consumes a single batch from the source stream and updates the subscribed aggregators.
        /// </summary>
        /// <param name="lease">A lease that can be used to extend or cancel the batch operation.</param>
        Task RunSingleBatch(ILease lease);
    }
}