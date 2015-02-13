using System;
using System.Threading.Tasks;

namespace Alluvial
{
    /// <summary>
    /// An persistent query over a set of streams of data, which updates one or more stream aggregators. 
    /// </summary>
    /// <typeparam name="TData">The type of the data that the catchup queries.</typeparam>
    public interface IStreamCatchup<out TData>
    {
        /// <summary>
        /// Subscribes an aggregator to the catchup.
        /// </summary>
        /// <typeparam name="TProjection">The type of the aggregated projection.</typeparam>
        /// <param name="aggregator">The aggregator.</param>
        /// <returns>An <see cref="IDisposable" /> that, when disposed, unsubscribed the aggregator from the catchup.</returns>
        IDisposable SubscribeAggregator<TProjection>(IStreamAggregator<TProjection, TData> aggregator,
                                                     FetchAndSaveProjection<TProjection> fetchAndSaveProjection);

        /// <summary>
        /// Consumes a single batch from the source stream and updates the subscribed aggregators.
        /// </summary>
        /// <returns>The catchup's persistent query.</returns>
        Task<ICursor> RunSingleBatch();
    }
}