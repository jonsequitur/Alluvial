using System;
using System.Linq;
using System.Threading.Tasks;
using NEventStore;

namespace Alluvial.Tests
{
    public class NEventStoreStreamUpdate
    {
        public string StreamId{get;set;}
        public string CheckpointToken{get;set;}
    }

    public class NEventStoreDataStream : IDataStream<EventMessage>, IDisposable
    {
        private readonly IStoreEvents store;
        private readonly string streamId;

        public NEventStoreDataStream(IStoreEvents store, string streamId)
        {
            if (store == null)
            {
                throw new ArgumentNullException("store");
            }
            if (streamId == null)
            {
                throw new ArgumentNullException("streamId");
            }
            this.store = store;
            this.streamId = streamId;
        }

        public string Id
        {
            get
            {
                return streamId;
            }
        }

        public async Task<IStreamQueryBatch<EventMessage>> Fetch(IStreamQuery<EventMessage> query)
        {
            int lastFetchedRevision = query.Cursor.Position;

            int maxRevisionToFetch = query.BatchCount == null
                                         ? int.MaxValue
                                         : lastFetchedRevision + query.BatchCount.Value;

            int maxExistingRevision = store.Advanced
                                           .GetFrom("default",
                                                    streamId,
                                                    lastFetchedRevision,
                                                    int.MaxValue)
                                           .Select(c => c.StreamRevision)
                                           .LastOrDefault();

            if (maxExistingRevision <= lastFetchedRevision)
            {
                return StreamQueryBatch.Empty<EventMessage>(query.Cursor);
            }

            using (var stream = store.OpenStream(streamId,
                                                 minRevision: lastFetchedRevision + 1,
                                                 maxRevision: maxRevisionToFetch))
            {
                var batch = StreamQueryBatch.Create(stream.CommittedEvents.ToArray(), query.Cursor);

                query.Cursor.AdvanceTo(stream.StreamRevision);

                return batch;
            }
        }

        public void Dispose()
        {
            store.Dispose();
        }
    }
}