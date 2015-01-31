using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using NEventStore;

namespace Alluvial.Tests
{
    public class NEventStoreStream : IStream<EventMessage>, IDisposable
    {
        private readonly IStoreEvents store;
        private readonly string streamId;

        public NEventStoreStream(IStoreEvents store, string streamId)
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

        public async Task<IStreamBatch<EventMessage>> Fetch(IStreamQuery query)
        {
            int lastFetchedRevision = query.Cursor.Position;

            var maxRevisionToFetch = lastFetchedRevision + query.BatchCount ?? int.MaxValue;

            var maxExistingRevision = store.Advanced
                                           .GetFrom("default",
                                                    streamId,
                                                    lastFetchedRevision,
                                                    int.MaxValue)
                                           .Select(c => c.StreamRevision)
                                           .LastOrDefault();

            if (maxExistingRevision <= lastFetchedRevision)
            {
                return StreamBatch.Empty<EventMessage>(query.Cursor);
            }

            var events = new List<EventMessage>();

            for (var i = lastFetchedRevision + 1; i < maxRevisionToFetch; i++)
            {
                try
                {
                    using (var stream = store.OpenStream(streamId,
                                                         minRevision: i,
                                                         maxRevision: i))
                    {
                        if (stream.CommittedEvents.Count == 0)
                        {
                            break;
                        }

                        events.AddRange(stream.CommittedEvents
                                              .Select(e =>
                                              {
                                                  e.Headers["StreamRevision"] = stream.StreamRevision;
                                                  return e;
                                              }));
                    }
                }
                catch (StreamNotFoundException)
                {
                    break;
                }
            }

            var batch = StreamBatch.Create(events, query.Cursor);

            query.Cursor.AdvanceTo(maxExistingRevision);

            return batch;
        }

        public void Dispose()
        {
            store.Dispose();
        }
    }
}