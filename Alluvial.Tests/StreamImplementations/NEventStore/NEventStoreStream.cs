using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using NEventStore;

namespace Alluvial.Tests
{
    public class NEventStoreStream : IStream<EventMessage, int>, IDisposable
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

        public async Task<IStreamBatch<EventMessage>> Fetch(IStreamQuery<int> query)
        {
            var lastFetchedRevision = Math.Max(query.Cursor.Position, 0);

            int maxRevisionToFetch;

            checked
            {
                maxRevisionToFetch = lastFetchedRevision + query.BatchCount ?? 100000;
            }

            var maxExistingRevision = store.Advanced
                                           .GetFrom("default",
                                                    streamId,
                                                    lastFetchedRevision,
                                                    int.MaxValue)
                                           .Select(c => c.StreamRevision)
                                           .LastOrDefault();

            if (maxExistingRevision <= lastFetchedRevision)
            {
                return query.Cursor.EmptyBatch<EventMessage, int>();
            }

            var events = new List<EventMessage>();

            checked
            {
                for (var i = lastFetchedRevision + 1; i <= maxRevisionToFetch; i++)
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
                                                      e.SetStreamRevision(stream.StreamRevision);
                                                      return e;
                                                  }));
                        }
                    }
                    catch (StreamNotFoundException)
                    {
                        break;
                    }
                }
            }

            var batch = StreamBatch.Create(events, query.Cursor);

            if (batch.Count > 0)
            {
                query.Cursor.AdvanceTo(batch.Max(i => i.StreamRevision()));
            }

            return batch;
        }

        public ICursor<int> NewCursor()
        {
            return Cursor.New<int>();
        }

        public void Dispose()
        {
            store.Dispose();
        }
    }
}