using System;
using System.Linq;
using Alluvial.Tests.BankDomain;
using NEventStore;

namespace Alluvial.Tests
{
    public class NEventStoreStreamSource :
        IStreamSource<string, IDomainEvent>
    {
        private readonly IStoreEvents store;

        public NEventStoreStreamSource(IStoreEvents store)
        {
            if (store == null)
            {
                throw new ArgumentNullException("store");
            }
            this.store = store;
        }

        public IStream<IDomainEvent> Open(string streamId)
        {
            return OpenStream(streamId);
        }

        private IStream<IDomainEvent> OpenStream(string streamId, string startAfter = null)
        {
            return new NEventStoreStream(store, streamId).DomainEvents();
        }

        public IStream<IStream<IDomainEvent>> UpdatedStreams()
        {
            return Stream.Create(
                id: "NEventStoreStreamSource.UpdatedStreams",
                // get only changes since the last checkpoint
                query: q => store.Advanced
                                 .GetFrom(q.Cursor.As<string>())
                                 .GroupBy(c => c.StreamId)
                                 .Select(c => new NEventStoreStreamUpdate
                                 {
                                     StreamId = c.Key,
                                     CheckpointToken = c.Max(e => e.CheckpointToken),
                                     StreamRevision = c.Max(e => e.StreamRevision)
                                 })
                                 .Take(q.BatchCount ?? 100000),
                advanceCursor: (query, batch) =>
                {
                    var last = batch.LastOrDefault();
                    if (last != null)
                    {
                        query.Cursor.AdvanceTo(last.CheckpointToken);
                    }
                })
                         .Requery(update => OpenStream(update.StreamId,
                                                       startAfter: update.CheckpointToken));
        }
    }
}