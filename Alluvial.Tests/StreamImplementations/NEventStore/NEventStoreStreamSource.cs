using System;
using System.Linq;
using Alluvial.Tests.BankDomain;
using NEventStore;

namespace Alluvial.Tests
{
    public class NEventStoreStreamSource :
        IStreamSource<string, IDomainEvent, int>
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

        public IStream<IDomainEvent, int> Open(string streamId)
        {
            return NEventStoreStream.ByAggregate(store, streamId).DomainEvents();
        }

        public IStream<IStream<IDomainEvent, int>, string> EventsByAggregate()
        {
            return StreamUpdates()
                .Requery(update =>
                {
                    var stream = Open(update.StreamId);

                    stream = stream.Map(
                        id: stream.Id,
                        map: es => es.Select(e =>
                        {
                            e.CheckpointToken = update.CheckpointToken;
                            return e;
                        }));

                    return stream;
                });
        }

        private IStream<NEventStoreStreamUpdate, string> StreamUpdates()
        {
            return Stream.Create(
                id: "NEventStoreStreamSource.StreamUpdates",
                // get only changes since the last checkpoint
                query: async q => store.Advanced
                                       .GetFrom(q.Cursor.Position)
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
                },
                newCursor: () => Cursor.New<string>());
        }
    }
}