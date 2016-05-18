using System;
using System.Linq;
using Alluvial.Tests.BankDomain;
using NEventStore;

namespace Alluvial.Tests
{
    public class NEventStoreStreamSource
    {
        private readonly IStoreEvents store;

        public NEventStoreStreamSource(IStoreEvents store)
        {
            if (store == null)
            {
                throw new ArgumentNullException(nameof(store));
            }
            this.store = store;
        }

        public IStream<IStream<IDomainEvent, int>, string> StreamPerAggregate() =>
            StreamUpdates()
                .IntoMany(
                    async (streamUpdate, fromCursor, toCursor) =>
                    {
                        var allEvents = NEventStoreStream.AllEvents(store);

                        var cursor = Cursor.New(fromCursor);
                        var batch = await allEvents.CreateQuery(cursor, int.Parse(toCursor))
                                                   .NextBatch();

                        var aggregate = batch.Select(e => e.Body)
                                             .Cast<IDomainEvent>()
                                             .Where(e => e.AggregateId == streamUpdate.StreamId);

                        return aggregate.AsStream(
                            id: streamUpdate.StreamId,
                            cursorPosition: e => e.StreamRevision);
                    });

        private IStream<NEventStoreStreamUpdate, string> StreamUpdates()
        {
            return Stream.Create(
                id: "NEventStoreStreamSource.StreamUpdates",
                // get only changes since the last checkpoint
                query: q => store.Advanced
                                 .GetFrom(q.Cursor.Position)
                                 .GroupBy(c => c.StreamId)
                                 .Select(c => new NEventStoreStreamUpdate
                                 {
                                     StreamId = c.Key,
                                     CheckpointToken = c.Max(e => e.CheckpointToken),
                                     StreamRevision = c.Max(e => e.StreamRevision)
                                 })
                                 .Take(q.BatchSize ?? 100000),
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