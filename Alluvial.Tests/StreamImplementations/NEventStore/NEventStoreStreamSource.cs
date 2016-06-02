using System;
using System.Linq;
using Alluvial.Fluent;
using Alluvial.Tests.BankDomain;
using NEventStore;

namespace Alluvial.Tests.StreamImplementations.NEventStore
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
                    (streamUpdate, fromCursor, toCursor) =>
                    {
#if true
                        return Stream.Of<IDomainEvent>(streamUpdate.StreamId)
                                     .Cursor(_ => _.By<int>())
                                     .Advance(
                                         (q, b) =>
                                         {
                                             var last = b.LastOrDefault();
                                             if (last != null)
                                             {
                                                 q.Cursor.AdvanceTo(last.StreamRevision);
                                             }
                                         })
                                     .Create(async query =>
                                     {
                                         var events = NEventStoreStream.ByAggregate(
                                             store,
                                             streamUpdate.StreamId);

                                         var batch = await events.Fetch(query);

                                         return batch
                                             .Select(e => e.Body)
                                             .Cast<IDomainEvent>();
                                     });
#else
                        var allEvents = NEventStoreStream.AllEvents(store);
                        var cursor = Cursor.New(fromCursor);
                        var batch = allEvents.CreateQuery(cursor)
                                             .NextBatch()
                                             .Result;

                        var aggregate = batch.Select(e => e.Body)
                                             .Cast<IDomainEvent>()
                                             .Where(e => e.AggregateId == streamUpdate.StreamId);

                        return aggregate.AsStream(
                            id: streamUpdate.StreamId,
                            cursorPosition: e => e.StreamRevision);
#endif
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