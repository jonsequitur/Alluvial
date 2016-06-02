using System;
using System.Linq;
using Alluvial.Tests.BankDomain;
using NEventStore;

namespace Alluvial.Tests.StreamImplementations.NEventStore
{
    public static class NEventStoreExtensions
    {
        public static IStream<IDomainEvent, TCursor> DomainEvents<TCursor>(this IStream<EventMessage, TCursor> stream)
        {
            return stream.Map(es => es.Select(e =>
            {
                var de = e.Body as IDomainEvent;
                if (de != null)
                {
                    de.StreamRevision = StreamRevision(e);
                }
                return e.Body;
            }).OfType<IDomainEvent>(),
                              id: stream.Id);
        }

        public static int StreamRevision(this EventMessage e)
        {
            return (int) e.Headers["StreamRevision"];
        }

        public static void SetStreamRevision(
            this EventMessage e, 
            int streamRevision,
            string checkpoint)
        {
            e.Headers["StreamRevision"] = streamRevision;
           
            var domainEvent = e.Body as IDomainEvent;
            if (domainEvent != null)
            {
                domainEvent.StreamRevision = streamRevision;
                domainEvent.CheckpointToken = checkpoint;
            }
        }

        public static void WriteEvents(
            this IStoreEvents store, 
            string streamId, 
            decimal amount = 1, 
            int howMany = 1, 
            string bucketId = "default")
        {
            for (var i = 0; i < howMany; i++)
            {
                using (var eventStream = store.OpenStream(bucketId, streamId, 0, int.MaxValue))
                {
                    if (amount > 0)
                    {
                        eventStream.Add(new EventMessage
                        {
                            Body = new FundsDeposited
                            {
                                AggregateId = streamId,
                                Amount = amount
                            }
                        });
                    }
                    else
                    {
                        eventStream.Add(new EventMessage
                        {
                            Body = new FundsWithdrawn
                            {
                                AggregateId = streamId,
                                Amount = amount
                            }
                        });
                    }

                    eventStream.CommitChanges(Guid.NewGuid());
                }
            }
        }

        public static void WriteEvents(
            this IStoreEvents store,
            Func<int, IDomainEvent> getEvent,
            int howMany = 1)
        {
            for (var i = 0; i < howMany; i++)
            {
                var @event = getEvent(i);
                @event.AggregateId = @event.AggregateId ?? Guid.NewGuid().ToString();
                using (var eventStream = store.OpenStream(@event.AggregateId, 0))
                {
                    eventStream.Add(new EventMessage
                    {
                        Body = @event
                    });

                    eventStream.CommitChanges(Guid.NewGuid());
                }
            }
        }
    }
}