using System.Linq;
using Alluvial.Tests.BankDomain;
using NEventStore;

namespace Alluvial.Tests
{
    public static class NEventStoreExtensions
    {
        public static IStream<IDomainEvent> DomainEvents(this IStream<EventMessage> stream)
        {
            return stream.Map(es => es.Select(e =>
            {
                var de = e.Body as IDomainEvent;
                if (de != null)
                {
                    de.StreamRevision = StreamRevision(e);
                }
                return e.Body;
            }).OfType<IDomainEvent>());
        }

        public static int StreamRevision(this EventMessage e)
        {
            return (int) e.Headers["StreamRevision"];
        }

        public static void SetStreamRevision(this EventMessage e, int streamRevision)
        {
            e.Headers["StreamRevision"] = streamRevision;
        }
    }
}