using System.Collections.Generic;
using System.Linq;
using Its.Log.Instrumentation;
using Microsoft.Its.Domain;

namespace Alluvial.Streams.ItsDomainSql.Tests
{
    public class MatchingEvents : Projection<List<IEvent>, long>
    {
        public MatchingEvents()
        {
            Value = new EventList();
        }

        private class EventList : List<IEvent>
        {
            public override string ToString()
            {
                return this
                    .OfType<Event>()
                    .Select(e => new
                    {
                        e.Metadata.AbsoluteSequenceNumber,
                        e.AggregateId,
                        Type = e.EventName()
                    })
                    .ToLogString();
            }
        }
    }
}