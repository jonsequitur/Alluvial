using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Alluvial.Tests
{
    public class QueryableTestsForInMemoryQueryable : QueryableTests
    {
        private readonly List<Event> events = new List<Event>();

        protected override async Task WriteEvents(Func<int, Event> createEvent, int howMany = 100)
        {
            for (var i = 1; i <= howMany; i++)
            {
                events.Add(createEvent(i));
            }
        }

        protected override IQueryable<Event> Events()
        {
            return events.AsAsyncQueryable();
        }
    }
}