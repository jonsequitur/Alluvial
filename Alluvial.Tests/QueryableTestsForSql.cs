using System;
using System.Linq;
using System.Threading.Tasks;

namespace Alluvial.Tests
{
    public class QueryableTestsForSql : QueryableTests
    {
        protected override async Task WriteEvents(Func<int, Event> createEvent, int howMany = 100)
        {
            using (var db = new AlluvialSqlTestsDbContext())
            {
                for (var i = 1; i <= howMany; i++)
                {
                    db.Events.Add(createEvent(i));
                }
                await db.SaveChangesAsync();
            }
        }

        protected override IQueryable<Event> Events()
        {
            return new AlluvialSqlTestsDbContext().Events;
        }
    }
}