using System.Collections.Generic;
using Microsoft.Its.Domain;

namespace Alluvial.Streams.ItsDomainSql
{
    public class CommandsApplied : Projection<IList<ScheduledCommandResult>, long>
    {
        public CommandsApplied()
        {
            Value = new List<ScheduledCommandResult>();
        }
    }
}