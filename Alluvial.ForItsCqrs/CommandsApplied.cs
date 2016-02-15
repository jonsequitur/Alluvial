using System;
using System.Collections.Generic;
using Microsoft.Its.Domain;

namespace Alluvial.Streams.ItsDomainSql
{
    public class CommandsApplied : Projection<IList<ScheduledCommandResult>, DateTimeOffset>
    {
        public CommandsApplied()
        {
            Value = new List<ScheduledCommandResult>();
        }
    }
}