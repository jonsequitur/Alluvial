using System;
using System.Collections.Generic;
using Microsoft.Its.Domain;

namespace Alluvial.For.ItsDomainSql
{
    public class CommandsApplied : Projection<IList<ScheduledCommandResult>, DateTimeOffset>
    {
        public CommandsApplied()
        {
            Value = new List<ScheduledCommandResult>();
        }
    }
}