using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Its.Domain;

namespace Alluvial.For.ItsDomainSql.Tests
{
    public class AggregateA : EventSourcedAggregate<AggregateA>
    {
        public AggregateA(Guid id, IEnumerable<IEvent> eventHistory)
            : base(id, eventHistory)
        {
        }

        public AggregateA(CreateAggregateA createAggregateA) : base(createAggregateA)
        {
        }

        public abstract class Event : Event<AggregateA>
        {
            public override void Update(AggregateA aggregate)
            {
            }
        }

        public class CommandHandler : ICommandHandler<AggregateA, CreateAggregateA>
        {
            public async Task EnactCommand(AggregateA aggregate, CreateAggregateA command)
            {
                aggregate.RecordEvent(new Created());
            }

            public async Task HandleScheduledCommandException(AggregateA aggregate, CommandFailed<CreateAggregateA> command)
            {
            }
        }

        public class Created : Event
        {
        }

        public class EventType1 : Event
        {
        }

        public class EventType2 : Event
        {
        }

        public class EventType3 : Event
        {
        }

        public class EventType4 : Event
        {
        }

        public class EventType5 : Event
        {
        }

        public class EventType6 : Event
        {
        }

        public class EventType7 : Event
        {
        }

        public class EventType8 : Event
        {
        }
    }

    public class CreateAggregateA : ConstructorCommand<AggregateA>, ISpecifySchedulingBehavior
    {
        public override bool Authorize(AggregateA aggregate)
        {
            return true;
        }

        public bool CanBeDeliveredDuringScheduling
        {
            get
            {
                return false;
            }
        }

        public bool RequiresDurableScheduling
        {
            get
            {
                return true;
            }
        }
    }

    public class AggregateB : EventSourcedAggregate<AggregateB>
    {
        public AggregateB(Guid id, IEnumerable<IEvent> eventHistory)
            : base(id, eventHistory)
        {
        }

        public abstract class Event : Event<AggregateB>
        {
            public override void Update(AggregateB aggregate)
            {
            }
        }

        public class EventType1 : Event
        {
        }

        public class EventType9 : Event
        {
        }

        public class EventType10 : Event
        {
        }

        public class EventType11 : Event
        {
        }

        public class EventType12 : Event
        {
        }

        public class EventType13 : Event
        {
        }

        public class EventType14 : Event
        {
        }
    }

    public class CreateAggregateB : ConstructorCommand<AggregateB>
    {
        public override bool Authorize(AggregateB aggregate)
        {
            return true;
        }
    }
}