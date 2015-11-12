using System;
using System.Collections.Generic;
using Microsoft.Its.Domain;

namespace Alluvial.ForItsCqrs.Tests
{
    partial class AggregateA : EventSourcedAggregate<AggregateA>
    {
        public AggregateA(Guid id, IEnumerable<IEvent> eventHistory)
            : base(id, eventHistory)
        {
        }

        public abstract class Event : Event<AggregateA>
        {
            public override void Update(AggregateA aggregate)
            {
                throw new NotImplementedException();
            }
        }
    }

    partial class AggregateB : EventSourcedAggregate<AggregateB>
    {
        public AggregateB(Guid id, IEnumerable<IEvent> eventHistory)
            : base(id, eventHistory)
        {
        }

        public abstract class Event : Event<AggregateB>
        {
            public override void Update(AggregateB aggregate)
            {
                throw new NotImplementedException();
            }
        }
    }

    public partial class AggregateA
    {
        public class EventType1 : Event
        {
        }
    }

    public partial class AggregateA
    {
        public class EventType2 : Event
        {
        }
    }

    public partial class AggregateA
    {
        public class EventType3 : Event
        {
        }
    }

    public partial class AggregateA
    {
        public class EventType4 : Event
        {
        }
    }

    public partial class AggregateA
    {
        public class EventType5 : Event
        {
        }
    }

    public partial class AggregateA
    {
        public class EventType6 : Event
        {
        }
    }

    public partial class AggregateA
    {
        public class EventType7 : Event
        {
        }
    }

    public partial class AggregateA
    {
        public class EventType8 : Event
        {
        }
    }

    public partial class AggregateB
    {
        public class EventType1 : Event
        {
        }
    }

    public partial class AggregateB
    {
        public class EventType9 : Event
        {
        }
    }

    public partial class AggregateB
    {
        public class EventType10 : Event
        {
        }
    }

    public partial class AggregateB
    {
        public class EventType11 : Event
        {
        }
    }

    public partial class AggregateB
    {
        public class EventType12 : Event
        {
        }
    }

    public partial class AggregateB
    {
        public class EventType13 : Event
        {
        }
    }

    public partial class AggregateB
    {
        public class EventType14 : Event
        {
        }
    }
}