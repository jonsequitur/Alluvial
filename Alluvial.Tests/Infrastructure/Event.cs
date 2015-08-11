using System;

namespace Alluvial.Tests
{
    public class Event
    {
        public Event()
        {
            Timestamp = DateTimeOffset.Now;
            Guid = Guid.NewGuid();
        }

        public string Id { get; set; }

        public int SequenceNumber { get; set; }

        public DateTimeOffset Timestamp { get; set; }

        public Guid Guid { get; set; }

        public string Body { get; set; }
    }
}