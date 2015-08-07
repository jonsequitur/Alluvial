using System;
using System.Diagnostics;

namespace Alluvial
{
    [DebuggerStepThrough]
    [DebuggerDisplay("{ToString()}")]
    internal class StreamQueryValuePartition<TPartition> :
        StreamQueryPartition<TPartition>,
        IStreamQueryValuePartition<TPartition>
    {
        private readonly TPartition value;

        public StreamQueryValuePartition(TPartition value) :
            base(v => v.Equals(value), value.ToString())
        {
            if (value == null)
            {
                throw new ArgumentNullException("value");
            }

            this.value = value;
        }

        public TPartition Value
        {
            get
            {
                return value;
            }
        }
    }
}