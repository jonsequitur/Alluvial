using System;
using System.Diagnostics;

namespace Alluvial
{
    [DebuggerStepThrough]
    [DebuggerDisplay("{ToString()}")]
    internal class StreamQueryPartition<TPartition> : IStreamQueryPartition<TPartition>
    {
        private readonly Func<TPartition, bool> contains;
        private readonly string name;

        public StreamQueryPartition(
            Func<TPartition, bool> contains,
            string name)
        {
            if (contains == null)
            {
                throw new ArgumentNullException("contains");
            }
            if (name == null)
            {
                throw new ArgumentNullException("name");
            }
            this.contains = contains;
            this.name = name;
        }

        public bool Contains(TPartition value)
        {
            return contains(value);
        }

        public override string ToString()
        {
            return string.Format("partition:{0}", name);
        }
    }
}