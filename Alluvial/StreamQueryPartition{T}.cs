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
                throw new ArgumentNullException(nameof(contains));
            }
            if (name == null)
            {
                throw new ArgumentNullException(nameof(name));
            }
            this.contains = contains;
            this.name = name;
        }

        public bool Contains(TPartition value) => contains(value);

        public override string ToString() => $"partition:{name}";
    }
}