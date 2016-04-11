using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Alluvial
{
    internal static class Grouping
    {
        public static IGrouping<TKey, TElement> Create<TKey, TElement>(TKey key, IEnumerable<TElement> elements)
        {
            return new Of<TKey, TElement>(key, elements);
        }

        private class Of<TKey, TElement> : IGrouping<TKey, TElement>
        {
            private readonly IEnumerable<TElement> elements;

            public Of(TKey key, IEnumerable<TElement> elements)
            {
                if (key == null)
                {
                    throw new ArgumentNullException(nameof(key));
                }
                if (elements == null)
                {
                    throw new ArgumentNullException(nameof(elements));
                }
                this.elements = elements;
                Key = key;
            }

            public IEnumerator<TElement> GetEnumerator()
            {
                return elements.GetEnumerator();
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }

            public TKey Key { get; }
        }
    }
}