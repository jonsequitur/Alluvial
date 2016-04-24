using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace Alluvial
{
    internal class AggregatorSubscriptionList : IEnumerable<IAggregatorSubscription>
    {
        private readonly ConcurrentDictionary<IAggregatorSubscription, Unit> subscriptions;

        public AggregatorSubscriptionList()
        {
            subscriptions = new ConcurrentDictionary<IAggregatorSubscription, Unit>();
        }

        public AggregatorSubscriptionList(AggregatorSubscriptionList fromSubscriptionList)
        {
            subscriptions =
                new ConcurrentDictionary<IAggregatorSubscription, Unit>(fromSubscriptionList.subscriptions);
        }

        public IEnumerator<IAggregatorSubscription> GetEnumerator() => subscriptions.Keys.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        public int Count => subscriptions.Count; 

        public void Add(IAggregatorSubscription subscription) => subscriptions.TryAdd(subscription, Unit.Default);

        public void Remove(IAggregatorSubscription subscription)
        {
            Unit _;
            subscriptions.TryRemove(subscription, out _);
        }
    }
}