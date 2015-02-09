using System;
using System.Reactive.Linq;

namespace Alluvial.Tests
{
    public static class Background
    {
        public static IDisposable Loop(
            Action<int> action,
            double rateInMilliseconds = 1)
        {
            var count = 0;

            return Observable.Timer(TimeSpan.FromMilliseconds(rateInMilliseconds))
                             .Repeat()
                             .Subscribe(i =>
                             {
                                 count++;
                                 action(count);
                             });
        }
    }
}