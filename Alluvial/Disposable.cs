using System;

namespace Alluvial
{
    internal static class Disposable
    {
        public static IDisposable Create(Action dispose) => 
            new AnonymousDisposable(dispose);
    }
}