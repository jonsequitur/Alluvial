using System;

namespace Alluvial
{
    internal class AnonymousDisposable : IDisposable
    {
        private readonly Action dispose;

        public AnonymousDisposable(Action dispose)
        {
            if (dispose == null)
            {
                throw new ArgumentNullException(nameof(dispose));
            }
            this.dispose = dispose;
        }

        public void Dispose()
        {
            dispose();
        }
    }
}