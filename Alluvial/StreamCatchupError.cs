using System;

namespace Alluvial
{
    public class StreamCatchupError<TProjection>
    {
        public Exception Exception { get; internal set; }
        public TProjection Projection { get; internal set; }
        public ICursor Cursor { get; internal set; }
        
        internal bool ShouldContinue { get; private set; }

        public void Continue()
        {
            ShouldContinue = true;
        }

        public void Throw()
        {
            ShouldContinue = false;
        }
    }
}