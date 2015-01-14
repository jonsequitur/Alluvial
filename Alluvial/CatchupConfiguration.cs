using System.Threading.Tasks;

namespace Alluvial
{
    public sealed class CatchupConfiguration
    {
        private readonly IProjectionStore<string, ICursor> defaultStore = new SingleInstanceProjectionCache<string, ICursor>(Cursor.New);

        internal CatchupConfiguration()
        {
            GetCursor = id => defaultStore.Get(id);
            StoreCursor = (id, cursor) => defaultStore.Put(id, cursor);
        }

        internal GetCursor GetCursor;

        internal StoreCursor StoreCursor;
    }

    public delegate Task<ICursor> GetCursor(string streamId);

    public delegate Task StoreCursor(string streamId, ICursor cursor);
}