using System;
using System.Data.Entity;
using System.Threading.Tasks;

namespace Alluvial.Streams.ItsDomainSql
{
    public static class SqlStorageFor
    {
        public static FetchAndSave<TProjection> Projection<TProjection, TDbContext>(
            Func<TDbContext, string, Task<TProjection>> getSingle,
            Func<string, TProjection> createNew,
            Func<TDbContext> createDbContext = null)
            where TProjection : class
            where TDbContext : DbContext, new()
        {
            createDbContext = createDbContext ?? (() => new TDbContext());

            return async (projectionId, callAggregatorPipeline) =>
            {
                using (var db = createDbContext())
                {
                    var projection = await getSingle(db, projectionId);

                    if (projection == null)
                    {
                        projection = createNew(projectionId);
                        db.Set<TProjection>().Add(projection);
                    }

                    await callAggregatorPipeline(projection);

                    await db.SaveChangesAsync();
                }
            };
        }
    }
}