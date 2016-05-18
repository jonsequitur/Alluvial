using System;
using System.Data.Entity;
using System.Threading.Tasks;

namespace Alluvial.For.ItsDomainSql
{
    public static class SqlStorageFor
    {
        public static FetchAndSave<ICursor<long>> Cursor<TDbContext>(
            string streamId,
            Func<TDbContext> createDbContext = null)
            where TDbContext : DbContext, new()
        {
            createDbContext = createDbContext ?? (() => new TDbContext());

            return async (partitionId, callAggregatorPipeline) =>
            {
                using (var db = createDbContext())
                {
                    await db.Database.Connection.OpenAsync(
                        backoff: TimeSpan.FromMilliseconds(100),
                        numberOfRetries: 3);

                    var projection = await db.Set<PartitionCursor>()
                                             .SingleOrDefaultAsync(p => p.StreamId == streamId &&
                                                                        p.PartitionId == partitionId);

                    if (projection == null)
                    {
                        projection = new PartitionCursor
                        {
                            StreamId = streamId,
                            PartitionId = partitionId
                        };

                        db.Set<PartitionCursor>().Add(projection);
                    }

                    await callAggregatorPipeline(projection);

                    await db.SaveChangesAsync();
                }
            };
        }

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
                    await db.Database.Connection.OpenAsync(
                        backoff: TimeSpan.FromMilliseconds(100),
                        numberOfRetries: 3);

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