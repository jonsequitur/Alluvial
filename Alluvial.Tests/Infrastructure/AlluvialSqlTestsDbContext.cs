using System.Data.Entity;

namespace Alluvial.Tests
{
    public class AlluvialSqlTestsDbContext : DbContext
    {
        static AlluvialSqlTestsDbContext()
        {
            Database.SetInitializer(new AlluvialSqlTestsDbInitializer());
        }

        public AlluvialSqlTestsDbContext() : base(@"Data Source=(localdb)\MSSQLLocalDB; Integrated Security=True; MultipleActiveResultSets=False; Initial Catalog=AlluvialSqlTests")
        {
        }

        protected override void OnModelCreating(DbModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Event>()
                        .HasKey(e => new { e.Id, e.SequenceNumber });

            modelBuilder.Entity<Event>()
                        .Property(e => e.Id)
                        .HasMaxLength(64);
        }

        public DbSet<Event> Events { get; set; }

        public class AlluvialSqlTestsDbInitializer : CreateDatabaseIfNotExists<AlluvialSqlTestsDbContext>
        {
        }
    }
}