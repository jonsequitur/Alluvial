using System.Data.Entity;

namespace Alluvial.Tests
{
    public class AlluvialSqlTestsDbContext : DbContext
    {
        public const string NameOrConnectionString = @"Data Source=(localdb)\MSSQLLocalDB; Integrated Security=True; MultipleActiveResultSets=False; Initial Catalog=AlluvialSqlTests";

        static AlluvialSqlTestsDbContext()
        {
            Database.SetInitializer(new DropCreateDatabaseIfModelChanges<AlluvialSqlTestsDbContext>());
        }

        public AlluvialSqlTestsDbContext() : base(NameOrConnectionString)
        {
        }

        public AlluvialSqlTestsDbContext(string nameOrConnectionString) : base(nameOrConnectionString)
        {
        }

        protected override void OnModelCreating(DbModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Event>()
                        .HasKey(e => new { e.Id, e.SequenceNumber });

            modelBuilder.Entity<Event>()
                        .Property(e => e.Id)
                        .HasMaxLength(64);

            modelBuilder.Entity<ProjectionModel>()
                        .HasKey(p => new  { p.Id, p.Pool });

            modelBuilder.Entity<ProjectionModel>()
                        .Ignore(p => p.CursorWasAdvanced);
        }

        public DbSet<Event> Events { get; set; }

        public DbSet<ProjectionModel> Projections { get; set; }
    }

    public class ProjectionModel : Projection<ProjectionModel, long>
    {
        public string Id { get; set; }

        public string Body { get; set; }

        public string Pool { get; set; }
    }
}