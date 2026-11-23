using Microsoft.EntityFrameworkCore;
using MediaProcessing.Shared.Models;

namespace MediaProcessing.Shared.Data;

public class MediaDbContext : DbContext
{
    public MediaDbContext(DbContextOptions<MediaDbContext> options) : base(options) { }

    public DbSet<Video> Videos => Set<Video>();
    public DbSet<VideoJob> VideoJobs => Set<VideoJob>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);

        modelBuilder.Entity<Video>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Title).IsRequired().HasMaxLength(255);
            entity.Property(e => e.FileName).IsRequired().HasMaxLength(255);
            entity.Property(e => e.StoragePath).IsRequired().HasMaxLength(500);
            entity.Property(e => e.Resolution).HasMaxLength(50);
            entity.Property(e => e.Format).HasMaxLength(50);

            entity.HasMany(e => e.Jobs)
                  .WithOne(j => j.Video)
                  .HasForeignKey(j => j.VideoId)
                  .OnDelete(DeleteBehavior.Cascade);
        });

        modelBuilder.Entity<VideoJob>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.HasIndex(e => e.VideoId);
            entity.HasIndex(e => e.Status);
            entity.HasIndex(e => e.CorrelationId);
            entity.Property(e => e.CorrelationId).HasMaxLength(100);
        });
    }
}
