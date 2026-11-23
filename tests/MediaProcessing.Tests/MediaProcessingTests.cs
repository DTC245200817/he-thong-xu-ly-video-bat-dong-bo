using System.Text.Json;
using MediaProcessing.Shared.Data;
using MediaProcessing.Shared.DTOs;
using MediaProcessing.Shared.Messaging;
using MediaProcessing.Shared.Models;
using MediaProcessing.Shared.Storage;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace MediaProcessing.Tests;

public class MediaProcessingTests
{
    [Fact]
    public void Video_Initialization_ShouldHaveDefaultValues()
    {
        var video = new Video
        {
            Title = "Test Video",
            FileName = "sample.mp4",
            StoragePath = "uploads/sample.mp4",
            FileSize = 1048576
        };

        Assert.NotEqual(Guid.Empty, video.Id);
        Assert.Equal(VideoStatus.Uploaded, video.Status);
        Assert.True((DateTime.UtcNow - video.CreatedAt).TotalSeconds < 5);
        Assert.Empty(video.Jobs);
    }

    [Fact]
    public void VideoJob_Initialization_ShouldHavePendingStatusAndCorrelationId()
    {
        var videoId = Guid.NewGuid();
        var job = new VideoJob
        {
            VideoId = videoId,
            JobType = JobType.GenerateThumbnail
        };

        Assert.NotEqual(Guid.Empty, job.Id);
        Assert.Equal(videoId, job.VideoId);
        Assert.Equal(JobStatus.Pending, job.Status);
        Assert.Equal(0, job.Progress);
        Assert.False(string.IsNullOrWhiteSpace(job.CorrelationId));
        Assert.Equal(0, job.RetryCount);
    }

    [Fact]
    public async Task MediaDbContext_CanAddAndQueryVideoWithJobs()
    {
        var options = new DbContextOptionsBuilder<MediaDbContext>()
            .UseSqlite($"Data Source=test_{Guid.NewGuid():N}.db")
            .Options;

        using var db = new MediaDbContext(options);
        await db.Database.EnsureCreatedAsync();

        try
        {
            var video = new Video
            {
                Title = "Async Processing Demo",
                FileName = "demo.mp4",
                StoragePath = "uploads/demo.mp4",
                FileSize = 5000000,
                Status = VideoStatus.Processing
            };

            var job1 = new VideoJob
            {
                VideoId = video.Id,
                JobType = JobType.ExtractMetadata,
                Status = JobStatus.Completed,
                Progress = 100,
                ResultData = "{\"duration\": 120}"
            };

            var job2 = new VideoJob
            {
                VideoId = video.Id,
                JobType = JobType.Transcode720p,
                Status = JobStatus.Processing,
                Progress = 50
            };

            video.Jobs.Add(job1);
            video.Jobs.Add(job2);

            db.Videos.Add(video);
            await db.SaveChangesAsync();

            // Query back
            var savedVideo = await db.Videos
                .Include(v => v.Jobs)
                .FirstOrDefaultAsync(v => v.Id == video.Id);

            Assert.NotNull(savedVideo);
            Assert.Equal("Async Processing Demo", savedVideo.Title);
            Assert.Equal(2, savedVideo.Jobs.Count);
            Assert.Contains(savedVideo.Jobs, j => j.JobType == JobType.ExtractMetadata && j.Status == JobStatus.Completed);
            Assert.Contains(savedVideo.Jobs, j => j.JobType == JobType.Transcode720p && j.Progress == 50);
        }
        finally
        {
            await db.Database.EnsureDeletedAsync();
        }
    }

    [Fact]
    public void VideoJobMessage_Serialization_RoundTrip_PreservesProperties()
    {
        var original = new VideoJobMessage
        {
            JobId = Guid.NewGuid(),
            VideoId = Guid.NewGuid(),
            JobType = JobType.Transcode1080p,
            FilePath = "uploads/sample.mp4",
            CorrelationId = "test-corr-12345",
            RetryCount = 1,
            Timestamp = DateTime.UtcNow
        };

        var json = JsonSerializer.Serialize(original);
        var deserialized = JsonSerializer.Deserialize<VideoJobMessage>(json);

        Assert.NotNull(deserialized);
        Assert.Equal(original.JobId, deserialized.JobId);
        Assert.Equal(original.VideoId, deserialized.VideoId);
        Assert.Equal(original.JobType, deserialized.JobType);
        Assert.Equal(original.FilePath, deserialized.FilePath);
        Assert.Equal(original.CorrelationId, deserialized.CorrelationId);
        Assert.Equal(original.RetryCount, deserialized.RetryCount);
    }

    [Fact]
    public async Task LocalFileStorageService_SaveAndOpen_WorksCorrectly()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), $"storage_test_{Guid.NewGuid():N}");
        var storage = new LocalFileStorageService(tempDir);

        try
        {
            var content = "SAMPLE_VIDEO_STREAM_BYTES_CONTENT";
            using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(content));
            var relativePath = await storage.SaveFileAsync("uploads", "test.mp4", stream);

            Assert.True(storage.FileExists(relativePath));

            using var readStream = storage.OpenRead(relativePath);
            Assert.NotNull(readStream);
            using var reader = new StreamReader(readStream);
            var readContent = await reader.ReadToEndAsync();

            Assert.Equal(content, readContent);
        }
        finally
        {
            if (Directory.Exists(tempDir))
            {
                Directory.Delete(tempDir, recursive: true);
            }
        }
    }
}
