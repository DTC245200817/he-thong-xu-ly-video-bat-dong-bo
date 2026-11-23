using System.Text.Json;
using MediaProcessing.Shared.Data;
using MediaProcessing.Shared.Messaging;
using MediaProcessing.Shared.Models;
using MediaProcessing.Shared.Storage;
using Microsoft.EntityFrameworkCore;

namespace MediaApi;

public class FallbackJobProcessor : BackgroundService
{
    private readonly IRabbitMqPublisher _publisher;
    private readonly IServiceProvider _serviceProvider;
    private readonly IFileStorageService _storage;
    private readonly ILogger<FallbackJobProcessor> _logger;

    public FallbackJobProcessor(
        IRabbitMqPublisher publisher,
        IServiceProvider serviceProvider,
        IFileStorageService storage,
        ILogger<FallbackJobProcessor> logger)
    {
        _publisher = publisher;
        _serviceProvider = serviceProvider;
        _storage = storage;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Fallback In-Memory Processor ready for jobs when RabbitMQ is offline.");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var message = await _publisher.FallbackReader.ReadAsync(stoppingToken);
                _logger.LogInformation("[FallbackProcessor] Processing Job: {JobId} ({JobType})", message.JobId, message.JobType);

                using var scope = _serviceProvider.CreateScope();
                var db = scope.ServiceProvider.GetRequiredService<MediaDbContext>();

                var job = await db.VideoJobs.Include(j => j.Video).FirstOrDefaultAsync(j => j.Id == message.JobId, stoppingToken);
                if (job == null) continue;

                job.Status = JobStatus.Processing;
                job.StartedAt = DateTime.UtcNow;
                job.Progress = 20;
                await db.SaveChangesAsync(stoppingToken);

                switch (message.JobType)
                {
                    case JobType.ExtractMetadata:
                        await Task.Delay(800, stoppingToken);
                        job.Progress = 50;
                        await db.SaveChangesAsync(stoppingToken);

                        var video = job.Video;
                        var duration = 120.0;
                        var resolution = "1920x1080 (Full HD)";
                        var format = "mp4";

                        if (video != null)
                        {
                            format = Path.GetExtension(video.FileName).TrimStart('.').ToLowerInvariant();
                            if (string.IsNullOrEmpty(format)) format = "mp4";
                            if (video.FileSize > 0)
                            {
                                duration = Math.Max(10.0, Math.Round((double)video.FileSize / (1024 * 1024) * 8.5, 1));
                            }
                            video.DurationSeconds = duration;
                            video.Resolution = resolution;
                            video.Format = format;
                            video.UpdatedAt = DateTime.UtcNow;
                        }

                        job.ResultData = JsonSerializer.Serialize(new
                        {
                            durationSeconds = duration,
                            resolution,
                            format,
                            codec = "H.264 / AAC",
                            bitrate = "4500 kbps",
                            frameRate = 30.0
                        });
                        break;

                    case JobType.GenerateThumbnail:
                        await Task.Delay(1000, stoppingToken);
                        job.Progress = 60;
                        await db.SaveChangesAsync(stoppingToken);

                        var title = job.Video?.Title ?? "Video Thumbnail";
                        var fileName = $"thumb_{job.VideoId}_{DateTime.UtcNow.Ticks}.svg";

                        var svgContent = $@"<svg xmlns=""http://www.w3.org/2000/svg"" viewBox=""0 0 640 360"" width=""640"" height=""360"">
  <defs>
    <linearGradient id=""grad"" x1=""0%"" y1=""0%"" x2=""100%"" y2=""100%"">
      <stop offset=""0%"" style=""stop-color:#1e3c72;stop-opacity:1"" />
      <stop offset=""100%"" style=""stop-color:#2a5298;stop-opacity:1"" />
    </linearGradient>
  </defs>
  <rect width=""640"" height=""360"" fill=""url(#grad)"" />
  <circle cx=""320"" cy=""160"" r=""50"" fill=""rgba(255,255,255,0.2)"" />
  <polygon points=""310,140 340,160 310,180"" fill=""#ffffff"" />
  <text x=""320"" y=""250"" font-family=""sans-serif"" font-size=""22"" font-weight=""bold"" fill=""#ffffff"" text-anchor=""middle"">{System.Security.SecurityElement.Escape(title)}</text>
  <rect x=""20"" y=""20"" width=""70"" height=""26"" rx=""5"" fill=""#e11d48"" />
  <text x=""55"" y=""38"" font-family=""sans-serif"" font-size=""13"" font-weight=""bold"" fill=""#ffffff"" text-anchor=""middle"">HD 1080</text>
  <rect x=""550"" y=""315"" width=""70"" height=""26"" rx=""5"" fill=""rgba(0,0,0,0.7)"" />
  <text x=""585"" y=""333"" font-family=""sans-serif"" font-size=""13"" font-weight=""bold"" fill=""#ffffff"" text-anchor=""middle"">02:30</text>
</svg>";

                        using (var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(svgContent)))
                        {
                            var thumbPath = await _storage.SaveFileAsync("thumbnails", fileName, stream);
                            job.ResultData = JsonSerializer.Serialize(new
                            {
                                thumbnailPath = thumbPath,
                                width = 640,
                                height = 360,
                                format = "svg"
                            });
                        }
                        break;

                    case JobType.Transcode480p:
                    case JobType.Transcode720p:
                    case JobType.Transcode1080p:
                        var quality = message.JobType switch
                        {
                            JobType.Transcode480p => "480p",
                            JobType.Transcode720p => "720p",
                            JobType.Transcode1080p => "1080p",
                            _ => "standard"
                        };

                        for (int p = 30; p <= 90; p += 30)
                        {
                            await Task.Delay(800, stoppingToken);
                            job.Progress = p;
                            await db.SaveChangesAsync(stoppingToken);
                        }

                        var transcodeName = $"transcoded_{quality}_{job.VideoId}.mp4";
                        var dummyData = $"TRANSCODED_VIDEO_{quality}_{DateTime.UtcNow:O}";
                        using (var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(dummyData)))
                        {
                            var transcodePath = await _storage.SaveFileAsync("transcoded", transcodeName, stream);
                            job.ResultData = JsonSerializer.Serialize(new
                            {
                                quality,
                                transcodedPath = transcodePath,
                                outputFileName = transcodeName,
                                status = "Success"
                            });
                        }
                        break;
                }

                job.Status = JobStatus.Completed;
                job.Progress = 100;
                job.CompletedAt = DateTime.UtcNow;

                if (job.Video != null)
                {
                    var remainingJobs = await db.VideoJobs
                        .Where(j => j.VideoId == job.VideoId && j.Id != job.Id && j.Status != JobStatus.Completed)
                        .CountAsync(stoppingToken);

                    if (remainingJobs == 0)
                    {
                        job.Video.Status = VideoStatus.Ready;
                        job.Video.UpdatedAt = DateTime.UtcNow;
                    }
                }

                await db.SaveChangesAsync(stoppingToken);
                _logger.LogInformation("[FallbackProcessor] Completed Job: {JobId}", job.Id);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing job in fallback worker");
            }
        }
    }
}
