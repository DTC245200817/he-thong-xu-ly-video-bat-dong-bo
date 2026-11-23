using System.Text;
using System.Text.Json;
using MediaProcessing.Shared.Data;
using MediaProcessing.Shared.Messaging;
using MediaProcessing.Shared.Models;
using MediaProcessing.Shared.Storage;
using Microsoft.EntityFrameworkCore;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace MediaWorker;

public class MediaJobConsumer : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly IConnection _connection;
    private readonly IFileStorageService _storage;
    private readonly ILogger<MediaJobConsumer> _logger;
    private IModel? _channel;

    public MediaJobConsumer(
        IServiceProvider serviceProvider,
        IConnection connection,
        IFileStorageService storage,
        ILogger<MediaJobConsumer> logger)
    {
        _serviceProvider = serviceProvider;
        _connection = connection;
        _storage = storage;
        _logger = logger;
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _channel = _connection.CreateModel();

        // Declare main exchange & DLX
        _channel.ExchangeDeclare(RabbitMqConstants.ExchangeName, ExchangeType.Direct, durable: true);
        _channel.ExchangeDeclare(RabbitMqConstants.DlxExchangeName, ExchangeType.Direct, durable: true);

        // Declare DLQ queue
        _channel.QueueDeclare(RabbitMqConstants.DlqQueueName, durable: true, exclusive: false, autoDelete: false);
        _channel.QueueBind(RabbitMqConstants.DlqQueueName, RabbitMqConstants.DlxExchangeName, RabbitMqConstants.DlqRoutingKey);

        // Declare main queue with DLX bindings
        var queueArgs = new Dictionary<string, object>
        {
            { "x-dead-letter-exchange", RabbitMqConstants.DlxExchangeName },
            { "x-dead-letter-routing-key", RabbitMqConstants.DlqRoutingKey }
        };

        _channel.QueueDeclare(RabbitMqConstants.JobsQueueName, durable: true, exclusive: false, autoDelete: false, arguments: queueArgs);
        _channel.QueueBind(RabbitMqConstants.JobsQueueName, RabbitMqConstants.ExchangeName, RabbitMqConstants.JobRoutingKey);

        // Fair dispatch
        _channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);

        var consumer = new AsyncEventingBasicConsumer(_channel);
        consumer.Received += OnMessageReceivedAsync;

        _channel.BasicConsume(queue: RabbitMqConstants.JobsQueueName, autoAck: false, consumer: consumer);
        _logger.LogInformation(" [*] MediaWorker listening for messages on '{QueueName}'...", RabbitMqConstants.JobsQueueName);

        return Task.CompletedTask;
    }

    private async Task OnMessageReceivedAsync(object sender, BasicDeliverEventArgs ea)
    {
        var body = ea.Body.ToArray();
        var json = Encoding.UTF8.GetString(body);

        VideoJobMessage? message = null;
        try
        {
            message = JsonSerializer.Deserialize<VideoJobMessage>(json);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to deserialize message: {Json}", json);
            _channel?.BasicReject(ea.DeliveryTag, requeue: false);
            return;
        }

        if (message == null)
        {
            _channel?.BasicReject(ea.DeliveryTag, requeue: false);
            return;
        }

        _logger.LogInformation("[{CorrelationId}] Received VideoJob: {JobId} ({JobType}) for Video: {VideoId}",
            message.CorrelationId, message.JobId, message.JobType, message.VideoId);

        using var scope = _serviceProvider.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<MediaDbContext>();

        // 1. Idempotency Check
        var job = await db.VideoJobs.Include(j => j.Video).FirstOrDefaultAsync(j => j.Id == message.JobId);
        if (job == null)
        {
            _logger.LogWarning("[{CorrelationId}] Job {JobId} not found in database. Acknowledging message.", message.CorrelationId, message.JobId);
            _channel?.BasicAck(ea.DeliveryTag, multiple: false);
            return;
        }

        if (job.Status == JobStatus.Completed || job.Status == JobStatus.Failed)
        {
            _logger.LogInformation("[{CorrelationId}] Job {JobId} already finished with status '{Status}'. Idempotent skip.",
                message.CorrelationId, job.Id, job.Status);
            _channel?.BasicAck(ea.DeliveryTag, multiple: false);
            return;
        }

        try
        {
            // 2. Mark Processing
            job.Status = JobStatus.Processing;
            job.StartedAt = DateTime.UtcNow;
            job.Progress = 10;
            await db.SaveChangesAsync();

            // 3. Process according to JobType
            switch (message.JobType)
            {
                case JobType.ExtractMetadata:
                    await ProcessExtractMetadataAsync(job, db);
                    break;

                case JobType.GenerateThumbnail:
                    await ProcessGenerateThumbnailAsync(job, db);
                    break;

                case JobType.Transcode480p:
                case JobType.Transcode720p:
                case JobType.Transcode1080p:
                    await ProcessTranscodeAsync(job, db, message.JobType);
                    break;

                default:
                    throw new NotSupportedException($"Unsupported JobType: {message.JobType}");
            }

            // 4. Mark Job Completed
            job.Status = JobStatus.Completed;
            job.Progress = 100;
            job.CompletedAt = DateTime.UtcNow;

            // Check if all jobs for video are done
            if (job.Video != null)
            {
                var remainingJobs = await db.VideoJobs
                    .Where(j => j.VideoId == job.VideoId && j.Id != job.Id && j.Status != JobStatus.Completed)
                    .CountAsync();

                if (remainingJobs == 0)
                {
                    job.Video.Status = VideoStatus.Ready;
                    job.Video.UpdatedAt = DateTime.UtcNow;
                }
            }

            await db.SaveChangesAsync();

            _logger.LogInformation("[{CorrelationId}] Successfully completed Job: {JobId} ({JobType})",
                message.CorrelationId, job.Id, job.JobType);

            _channel?.BasicAck(ea.DeliveryTag, multiple: false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "[{CorrelationId}] Error executing Job {JobId}: {Message}", 
                message.CorrelationId, message.JobId, ex.Message);

            job.RetryCount++;
            if (job.RetryCount >= 3)
            {
                job.Status = JobStatus.Failed;
                job.ErrorMessage = ex.Message;
                job.CompletedAt = DateTime.UtcNow;
                await db.SaveChangesAsync();

                _logger.LogWarning("[{CorrelationId}] Job {JobId} exceeded max retries. Routing to DLQ.", 
                    message.CorrelationId, job.Id);
                
                // Reject without requeue routes to Dead Letter Queue (DLQ)
                _channel?.BasicReject(ea.DeliveryTag, requeue: false);
            }
            else
            {
                await db.SaveChangesAsync();
                _logger.LogInformation("[{CorrelationId}] Job {JobId} will be requeued (Retry {Count}/3).",
                    message.CorrelationId, job.Id, job.RetryCount);

                _channel?.BasicReject(ea.DeliveryTag, requeue: true);
            }
        }
    }

    private async Task ProcessExtractMetadataAsync(VideoJob job, MediaDbContext db)
    {
        _logger.LogInformation("Processing metadata extraction for Video: {VideoId}", job.VideoId);
        
        await Task.Delay(800); // Simulate processing time
        job.Progress = 50;
        await db.SaveChangesAsync();

        var video = job.Video;
        var duration = 120.0; // default simulated 2 minutes
        var resolution = "1920x1080 (Full HD)";
        var format = "mp4";

        if (video != null)
        {
            format = Path.GetExtension(video.FileName).TrimStart('.').ToLowerInvariant();
            if (string.IsNullOrEmpty(format)) format = "mp4";

            if (video.FileSize > 0)
            {
                // Realistic calculation: assume average 2MB/10s bitrate
                duration = Math.Max(10.0, Math.Round((double)video.FileSize / (1024 * 1024) * 8.5, 1));
            }

            video.DurationSeconds = duration;
            video.Resolution = resolution;
            video.Format = format;
            video.UpdatedAt = DateTime.UtcNow;
        }

        var result = new
        {
            durationSeconds = duration,
            resolution,
            format,
            codec = "H.264 / AAC",
            bitrate = "4500 kbps",
            frameRate = 30.0
        };

        job.ResultData = JsonSerializer.Serialize(result);
    }

    private async Task ProcessGenerateThumbnailAsync(VideoJob job, MediaDbContext db)
    {
        _logger.LogInformation("Generating thumbnail for Video: {VideoId}", job.VideoId);
        
        await Task.Delay(1000); // Simulate rendering
        job.Progress = 60;
        await db.SaveChangesAsync();

        var title = job.Video?.Title ?? "Video Thumbnail";
        var fileName = $"thumb_{job.VideoId}_{DateTime.UtcNow.Ticks}.svg";
        
        // Generate high quality SVG thumbnail badge
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

        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(svgContent));
        var relativePath = await _storage.SaveFileAsync("thumbnails", fileName, stream);

        var result = new
        {
            thumbnailPath = relativePath,
            width = 640,
            height = 360,
            format = "svg"
        };

        job.ResultData = JsonSerializer.Serialize(result);
    }

    private async Task ProcessTranscodeAsync(VideoJob job, MediaDbContext db, JobType jobType)
    {
        var quality = jobType switch
        {
            JobType.Transcode480p => "480p",
            JobType.Transcode720p => "720p",
            JobType.Transcode1080p => "1080p",
            _ => "standard"
        };

        _logger.LogInformation("Starting transcoding to {Quality} for Video: {VideoId}", quality, job.VideoId);

        // Progress stepping (simulating real transcode chunks)
        for (int p = 20; p <= 90; p += 25)
        {
            await Task.Delay(1000);
            job.Progress = p;
            await db.SaveChangesAsync();
            _logger.LogInformation("[Job {JobId}] Transcoding progress: {Progress}%", job.Id, p);
        }

        var fileName = $"transcoded_{quality}_{job.VideoId}.mp4";
        var dummyData = $"TRANSCODED_VIDEO_{quality}_CONTENT_{DateTime.UtcNow:O}";
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(dummyData));
        var relativePath = await _storage.SaveFileAsync("transcoded", fileName, stream);

        var result = new
        {
            quality,
            transcodedPath = relativePath,
            outputFileName = fileName,
            sizeBytes = stream.Length,
            status = "Success"
        };

        job.ResultData = JsonSerializer.Serialize(result);
    }

    public override void Dispose()
    {
        _channel?.Dispose();
        base.Dispose();
    }
}
