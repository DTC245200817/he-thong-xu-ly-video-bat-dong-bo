using MediaProcessing.Shared.Data;
using MediaProcessing.Shared.Storage;
using MediaWorker;
using Microsoft.EntityFrameworkCore;
using Polly;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

var builder = Host.CreateApplicationBuilder(args);

// File Storage Service
var storageBasePath = Environment.GetEnvironmentVariable("STORAGE_PATH") 
    ?? builder.Configuration["Storage:BasePath"] 
    ?? "storage";
builder.Services.AddSingleton<IFileStorageService>(_ => new LocalFileStorageService(storageBasePath));

// Database Context Setup
var pgConnStr = Environment.GetEnvironmentVariable("POSTGRES_CONNECTION_STRING") 
    ?? builder.Configuration.GetConnectionString("Postgres");
var sqliteConnStr = builder.Configuration.GetConnectionString("Sqlite") ?? "Data Source=storage/media.db";

builder.Services.AddDbContext<MediaDbContext>(options =>
{
    var usePostgres = Environment.GetEnvironmentVariable("USE_POSTGRES") == "true" 
        || (!string.IsNullOrEmpty(pgConnStr) && pgConnStr.Contains("Host="));
    
    if (usePostgres && !string.IsNullOrEmpty(pgConnStr))
    {
        options.UseNpgsql(pgConnStr);
    }
    else
    {
        options.UseSqlite(sqliteConnStr);
    }
});

// RabbitMQ Connection with Retry Policy
builder.Services.AddSingleton<IConnection>(sp =>
{
    var logger = sp.GetRequiredService<ILogger<Program>>();
    var hostName = Environment.GetEnvironmentVariable("RABBITMQ_HOST") 
        ?? builder.Configuration["RabbitMQ:HostName"] 
        ?? "localhost";
    var port = int.TryParse(Environment.GetEnvironmentVariable("RABBITMQ_PORT") 
        ?? builder.Configuration["RabbitMQ:Port"], out var p) ? p : 5672;
    var user = Environment.GetEnvironmentVariable("RABBITMQ_USER") 
        ?? builder.Configuration["RabbitMQ:UserName"] 
        ?? "guest";
    var pass = Environment.GetEnvironmentVariable("RABBITMQ_PASS") 
        ?? builder.Configuration["RabbitMQ:Password"] 
        ?? "guest";

    var factory = new ConnectionFactory
    {
        HostName = hostName,
        Port = port,
        UserName = user,
        Password = pass,
        DispatchConsumersAsync = true,
        AutomaticRecoveryEnabled = true
    };

    var retryPolicy = Policy
        .Handle<BrokerUnreachableException>()
        .Or<Exception>()
        .WaitAndRetry(
            retryCount: 10,
            sleepDurationProvider: retryAttempt => TimeSpan.FromSeconds(Math.Min(Math.Pow(2, retryAttempt), 15)),
            onRetry: (exception, timeSpan, retryCount, context) =>
            {
                logger.LogWarning("Worker RabbitMQ connection attempt {RetryCount} failed: {Message}. Retrying in {Delay}s...", 
                    retryCount, exception.Message, timeSpan.TotalSeconds);
            });

    return retryPolicy.Execute(() =>
    {
        logger.LogInformation("Worker connecting to RabbitMQ at {Host}:{Port}...", hostName, port);
        return factory.CreateConnection();
    });
});

// Add Hosted Consumer Service
builder.Services.AddHostedService<MediaJobConsumer>();

var host = builder.Build();

// Ensure Database exists
using (var scope = host.Services.CreateScope())
{
    var db = scope.ServiceProvider.GetRequiredService<MediaDbContext>();
    var logger = scope.ServiceProvider.GetRequiredService<ILogger<Program>>();
    try
    {
        db.Database.EnsureCreated();
        logger.LogInformation("Worker verified database connection.");
    }
    catch (Exception ex)
    {
        logger.LogError(ex, "Worker database connection check error: {Message}", ex.Message);
    }
}

host.Run();
