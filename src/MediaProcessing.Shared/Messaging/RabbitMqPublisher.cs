using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using Polly;

namespace MediaProcessing.Shared.Messaging;

public interface IRabbitMqPublisher
{
    Task PublishJobAsync(VideoJobMessage message);
}

public class RabbitMqPublisher : IRabbitMqPublisher, IDisposable
{
    private readonly IConnection _connection;
    private readonly IModel _channel;
    private readonly ILogger<RabbitMqPublisher> _logger;

    public RabbitMqPublisher(IConnection connection, ILogger<RabbitMqPublisher> logger)
    {
        _connection = connection;
        _logger = logger;
        _channel = _connection.CreateModel();

        // Declare main exchange & DLX
        _channel.ExchangeDeclare(RabbitMqConstants.ExchangeName, ExchangeType.Direct, durable: true);
        _channel.ExchangeDeclare(RabbitMqConstants.DlxExchangeName, ExchangeType.Direct, durable: true);

        // Declare DLQ queue
        _channel.QueueDeclare(RabbitMqConstants.DlqQueueName, durable: true, exclusive: false, autoDelete: false);
        _channel.QueueBind(RabbitMqConstants.DlqQueueName, RabbitMqConstants.DlxExchangeName, RabbitMqConstants.DlqRoutingKey);

        // Declare Main Queue with Dead Letter Exchange configuration
        var queueArgs = new Dictionary<string, object>
        {
            { "x-dead-letter-exchange", RabbitMqConstants.DlxExchangeName },
            { "x-dead-letter-routing-key", RabbitMqConstants.DlqRoutingKey }
        };

        _channel.QueueDeclare(RabbitMqConstants.JobsQueueName, durable: true, exclusive: false, autoDelete: false, arguments: queueArgs);
        _channel.QueueBind(RabbitMqConstants.JobsQueueName, RabbitMqConstants.ExchangeName, RabbitMqConstants.JobRoutingKey);
    }

    public Task PublishJobAsync(VideoJobMessage message)
    {
        var body = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(message));

        var properties = _channel.CreateBasicProperties();
        properties.Persistent = true;
        properties.CorrelationId = message.CorrelationId;
        properties.Type = message.JobType.ToString();
        properties.Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds());

        _channel.BasicPublish(
            exchange: RabbitMqConstants.ExchangeName,
            routingKey: RabbitMqConstants.JobRoutingKey,
            basicProperties: properties,
            body: body
        );

        _logger.LogInformation(" [x] Published VideoJob: {JobId} ({JobType}) for Video: {VideoId} [CorrelationId: {CorrelationId}]",
            message.JobId, message.JobType, message.VideoId, message.CorrelationId);

        return Task.CompletedTask;
    }

    public void Dispose()
    {
        _channel?.Dispose();
    }
}
