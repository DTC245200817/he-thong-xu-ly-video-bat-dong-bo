using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace MediaProcessing.Shared.Messaging;

public interface IRabbitMqPublisher
{
    bool IsConnected { get; }
    Task PublishJobAsync(VideoJobMessage message);
    ChannelReader<VideoJobMessage> FallbackReader { get; }
}

public class RabbitMqPublisher : IRabbitMqPublisher, IDisposable
{
    private readonly IConnection? _connection;
    private readonly IModel? _channel;
    private readonly ILogger<RabbitMqPublisher> _logger;
    private readonly Channel<VideoJobMessage> _fallbackChannel;

    public bool IsConnected => _channel != null && _channel.IsOpen;
    public ChannelReader<VideoJobMessage> FallbackReader => _fallbackChannel.Reader;

    public RabbitMqPublisher(IConnection? connection, ILogger<RabbitMqPublisher> logger)
    {
        _connection = connection;
        _logger = logger;
        _fallbackChannel = Channel.CreateUnbounded<VideoJobMessage>();

        if (_connection != null && _connection.IsOpen)
        {
            try
            {
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

                _logger.LogInformation("✓ RabbitMQ Publisher initialized and connected successfully.");
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "! RabbitMQ exchange setup failed. Falling back to in-memory async queue.");
                _channel = null;
            }
        }
        else
        {
            _logger.LogWarning("! RabbitMQ is not available. Using In-Memory Fallback Channel (Graceful Degradation).");
        }
    }

    public async Task PublishJobAsync(VideoJobMessage message)
    {
        if (IsConnected && _channel != null)
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

            _logger.LogInformation(" [x] Published VideoJob to RabbitMQ: {JobId} ({JobType}) [CorrelationId: {CorrelationId}]",
                message.JobId, message.JobType, message.CorrelationId);
        }
        else
        {
            await _fallbackChannel.Writer.WriteAsync(message);
            _logger.LogInformation(" [x] Queued VideoJob to Local Async Channel: {JobId} ({JobType}) [CorrelationId: {CorrelationId}]",
                message.JobId, message.JobType, message.CorrelationId);
        }
    }

    public void Dispose()
    {
        _channel?.Dispose();
    }
}
