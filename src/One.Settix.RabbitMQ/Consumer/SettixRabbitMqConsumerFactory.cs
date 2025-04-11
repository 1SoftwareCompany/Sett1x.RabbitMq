using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using One.Settix.RabbitMQ.Bootstrap;
using One.Settix.RabbitMQ.SettixConfigurationMessageProcessors;
using RabbitMQ.Client;

namespace One.Settix.RabbitMQ.Consumer;

public sealed class SettixRabbitMqConsumerFactory
{
    private AsyncSettixRabbitMqConsumer _consumer;

    private readonly ISettixConfigurationMessageProcessor _settixConfigurationMessageProcessor;
    private readonly RabbitMqOptions options;
    private readonly AsyncConsumerPerQueueChannelResolver _channelResolver;
    private readonly ILogger<SettixRabbitMqConsumerFactory> _logger;

    public SettixRabbitMqConsumerFactory(ISettixConfigurationMessageProcessor settixConfigurationMessageProcessor, IOptionsMonitor<RabbitMqOptions> optionsMonitor, AsyncConsumerPerQueueChannelResolver channelResolver, ILogger<SettixRabbitMqConsumerFactory> logger)
    {
        _settixConfigurationMessageProcessor = settixConfigurationMessageProcessor;
        options = optionsMonitor.CurrentValue; //TODO: Implement onChange event
        _channelResolver = channelResolver;
        _logger = logger;
    }

    public async Task CreateAndStartConsumerAsync(string serviceKey, CancellationToken cancellationToken)
    {
        try
        {
            string consumerChannelKey = SettixRabbitMqNamer.GetConsumerChannelName(serviceKey);
            IChannel channel = await _channelResolver.ResolveAsync(consumerChannelKey, options, options.VHost).ConfigureAwait(false);
            string queueName = SettixRabbitMqNamer.GetQueueName(serviceKey);

            _consumer = new AsyncSettixRabbitMqConsumer(_settixConfigurationMessageProcessor, channel, _logger);
            await _consumer.ConfigureConsumerAsync(queueName).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to start Sett1x.RabbitMQ consumer.");
        }
    }

    public async Task StopConsumerAsync()
    {
        if (_consumer is null)
            return;

        await _consumer.StopAsync().ConfigureAwait(false);
    }
}
