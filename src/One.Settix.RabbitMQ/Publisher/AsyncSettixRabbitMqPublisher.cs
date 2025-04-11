using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using One.Settix.RabbitMQ.Bootstrap;
using One.Settix.RabbitMQ.SettixConfigurationMessageProcessors;
using RabbitMQ.Client;
using System.Text.Json;

namespace One.Settix.RabbitMQ.Publisher;

public sealed class AsyncSettixRabbitMqPublisher
{
    private readonly RabbitMqClusterOptions options;
    private readonly AsyncPublisherChannelResolver _channelResolver;
    private readonly ILogger<AsyncSettixRabbitMqPublisher> _logger;

    public AsyncSettixRabbitMqPublisher(IOptionsMonitor<RabbitMqClusterOptions> optionsMonitor, AsyncPublisherChannelResolver channelResolver, ILogger<AsyncSettixRabbitMqPublisher> logger)
    {
        options = optionsMonitor.CurrentValue; // TODO: Implement onChange event
        _channelResolver = channelResolver;
        _logger = logger;
    }

    public async Task PublishAsync(ConfigurationRequest message)
    {
        string exchangeName = SettixRabbitMqNamer.GetExchangeName();

        try
        {
            byte[] body = JsonSerializer.SerializeToUtf8Bytes(message);

            foreach (var option in options.Clusters)
            {
                string routingKey = SettixRabbitMqNamer.GetRoutingKey(message.ServiceKey);

                IChannel exchangeChannel = await _channelResolver.ResolveAsync(exchangeName, option, message.ServiceKey).ConfigureAwait(false);
                BasicProperties props = BuildProps(ConfigurationRequest.ContractId);

                await exchangeChannel.BasicPublishAsync(exchangeName, routingKey, false, props, body).ConfigureAwait(false);

                _logger.LogInformation("Published message: {message}", message);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to publish message: {message} to {exchange}", message, exchangeName);
        }
    }

    /// <summary>
    /// Publishes the response of the configured service
    /// </summary>
    /// <param name="message">The message that contains the response of the configured service</param>
    /// <param name="serviceKey">The key that will be used to construct the routing key <see cref="SettixRabbitMqNamer.GetRoutingKey"/> where the message will be published to</param>
    public async Task PublishAsync(ConfigurationResponse message, string serviceKey)
    {
        string exchangeName = SettixRabbitMqNamer.GetExchangeName();

        try
        {
            byte[] body = JsonSerializer.SerializeToUtf8Bytes(message);

            foreach (var option in options.Clusters)
            {
                string routingKey = SettixRabbitMqNamer.GetRoutingKey(serviceKey);

                IChannel exchangeChannel = await _channelResolver.ResolveAsync(exchangeName, option, serviceKey).ConfigureAwait(false);
                BasicProperties props = BuildProps(ConfigurationResponse.ContractId);

                await exchangeChannel.BasicPublishAsync(exchangeName, routingKey, false, props, body).ConfigureAwait(false);

                _logger.LogInformation("Published response message: {@message}", message);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to publish response message: {@message} to {exchange}", message, exchangeName);
        }
    }

    /// <summary>
    /// Publishes the message that will remove the configuration from the configured service.
    /// </summary>
    /// <param name="message"></param>
    public async Task PublishAsync(RemoveConfigurationRequest message)
    {
        string exchangeName = SettixRabbitMqNamer.GetExchangeName();

        try
        {
            byte[] body = JsonSerializer.SerializeToUtf8Bytes(message);

            foreach (var option in options.Clusters)
            {
                string routingKey = SettixRabbitMqNamer.GetRoutingKey(message.ServiceKey);

                IChannel exchangeChannel = await _channelResolver.ResolveAsync(exchangeName, option, message.ServiceKey).ConfigureAwait(false);
                BasicProperties props = BuildProps(RemoveConfigurationRequest.ContractId);

                await exchangeChannel.BasicPublishAsync(exchangeName, routingKey, false, props, body).ConfigureAwait(false);

                _logger.LogInformation("Published message: {message}", message);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to publish message: {message} to {exchange}", message, exchangeName);
        }
    }

    /// <summary>
    /// Publishes the response of removing the configuration from the configured service
    /// </summary>
    /// <param name="message">The message that contains the response of the configured service</param>
    /// <param name="serviceKey">The key that will be used to construct the routing key <see cref="SettixRabbitMqNamer.GetRoutingKey"/> where the message will be published to</param>
    public async Task PublishAsync(RemoveConfigurationResponse message, string serviceKey)
    {
        string exchangeName = SettixRabbitMqNamer.GetExchangeName();

        try
        {
            byte[] body = JsonSerializer.SerializeToUtf8Bytes(message);

            foreach (var option in options.Clusters)
            {
                string routingKey = SettixRabbitMqNamer.GetRoutingKey(serviceKey);

                IChannel exchangeChannel = await _channelResolver.ResolveAsync(exchangeName, option, serviceKey).ConfigureAwait(false);
                BasicProperties props = BuildProps(RemoveConfigurationResponse.ContractId);

                await exchangeChannel.BasicPublishAsync(exchangeName, routingKey, false, props, body).ConfigureAwait(false);

                _logger.LogInformation("Published response message: {@message}", message);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to publish response message: {@message} to {exchange}", message, exchangeName);
        }
    }

    private static BasicProperties BuildProps(string contractId)
    {
        BasicProperties props = new BasicProperties();
        props.Persistent = true;
        props.Headers = new Dictionary<string, object>
        {
            { "settix-message-type", contractId }
        };

        return props;
    }
}
