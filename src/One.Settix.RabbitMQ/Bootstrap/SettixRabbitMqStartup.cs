﻿using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using One.Settix.RabbitMQ.Bootstrap.Management;
using One.Settix.RabbitMQ.Bootstrap.Management.Model;
using RabbitMQ.Client;

namespace One.Settix.RabbitMQ.Bootstrap;

public sealed class SettixRabbitMqStartup
{
    private readonly RabbitMqClusterOptions _options;
    private readonly IAsyncRabbitMqConnectionFactory _connectionFactory;
    private readonly ILogger<SettixRabbitMqStartup> _logger;

    public SettixRabbitMqStartup(IOptionsMonitor<RabbitMqClusterOptions> optionsMonitor, IAsyncRabbitMqConnectionFactory connectionFactory, ILogger<SettixRabbitMqStartup> logger)
    {
        _options = optionsMonitor.CurrentValue;
        _connectionFactory = connectionFactory;
        _logger = logger;
    }

    public async Task StartAsync(string queuePrefix)
    {
        try
        {
            foreach (RabbitMqOptions clusterOption in _options.Clusters)
            {
                RabbitMqManagementClient rmqClient = new RabbitMqManagementClient(clusterOption);
                CreateVHost(rmqClient, clusterOption);

                using var connection = await _connectionFactory.CreateConnectionWithOptionsAsync(clusterOption).ConfigureAwait(false);
                using var channel = await connection.CreateChannelAsync().ConfigureAwait(false);
                await RecoverChannelAsync(channel, queuePrefix).ConfigureAwait(false);
            }

        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to start Sett1x.RabbitMQ infrastructure.");
        }
    }

    private async Task RecoverChannelAsync(IChannel channel, string queuePrefix)
    {
        string exchangeName = SettixRabbitMqNamer.GetExchangeName();
        string queueName = SettixRabbitMqNamer.GetQueueName(queuePrefix);
        string routingKey = SettixRabbitMqNamer.GetRoutingKey(queuePrefix);

        await channel.ExchangeDeclareAsync(exchangeName, ExchangeType.Direct, true).ConfigureAwait(false);
        await channel.QueueDeclareAsync(queueName, true, false, false, null).ConfigureAwait(false);
        await channel.QueueBindAsync(queueName, exchangeName, routingKey).ConfigureAwait(false);
    }

    private void CreateVHost(RabbitMqManagementClient client, RabbitMqOptions options)
    {
        if (client.GetVHosts().Any(vh => vh.Name == options.VHost) == false)
        {
            var vhost = client.CreateVirtualHost(options.VHost);
            var rabbitMqUser = client.GetUsers().SingleOrDefault(x => x.Name == options.Username);
            var permissionInfo = new PermissionInfo(rabbitMqUser, vhost);
            client.CreatePermission(permissionInfo);
        }
    }
}
