using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace One.Settix.RabbitMQ.Bootstrap;

public sealed class AsyncRabbitMqConnectionFactory : IAsyncRabbitMqConnectionFactory
{
    private readonly ILogger<AsyncRabbitMqConnectionFactory> logger;

    public AsyncRabbitMqConnectionFactory(ILogger<AsyncRabbitMqConnectionFactory> logger)
    {
        this.logger = logger;
    }

    public async Task<IConnection> CreateConnectionWithOptionsAsync(RabbitMqOptions options)
    {
        logger.LogDebug("Loaded Sett1x.RabbitMQ options are {@Options}", options);

        bool tailRecursion = false;

        do
        {
            try
            {
                var connectionFactory = new ConnectionFactory();
                connectionFactory.Port = options.Port;
                connectionFactory.UserName = options.Username;
                connectionFactory.Password = options.Password;
                connectionFactory.VirtualHost = options.VHost;
                connectionFactory.AutomaticRecoveryEnabled = true;
                connectionFactory.Ssl.Enabled = options.UseSsl;
                connectionFactory.EndpointResolverFactory = (_) => MultipleEndpointResolver.ComposeEndpointResolver(options);

                return await connectionFactory.CreateConnectionAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                if (ex is BrokerUnreachableException)
                    logger.LogDebug("Failed to create Sett1x.RabbitMQ connection using options {@options}. Retrying...", options);
                else
                    logger.LogWarning(ex, "Failed to create Sett1x.RabbitMQ connection using options {@options}. Retrying...", options);

                Task.Delay(5000).GetAwaiter().GetResult();
                tailRecursion = true;
            }
        } while (tailRecursion == true);

        return default;
    }

    private class MultipleEndpointResolver : DefaultEndpointResolver
    {
        MultipleEndpointResolver(AmqpTcpEndpoint[] amqpTcpEndpoints) : base(amqpTcpEndpoints) { }

        public static MultipleEndpointResolver ComposeEndpointResolver(RabbitMqOptions options)
        {
            AmqpTcpEndpoint[] endpoints = AmqpTcpEndpoint.ParseMultiple(options.Server);

            if (options.UseSsl is false)
                return new MultipleEndpointResolver(endpoints);

            foreach (AmqpTcpEndpoint endp in endpoints)
            {
                endp.Ssl.Enabled = true;
                endp.Ssl.ServerName = options.Server;
            }

            return new MultipleEndpointResolver(endpoints);
        }
    }
}
