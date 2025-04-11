using RabbitMQ.Client;

namespace One.Settix.RabbitMQ.Bootstrap;

public interface IAsyncRabbitMqConnectionFactory
{
    Task<IConnection> CreateConnectionWithOptionsAsync(RabbitMqOptions options);
}
