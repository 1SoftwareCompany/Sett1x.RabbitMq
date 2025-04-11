using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using One.Settix.RabbitMQ.Bootstrap;
using One.Settix.RabbitMQ.Consumer;
using One.Settix.RabbitMQ.Publisher;

namespace One.Settix.RabbitMQ;

public static class SettixRabbitMqExtensions
{
    internal static IServiceCollection AddSettixRabbitMqBase(this IServiceCollection services)
    {
        services.AddSingleton<IAsyncRabbitMqConnectionFactory, AsyncRabbitMqConnectionFactory>();
        services.AddSingleton<SettixRabbitMqStartup>();
        services.AddSingleton<AsyncConnectionResolver>();

        return services;
    }

    public static IServiceCollection AddSettixRabbitMqPublisher(this IServiceCollection services)
    {
        services.AddSettixRabbitMqBase();

        services.AddOptions<RabbitMqClusterOptions>().Configure<IConfiguration>((options, configuration) =>
        {
            configuration.GetRequiredSection("settix:rabbitmq:publisher").Bind(options.Clusters);
        });

        services.AddSingleton<AsyncPublisherChannelResolver>();
        services.AddSingleton<AsyncSettixRabbitMqPublisher>();

        return services;
    }

    public static IServiceCollection AddSettixRabbitMqConsumer(this IServiceCollection services)
    {
        services.AddSettixRabbitMqBase();

        services.AddOptions<RabbitMqOptions>().Configure<IConfiguration>((options, configuration) =>
        {
            configuration.GetRequiredSection("settix:rabbitmq:consumer").Bind(options);
        });

        services.AddSingleton<AsyncConsumerPerQueueChannelResolver>();
        services.AddSingleton<SettixRabbitMqConsumerFactory>();

        return services;
    }
}

