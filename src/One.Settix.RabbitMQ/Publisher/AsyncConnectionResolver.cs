using One.Settix.RabbitMQ.Bootstrap;
using RabbitMQ.Client;
using System.Collections.Concurrent;

namespace One.Settix.RabbitMQ.Publisher;

public class AsyncConnectionResolver : IAsyncDisposable
{
    private readonly ConcurrentDictionary<string, IConnection> connectionsPerVHost;
    private readonly IAsyncRabbitMqConnectionFactory connectionFactory;

    private static SemaphoreSlim connectionResolverLock = new SemaphoreSlim(1, 1); // It's crucial to set values for initial and max count of allowed threads, otherwise it is possible to allow more than expected threads to enter the lock.

    public AsyncConnectionResolver(IAsyncRabbitMqConnectionFactory connectionFactory)
    {
        connectionsPerVHost = new ConcurrentDictionary<string, IConnection>();
        this.connectionFactory = connectionFactory;
    }

    public async Task<IConnection> ResolveAsync(string key, RabbitMqOptions options)
    {
        IConnection connection = GetExistingConnection(key);

        if (connection is null || connection.IsOpen == false)
        {
            bool lockTaken = false;
            try
            {
                lockTaken = await connectionResolverLock.WaitAsync(10_000).ConfigureAwait(false);
                if (lockTaken == false)
                {
                    throw new TimeoutException("Unable to acquire lock for connection resolver.");
                }

                connection = GetExistingConnection(key);
                if (connection is null || connection.IsOpen == false)
                {
                    connection = await CreateConnectionAsync(key, options).ConfigureAwait(false);
                }
            }
            finally
            {
                if (lockTaken) // only release if we acquired the lock, otherwise it will throw an exception if we exceed the max count of allowed threads
                    connectionResolverLock?.Release();
            }
        }

        return connection;
    }

    private IConnection GetExistingConnection(string key)
    {
        connectionsPerVHost.TryGetValue(key, out IConnection connection);

        return connection;
    }

    private async Task<IConnection> CreateConnectionAsync(string key, RabbitMqOptions options)
    {
        IConnection connection = await connectionFactory.CreateConnectionWithOptionsAsync(options).ConfigureAwait(false);

        if (connectionsPerVHost.TryGetValue(key, out _))
        {
            if (connectionsPerVHost.TryRemove(key, out _))
                connectionsPerVHost.TryAdd(key, connection);
        }
        else
        {
            connectionsPerVHost.TryAdd(key, connection);
        }

        return connection;
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var connection in connectionsPerVHost)
        {
            await connection.Value.CloseAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
        }
    }
}
