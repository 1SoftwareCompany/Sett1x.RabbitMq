using One.Settix.RabbitMQ.Bootstrap;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace One.Settix.RabbitMQ.Publisher;

public class AsyncPublisherChannelResolver : AsyncChannelResolverBase // channels per exchange
{
    private static SemaphoreSlim publisherLock = new SemaphoreSlim(1, 1); // It's crucial to set values for initial and max count of allowed threads, otherwise it is possible to allow more than expected threads to enter the lock.

    public AsyncPublisherChannelResolver(AsyncConnectionResolver connectionResolver) : base(connectionResolver) { }

    public override async ValueTask<IChannel> ResolveAsync(string exchange, RabbitMqOptions options, string serviceKey)
    {
        string channelKey = $"{serviceKey}_{exchange}_{options.Server}".ToLower();
        string connectionKey = $"{options.VHost}_{options.Server}".ToLower();

        IChannel channel = GetExistingChannel(channelKey);

        if (channel is null || channel.IsClosed)
        {
            bool lockTaken = false;
            try
            {
                lockTaken = await publisherLock.WaitAsync(10_000).ConfigureAwait(false);
                if (lockTaken == false)
                {
                    throw new TimeoutException("Unable to acquire lock for publisher channel resolver.");
                }

                channel = GetExistingChannel(channelKey);

                if (channel?.IsClosed == true)
                {
                    channels.Remove(channelKey);
                    channel = null;
                }

                if (channel is null)
                {
                    IConnection connection = await connectionResolver.ResolveAsync(connectionKey, options).ConfigureAwait(false);
                    IChannel scopedChannel = await CreateChannelForPublisherAsync(connection).ConfigureAwait(false);
                    try
                    {
                        if (string.IsNullOrEmpty(exchange) == false)
                        {
                            await scopedChannel.ExchangeDeclarePassiveAsync(exchange).ConfigureAwait(false);
                        }
                    }
                    catch (OperationInterruptedException)
                    {
                        scopedChannel.Dispose();
                        scopedChannel = await CreateChannelForPublisherAsync(connection).ConfigureAwait(false);
                        await scopedChannel.ExchangeDeclareAsync(exchange, ExchangeType.Direct, true).ConfigureAwait(false);
                    }

                    channels.Add(channelKey, scopedChannel);
                }
            }
            finally
            {
                if (lockTaken) // only release if we acquired the lock, otherwise it will throw an exception if we exceed the max count of allowed threads
                    publisherLock?.Release();
            }
        }

        return GetExistingChannel(channelKey);
    }

    private async Task<IChannel> CreateChannelForPublisherAsync(IConnection connection)
    {
        CreateChannelOptions channelOpts = new CreateChannelOptions(publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: false);
        IChannel channel = await connection.CreateChannelAsync(channelOpts).ConfigureAwait(false);

        return channel;
    }
}
