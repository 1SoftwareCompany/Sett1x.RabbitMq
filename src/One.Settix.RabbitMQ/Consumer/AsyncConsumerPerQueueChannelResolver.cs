using One.Settix.RabbitMQ.Bootstrap;
using One.Settix.RabbitMQ.Publisher;

namespace One.Settix.RabbitMQ.Consumer;

public class AsyncConsumerPerQueueChannelResolver : AsyncChannelResolverBase // channels per queue
{
    public AsyncConsumerPerQueueChannelResolver(AsyncConnectionResolver connectionResolver) : base(connectionResolver) { }
}
