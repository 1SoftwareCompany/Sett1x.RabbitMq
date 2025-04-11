using One.Settix.RabbitMQ.Bootstrap;
using One.Settix.RabbitMQ.SettixConfigurationMessageProcessors;
using One.Settix.RabbitMQ.Publisher;

namespace GG.Publisher
{
    public class Worker : BackgroundService
    {
        private readonly AsyncSettixRabbitMqPublisher _publisher;
        private readonly SettixRabbitMqStartup _rabbitMqStartup;
        private readonly ILogger<Worker> _logger;

        public Worker(AsyncSettixRabbitMqPublisher publisher, SettixRabbitMqStartup rabbitMqStartup, ILogger<Worker> logger)
        {
            _publisher = publisher;
            _rabbitMqStartup = rabbitMqStartup;
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            await _rabbitMqStartup.StartAsync("giService").ConfigureAwait(false);

            Dictionary<string, string> keyValuePairs = new Dictionary<string, string>();
            keyValuePairs.Add("key1", "value1");
            //_publisher.Publish(new ConfigurationRequest("tenant", "giService", keyValuePairs, DateTimeOffset.UtcNow));

            await _publisher.PublishAsync(new RemoveConfigurationRequest("tenant", "giService", keyValuePairs, true, DateTimeOffset.UtcNow)).ConfigureAwait(false);

            while (!stoppingToken.IsCancellationRequested)
            {
                if (_logger.IsEnabled(LogLevel.Information))
                {
                    _logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);
                }
                await Task.Delay(1000, stoppingToken);
            }
        }
    }
}
