using GG.Publisher;
using One.Settix.RabbitMQ;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddHostedService<Worker>();

builder.Services.AddLogging();
builder.Services.AddSettixRabbitMqPublisher();

var host = builder.Build();
host.Run();
