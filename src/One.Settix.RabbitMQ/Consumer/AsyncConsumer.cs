﻿using Microsoft.Extensions.Logging;
using One.Settix.RabbitMQ.SettixConfigurationMessageProcessors;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;
using System.Text.Json;

namespace One.Settix.RabbitMQ.Consumer;

public sealed class AsyncConsumer : AsyncEventingBasicConsumer
{
    private bool isСurrentlyConsuming;

    private readonly ISettixConfigurationMessageProcessor _settixConfigurationMessageProcessor;
    private readonly IModel _model;
    private readonly ILogger _logger;

    private const string MessageType = "settix-message-type";

    public AsyncConsumer(string queuName, ISettixConfigurationMessageProcessor settixConfigurationMessageProcessor, IModel model, ILogger logger) : base(model)
    {
        model.BasicQos(0, 1, false); // prefetch allow to avoid buffer of messages on the flight
        model.BasicConsume(queuName, false, string.Empty, this); // we should use autoAck: false to avoid messages loosing

        _settixConfigurationMessageProcessor = settixConfigurationMessageProcessor;
        _model = model;
        _logger = logger;
        isСurrentlyConsuming = false;
        Received += AsyncListener_Received;
    }

    public async Task StopAsync()
    {
        // 1. We detach the listener so ther will be no new messages coming from the queue
        Received -= AsyncListener_Received;

        // 2. Wait to handle any messages in progress
        while (isСurrentlyConsuming)
        {
            // We are trying to wait all consumers to finish their current work.
            // Ofcourse the host could be forcibly shut down but we are doing our best.

            await Task.Delay(10).ConfigureAwait(false);
        }

        if (_model.IsOpen)
            _model.Abort();
    }

    private async Task AsyncListener_Received(object sender, BasicDeliverEventArgs @event)
    {
        try
        {
            _logger.LogDebug("Message received. Sender {sender}.", sender.GetType().Name);
            isСurrentlyConsuming = true;

            if (sender is AsyncEventingBasicConsumer consumer)
                await ProcessAsync(@event, consumer).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to deliver message");
            throw;
        }
        finally
        {
            isСurrentlyConsuming = false;
        }
    }

    private async Task ProcessAsync(BasicDeliverEventArgs ev, AsyncEventingBasicConsumer consumer)
    {
        if (ev.BasicProperties.IsHeadersPresent() && ev.BasicProperties.Headers.TryGetValue(MessageType, out object messageType))
        {
            string contract = GetMessageContract(messageType);

            try
            {
                switch (contract)
                {
                    case ConfigurationRequest.ContractId: // TODO: use the contract id
                        await ProcessConfigurationRequestAsync(ev, consumer).ConfigureAwait(false);
                        break;
                    case ConfigurationResponse.ContractId:
                        await ProcessConfigurationResponseAsync(ev, consumer).ConfigureAwait(false);
                        break;
                    case RemoveConfigurationRequest.ContractId:
                        await ProcessRemoveConfigurationRequestAsync(ev, consumer).ConfigureAwait(false);
                        break;
                    case RemoveConfigurationResponse.ContractId:
                        await ProcessRemoveConfigurationResponseAsync(ev, consumer).ConfigureAwait(false);
                        break;
                    default:
                        _logger.LogError("Mising MessageType {MessageType}, can't desialize message {message}", MessageType, Convert.ToBase64String(ev.Body.ToArray()));
                        break;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Failed to process message. Failed to deserialize : {Convert.ToBase64String(ev.Body.ToArray())}");
            }
        }
        else
        {
            _logger.LogError("Missing MessageType {MessageType}, can't deserialize message {message}", MessageType, Convert.ToBase64String(ev.Body.ToArray()));
        }

        Ack(ev, consumer);

        static void Ack(BasicDeliverEventArgs ev, AsyncEventingBasicConsumer consumer)
        {
            if (consumer.Model.IsOpen)
            {
                consumer.Model.BasicAck(ev.DeliveryTag, false);
            }
        }
    }

    private static string GetMessageContract(object messageHeader)
    {
        byte[] headerBytes = messageHeader as byte[];
        return Encoding.UTF8.GetString(headerBytes);
    }

    private async Task ProcessConfigurationRequestAsync(BasicDeliverEventArgs ev, AsyncEventingBasicConsumer consumer)
    {
        ConfigurationRequest request = JsonSerializer.Deserialize<ConfigurationRequest>(ev.Body.ToArray());
        await _settixConfigurationMessageProcessor.ProcessAsync(request).ConfigureAwait(false);
    }

    private async Task ProcessConfigurationResponseAsync(BasicDeliverEventArgs ev, AsyncEventingBasicConsumer consumer)
    {
        ConfigurationResponse response = JsonSerializer.Deserialize<ConfigurationResponse>(ev.Body.ToArray());
        await _settixConfigurationMessageProcessor.ProcessAsync(response).ConfigureAwait(false);
    }

    private async Task ProcessRemoveConfigurationRequestAsync(BasicDeliverEventArgs ev, AsyncEventingBasicConsumer consumer)
    {
        RemoveConfigurationRequest request = JsonSerializer.Deserialize<RemoveConfigurationRequest>(ev.Body.ToArray());
        await _settixConfigurationMessageProcessor.ProcessAsync(request).ConfigureAwait(false);
    }

    private async Task ProcessRemoveConfigurationResponseAsync(BasicDeliverEventArgs ev, AsyncEventingBasicConsumer consumer)
    {
        RemoveConfigurationResponse response = JsonSerializer.Deserialize<RemoveConfigurationResponse>(ev.Body.ToArray());
        await _settixConfigurationMessageProcessor.ProcessAsync(response).ConfigureAwait(false);
    }
}
