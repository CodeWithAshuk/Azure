using System;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;

namespace Consumer;

public class SubscribeTopic
{
    private static ServiceBusClient? _client;
    private static ServiceBusProcessor? _processor;
    // Configure these for your Service Bus namespace and topic
    private static string connectionString = "your connection string";
    private static string topicName = "your topic name";
    private static string subscriptionName = "your subscriber name";
    /// <summary>
    /// Starts processing messages from a Service Bus topic subscription.
    /// </summary>
    /// <param name="connectionString">Service Bus namespace connection string</param>
    /// <param name="topicName">Topic name</param>
    /// <param name="subscriptionName">Subscription name</param>
    /// <param name="messageHandler">Handler invoked with the message body</param>
    /// <param name="errorHandler">Optional error handler for processing errors</param>
    public static async Task StartSubscriptionProcessingAsync(
       
        Func<string, Task> messageHandler,
        Func<ProcessErrorEventArgs, Task>? errorHandler = null)
    {
        if (string.IsNullOrEmpty(connectionString)) throw new ArgumentException("connectionString is required", nameof(connectionString));
        if (string.IsNullOrEmpty(topicName)) throw new ArgumentException("topicName is required", nameof(topicName));
        if (string.IsNullOrEmpty(subscriptionName)) throw new ArgumentException("subscriptionName is required", nameof(subscriptionName));
        if (messageHandler is null) throw new ArgumentNullException(nameof(messageHandler));

        _client = new ServiceBusClient(connectionString);
        var options = new ServiceBusProcessorOptions
        {
            AutoCompleteMessages = false,
            MaxConcurrentCalls = 1
        };

        _processor = _client.CreateProcessor(topicName, subscriptionName, options);

        _processor.ProcessMessageAsync += async args =>
        {
            try
            {
                string body = args.Message.Body.ToString();
                await messageHandler(body).ConfigureAwait(false);
                await args.CompleteMessageAsync(args.Message).ConfigureAwait(false);
            }
            catch (Exception)
            {
                try
                {
                    await args.AbandonMessageAsync(args.Message).ConfigureAwait(false);
                }
                catch
                {
                    // swallow
                }
                throw;
            }
        };

        _processor.ProcessErrorAsync += async args =>
        {
            if (errorHandler is not null)
            {
                await errorHandler(args).ConfigureAwait(false);
            }
            else
            {
                Console.Error.WriteLine($"ServiceBus error: {args.Exception}");
            }
        };

        await _processor.StartProcessingAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Stops processing and disposes resources.
    /// </summary>
    public static async Task StopSubscriptionProcessingAsync()
    {
        if (_processor is not null)
        {
            try
            {
                await _processor.StopProcessingAsync().ConfigureAwait(false);
            }
            catch { }

            await _processor.DisposeAsync().ConfigureAwait(false);
            _processor = null;
        }

        if (_client is not null)
        {
            await _client.DisposeAsync().ConfigureAwait(false);
            _client = null;
        }
    }
}
