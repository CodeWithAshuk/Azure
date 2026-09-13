using System;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;

namespace Consumer
{
    public class ConsumeQueueMessage
    {
       static string connectionString = "Endpoint=sb://servicebusashu.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=7jtRf6kajBJIBmy3faWu6u0KLCk7BzCq/+ASbBoSKw4="; // Add your Service Bus connection string here
       static string queueName = "testashuqueue"; // Add your queue name here
        private static ServiceBusClient? _client;
        private static ServiceBusProcessor? _processor;

        /// <summary>
        /// Starts processing messages from the specified Service Bus queue using a handler delegate.
        /// The handler receives the message body as a string. Messages are completed after the handler completes successfully.
        /// </summary>
        public static  async Task StartProcessingAsync( Func<string, Task> messageHandler, Func<ProcessErrorEventArgs, Task>? errorHandler = null)
        {
            if (string.IsNullOrEmpty(connectionString)) throw new ArgumentException("connectionString is required", nameof(connectionString));
            if (string.IsNullOrEmpty(queueName)) throw new ArgumentException("queueName is required", nameof(queueName));
            if (messageHandler is null) throw new ArgumentNullException(nameof(messageHandler));

            // Create client and processor
            _client = new ServiceBusClient(connectionString);
            var options = new ServiceBusProcessorOptions
            {
                AutoCompleteMessages = false,
                MaxConcurrentCalls = 1
            };

            _processor = _client.CreateProcessor(queueName, options);

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
                    // If the handler throws, abandon the message so it can be retried or dead-lettered.
                    try
                    {
                        await args.AbandonMessageAsync(args.Message).ConfigureAwait(false);
                    }
                    catch
                    {
                        // ignore
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
                    // Basic fallback logging
                    Console.Error.WriteLine($"ServiceBus error: {args.Exception}");
                }
            };

            await _processor.StartProcessingAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// Stops processing and disposes the Service Bus client and processor.
        /// </summary>
        public static async Task StopProcessingAsync()
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
}
