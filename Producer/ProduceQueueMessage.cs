using System;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;

namespace Producer
{
    public class ProduceQueueMessage
    {
        static string connectionString = "your connection string"; // Add your Service Bus connection string here
        static string queueName = "your queue name"; // Add your queue name here
        /// <summary>
        /// Sends a single message to an Azure Service Bus queue.
        /// Requires the Azure.Messaging.ServiceBus NuGet package.
        /// Example usage:
        /// await ProduceQueueMessage.SendMessageAsync(connectionString, queueName, "Hello from producer");
        /// </summary>
        public static async Task SendMessageAsync(string messageBody)
        {
            if (string.IsNullOrEmpty(connectionString)) throw new ArgumentException("connectionString is required", nameof(connectionString));
            if (string.IsNullOrEmpty(queueName)) throw new ArgumentException("queueName is required", nameof(queueName));
            if (messageBody is null) throw new ArgumentNullException(nameof(messageBody));

            // Create a ServiceBusClient which can be shared for the lifetime of the application
            await using var client = new ServiceBusClient(connectionString);

            // Create a sender for the queue
            ServiceBusSender sender = client.CreateSender(queueName);

            // Create a message that we can send. UTF-8 is used by default for string bodies.
            ServiceBusMessage message = new ServiceBusMessage(messageBody);

            // Send the message
            await sender.SendMessageAsync(message).ConfigureAwait(false);
        }
    }
}
