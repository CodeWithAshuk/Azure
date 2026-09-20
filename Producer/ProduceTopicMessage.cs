using System;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;

namespace Producer;

public class ProduceTopicMessage
{
    // Configure these for your Service Bus namespace and topic
    private static string connectionString = "your connection string";
    private static string topicName = "your topic name";

    /// <summary>
    /// Sends a single message to the configured Service Bus topic.
    /// </summary>
    public static async Task SendMessageAsync(string messageBody)
    {
        if (string.IsNullOrEmpty(connectionString)) throw new ArgumentException("connectionString is required", nameof(connectionString));
        if (string.IsNullOrEmpty(topicName)) throw new ArgumentException("topicName is required", nameof(topicName));
        if (messageBody is null) throw new ArgumentNullException(nameof(messageBody));

        await using var client = new ServiceBusClient(connectionString);
        ServiceBusSender sender = client.CreateSender(topicName);

        var message = new ServiceBusMessage(messageBody);

        await sender.SendMessageAsync(message).ConfigureAwait(false);
    }
}
