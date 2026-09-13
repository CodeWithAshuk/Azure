using System;
using System.Threading.Tasks;

namespace Consumer
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            string connectionString = Environment.GetEnvironmentVariable("SERVICEBUS_CONNECTION") ?? "<your-connection-string>";
            string queueName = args.Length > 0 ? args[0] : "my-queue";

            // Start processing messages. Provide a simple message handler and an error handler.
            await ConsumeQueueMessage.StartProcessingAsync(
                async body =>
                {
                    Console.WriteLine($"Received message: {body}");
                    await Task.CompletedTask;
                },
                async errorArgs =>
                {
                    Console.Error.WriteLine($"Error: {errorArgs.Exception}");
                    await Task.CompletedTask;
                });

            Console.WriteLine("Processing messages. Press ENTER to stop.");
            await Task.Run(() => Console.ReadLine());

            await ConsumeQueueMessage.StopProcessingAsync();
            Console.WriteLine("Stopped processing.");
        }
    }
}
