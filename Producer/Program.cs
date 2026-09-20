using Microsoft.Azure.Amqp.Framing;
using System;
using System.Threading.Tasks;

namespace Producer
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            string message;

            if (args.Length > 1)
            {
                message = args[1];
            }
            else
            {
                Console.Write("Enter message to send: ");
                message = Console.ReadLine() ?? string.Empty;

                if (string.IsNullOrWhiteSpace(message))
                {
                    Console.WriteLine("No message entered. Exiting.");
                    return;
                }
            }
            await CallTopic(message);
            await CallQueue(message);
            Console.WriteLine("Message sent.");
            Console.ReadLine();
        }
        private static async Task CallQueue(string message)
        {
            await ProduceQueueMessage.SendMessageAsync(message);
        }
        private static async Task CallTopic(string message)
        {
            await ProduceTopicMessage.SendMessageAsync(message);
        }
    }


}
