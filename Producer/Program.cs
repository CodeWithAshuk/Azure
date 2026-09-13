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

            await ProduceQueueMessage.SendMessageAsync(message);
            Console.WriteLine("Message sent.");
        }
    }
}
