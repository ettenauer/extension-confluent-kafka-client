using Local.Runner.Examples;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;

namespace Local.Runner
{
    class Program
    {
        static void Main(string[] args)
        {
            var serviceCollection = new ServiceCollection();
            serviceCollection.AddLogging(configure => configure.AddConsole());

            var serviceProvider = serviceCollection.BuildServiceProvider();
            var logger = serviceProvider.GetService<ILoggerFactory>()
                                        .CreateLogger<Program>();

            logger.LogInformation("configure consumer & producer");

            using (var consumer = new SampleConsumer(serviceProvider
                .GetService<ILoggerFactory>()
                .CreateLogger<SampleConsumer>()))
            {
                logger.LogInformation("Start consumer");

                consumer.Subscribe();

                using(var producer = new SampleProducer(10000, serviceProvider
                .GetService<ILoggerFactory>()
                .CreateLogger<SampleProducer>()))
                {
                    logger.LogInformation("Start producer");

                    producer.Produce();

                    logger.LogInformation("Producer is finished");
                }

                Console.ReadLine();
            }
   
            logger.LogInformation("Consumer & Producer terminated");
        }

    }
}
