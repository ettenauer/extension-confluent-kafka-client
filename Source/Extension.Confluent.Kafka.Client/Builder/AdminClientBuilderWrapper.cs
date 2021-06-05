using Confluent.Kafka;
using System;
using System.Diagnostics.CodeAnalysis;

namespace Extension.Confluent.Kafka.Client.Builder
{
    /// <summary>
    /// Required in order to do unit tests for BufferedConsumer
    /// </summary>
    [ExcludeFromCodeCoverage]
    internal class AdminClientBuilderWrapper : IAdminClientBuilderWrapper
    {
        private readonly AdminClientBuilder adminClientBuilder;

        public AdminClientBuilderWrapper(AdminClientBuilder adminClientBuilder)
        {
            this.adminClientBuilder = adminClientBuilder ?? throw new ArgumentNullException(nameof(adminClientBuilder));
        }

        public IAdminClient Build()
        {
            return this.adminClientBuilder.Build();
        }
    }
}
