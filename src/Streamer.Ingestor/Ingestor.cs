using System;
using System.Collections.Generic;
using System.Fabric;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Data.Collections;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;

using Streamer.Ingestor.Extensions;

namespace Streamer.Ingestor
{
    /// <summary>
    /// An instance of this class is created for each service replica by the Service Fabric runtime.
    /// </summary>
    internal sealed class Ingestor : StatefulService
    {
        private const string OffsetDictionaryName = "ingestor.OffsetDictionary";
        private const string EpochDictionaryName = "ingestor.EpochDictionary";

        public Ingestor(StatefulServiceContext context)
            : base(context)
        { }

        /// <summary>
        /// Optional override to create listeners (e.g., HTTP, Service Remoting, WCF, etc.) for this service replica to handle client or user requests.
        /// </summary>
        /// <remarks>
        /// For more information on service communication, see https://aka.ms/servicefabricservicecommunication
        /// </remarks>
        /// <returns>A collection of listeners.</returns>
        protected override IEnumerable<ServiceReplicaListener> CreateServiceReplicaListeners()
        {
            return new ServiceReplicaListener[0];
        }

        private async Task<Tuple<EventPosition, long>> GetOffsetAndEpochAsync(IReliableDictionary<string, string> offsetDictionary, IReliableDictionary<string, long> epochDictionary)
        {
            using (ITransaction tx = this.StateManager.CreateTransaction())
            {
                ConditionalValue<string> offsetResult = await offsetDictionary.TryGetValueAsync(tx, "offset", LockMode.Default);
                ConditionalValue<long> epochResult = await epochDictionary.TryGetValueAsync(tx, "epoch", LockMode.Update);

                long newEpoch = epochResult.HasValue
                   ? epochResult.Value + 1
                   : 0;

                // epoch is recorded each time the service fails over or restarts.
                await epochDictionary.SetAsync(tx, "epoch", newEpoch);
                await tx.CommitAsync();

                EventPosition eventPosition;
                if(offsetResult.HasValue)
                {
                    eventPosition = EventPosition.FromOffset(offsetResult.Value);
                } else
                {
                    // TODO: make this configurable behaviour 
                    eventPosition = EventPosition.FromStart();
                }

                return new Tuple<EventPosition, long>(eventPosition, newEpoch);
            }
        }

        /// <summary>
        /// This is the main entry point for your service replica.
        /// This method executes when this replica of your service becomes primary and has write status.
        /// </summary>
        /// <param name="cancellationToken">Canceled when Service Fabric needs to shut down this service replica.</param>
        protected override async Task RunAsync(CancellationToken cancellationToken)
        {
            // load the connection string from event hub
            var ehConfiguration = this.Context.CodePackageActivationContext
                .GetConfigurationPackageObject("Config")
                .Settings
                .Sections["EventHubConfiguration"];

            var eventHubConnectionString = ehConfiguration.Get("ConnectionString");
            var eventHubConsumerGroup = ehConfiguration.Get("ConsumerGroup");
            var eventHubMaxBatchSize = ehConfiguration.TryGetAsInt("MaxBatchSize", 100);

            // we'll be storing our offset here, in case our service is killed
            IReliableDictionary<string, string> offsetDictionary =
                await this.StateManager.GetOrAddAsync<IReliableDictionary<string, string>>(OffsetDictionaryName);

            IReliableDictionary<string, long> epochDictionary =
                await this.StateManager.GetOrAddAsync<IReliableDictionary<string, long>>(EpochDictionaryName);

            // Each partition of this service corresponds to a partition in EventHub.
            // Partitions are automatically assigned based on the partitioning scheme 
            // which is defined in the Orchestrator service, where this service is supposed
            // to be created. 
            Int64RangePartitionInformation partitionInfo = (Int64RangePartitionInformation)this.Partition.PartitionInfo;
            long servicePartitionKey = partitionInfo.LowKey;

            // start connecting to the event hub
            var client = EventHubClient.CreateFromConnectionString(eventHubConnectionString);

            // verify the partition exists
            var eventHubInfo = await client.GetRuntimeInformationAsync();
            if (eventHubInfo.PartitionCount < servicePartitionKey || servicePartitionKey < 0)
            {
                ServiceEventSource.Current.ServiceMessage(this.Context,
                    "The instance is running on partition {0} but there are {1} partitions on the EventHub.",
                    servicePartitionKey, eventHubInfo.PartitionCount);

                throw new ArgumentOutOfRangeException($"Partition {servicePartitionKey} does not exist on event hub with {eventHubInfo.PartitionCount} partitions.");
            }


            if (string.IsNullOrEmpty(eventHubConsumerGroup))
            {
                // TODO: replace with a better way
                eventHubConsumerGroup = "$Default";
            }

            ServiceEventSource.Current.ServiceMessage(this.Context,
                "Starting reading from Event Hub {0}, running on partition {1}",
                client.EventHubName,
                servicePartitionKey);

            Tuple<EventPosition, long> offsetAndEpoch = await GetOffsetAndEpochAsync(offsetDictionary, epochDictionary);

            PartitionReceiver partitionReceiever = client.CreateEpochReceiver(
                eventHubConsumerGroup,
                eventHubInfo.PartitionIds[servicePartitionKey],
                offsetAndEpoch.Item1,
                offsetAndEpoch.Item2);


            // TODO: make this configurable
            var waitTime = TimeSpan.FromSeconds(5);

            // TODO: instantiate the http client
            long count = 0;
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                IEnumerable<EventData> eventData = await partitionReceiever.ReceiveAsync(eventHubMaxBatchSize, waitTime);

                if(eventData?.Count() > 0)
                {
                    count += eventData.Count();

                    ServiceEventSource.Current.ServiceMessage(this.Context, 
                        "Data read from event hub partition {2}. I've read {0} messages, last offset was {1}. Total count: {3}", 
                        eventData.Count(), 
                        eventData.Last().SystemProperties.Offset,
                        servicePartitionKey,
                        count);
                }

                // TODO: this is where we parse the events
                // send it to the "other" service
                // and "checkpoint" the offset
            }
        }
    }
}
