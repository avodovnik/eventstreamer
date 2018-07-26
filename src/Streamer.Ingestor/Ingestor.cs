using System;
using System.Collections.Generic;
using System.Fabric;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Data.Collections;
using Microsoft.ServiceFabric.Services.Client;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Remoting.Client;
using Microsoft.ServiceFabric.Services.Remoting.V2.FabricTransport.Client;
using Microsoft.ServiceFabric.Services.Runtime;
using Newtonsoft.Json;
using Streamer.Common.Contracts;
using Streamer.Common.Extensions;
using Streamer.Common.Models;

namespace Streamer.Ingestor
{

    // TODO: https://docs.microsoft.com/en-us/azure/service-fabric/service-fabric-reliable-services-communication-remoting

    /// <summary>
    /// An instance of this class is created for each service replica by the Service Fabric runtime.
    /// </summary>
    internal sealed class Ingestor : StatefulService
    {
        private const string OffsetDictionaryName = "ingestor.OffsetDictionary";
        private const string EpochDictionaryName = "ingestor.EpochDictionary";
        private readonly IRouter _router;
        private readonly ServiceProxyFactory _proxyFactory;
        private IOrchestrator orchestratorClient;

        public Ingestor(StatefulServiceContext context, IRouter router)
            : base(context)
        {
            this._proxyFactory = new ServiceProxyFactory((c) =>
            {
                return new FabricTransportServiceRemotingClientFactory();
            });

            orchestratorClient = _proxyFactory.CreateServiceProxy<IOrchestrator>(new Uri(Common.Names.OrchestratorServiceUri), new ServicePartitionKey(1));
            this._router = router;
        }

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
                if (offsetResult.HasValue)
                {
                    eventPosition = EventPosition.FromOffset(offsetResult.Value);
                }
                else
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

            long? count = 0;
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                IEnumerable<EventData> eventData = await partitionReceiever.ReceiveAsync(eventHubMaxBatchSize, waitTime);

                if ((count = eventData?.Count() ?? 0) == 0) continue;

                ServiceEventSource.Current.ServiceMessage(this.Context,
                       "Data read from event hub partition {2}. I've read {0} messages, last offset was {1}. Total count: {3}.",
                       eventData.Count(),
                       eventData.Last().SystemProperties.Offset,
                       servicePartitionKey,
                       count
                       );

                var messages = eventData.Select(x => new MessageWrapper(x, _router));

                var groups = messages.GroupBy(x => x.Id);
                var keys = groups
                    .Select(x => new
                    {
                        // we're creating this object because we need both, the id and address
                        Id = x.Key,
                        Task = orchestratorClient.OrchestrateWorker(new WorkerDescription() { Identifier = x.Key })
                    })
                    .ToArray();

                // the operation to call the workers is async, and a bunch of them can run in parallel, so let's wait
                // until they all finish to get the keys out
                Task.WaitAll(keys.Select(x => x.Task).ToArray(), cancellationToken);

                var workers = new Dictionary<string, IProcessor>();

                // now we need to actually create the service proxies 
                foreach (var key in keys)
                {
                    var address = key.Task.Result;
                    var processor = _proxyFactory.CreateServiceProxy<IProcessor>(new Uri(address), new ServicePartitionKey(1));

                    // we need to attach to the ID, not the address!
                    workers.Add(key.Id, processor);
                }

                // now we can go through the messages
                // but we won't go through it in groups, to allow for offsetting this properly
                string lastOffset = String.Empty;
                long i = 0;
                foreach (var message in messages)
                {
                    //$"{appName}/{Names.ProcessorSuffix}/{workerDescription.Identifier}";
                    if (!await workers[message.Id].Process(message.DataPoint))
                    {
                        // what do we do? 
                        ServiceEventSource.Current.ServiceMessage(this.Context,
                                "Processing failed for message index {0}. Throwing exception.", i);
                        throw new InvalidOperationException("Processign failed. False returned from worker process.");
                    }

                    i++;

                    // TODO: not sure this works
                    if (i % 50 == 0)
                    {
                        await Checkpoint(message, offsetDictionary);
                    }
                }

                // make sure we checkpoint the last message as well
                await Checkpoint(messages.Last(), offsetDictionary);
            }
        }

        private async Task Checkpoint(MessageWrapper message, IReliableDictionary<string, string> offsetDictionary)
        {
            using (var tx = this.StateManager.CreateTransaction())
            {
                ServiceEventSource.Current.ServiceMessage(this.Context,
                   "Updating offset to {0}", message.Offset);

                // checkpoint!
                await offsetDictionary.AddOrUpdateAsync(tx, "offset", message.Offset, (x, y) => message.Offset);
                await tx.CommitAsync();
            }
        }

        private class MessageWrapper
        {
            public MessageWrapper(EventData x, IRouter router)
            {
                // TODO: this will fail horribly for poison messages
                this.DataPoint = JsonConvert.DeserializeObject<Common.Models.DataPoint>(
                        Encoding.UTF8.GetString(x.Body.Array, x.Body.Offset, x.Body.Count));

                this.Offset = x.SystemProperties.Offset;
                this.Id = router.GetRoutableIdentifier(this.DataPoint);
            }

            public DataPoint DataPoint { get; }
            public string Offset { get; }
            public string Id { get; }
        }
    }
}
