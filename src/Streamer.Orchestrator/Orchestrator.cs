using System;
using System.Collections.Generic;
using System.Fabric;
using System.IO;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.ServiceFabric.Services.Communication.AspNetCore;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;
using Microsoft.ServiceFabric.Services.Remoting.Runtime;
using Microsoft.ServiceFabric.Data;
using Streamer.Common.Contracts;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Data.Collections;
using Streamer.Common;
using System.Fabric.Description;
using System.Runtime.Serialization;

namespace Streamer.Orchestrator
{
    /// <summary>
    /// The FabricRuntime creates an instance of this class for each service type instance. 
    /// </summary>
    internal sealed class Orchestrator : StatefulService, IOrchestrator
    {
        [DataContract]
        private class ProcessorInformation
        {
            [DataMember]
            public string Address { get; set; }

            [DataMember]
            public long TicksLastUpdated { get; set; }
        }

        private IReliableDictionary<string, ProcessorInformation> _processorDictionary;
        private readonly FabricClient _fabricClient;

        public Orchestrator(StatefulServiceContext context)
            : base(context)
        {
            this._fabricClient = new FabricClient();
        }

        public async Task<string> OrchestrateWorker(WorkerDescription workerDescription)
        {
            if(_processorDictionary == null)
            {
                this._processorDictionary = this.StateManager
                            .GetOrAddAsync<IReliableDictionary<string, ProcessorInformation>>("orchestrator.ProcessorDictionary").Result;
            }

            ServiceEventSource.Current.ServiceMessage(this.Context, $"Orchestrate worker called for {workerDescription.Identifier}");

            var address = String.Empty;

            using (var tx = this.StateManager.CreateTransaction())
            {
                var result = await _processorDictionary.TryGetValueAsync(tx, workerDescription.Identifier);
                if (result.HasValue)
                {
                    var info = result.Value;

                    await _processorDictionary.TryUpdateAsync(tx, workerDescription.Identifier,
                        new ProcessorInformation()
                        {
                            Address = info.Address,
                            TicksLastUpdated = DateTime.UtcNow.Ticks
                        },
                        info);

                    await tx.CommitAsync();

                    address = info.Address;
                }
                else
                {
                    // spin up the new service here
                    ServiceEventSource.Current.ServiceMessage(this.Context, $"Creating processor for {workerDescription.Identifier}");

                    var appName = Context.CodePackageActivationContext.ApplicationName;
                    var svcName = $"{appName}/{Names.ProcessorSuffix}/{workerDescription.Identifier}";

                    await _fabricClient.ServiceManager.CreateServiceAsync(new StatefulServiceDescription()
                    {
                        HasPersistedState = true,
                        PartitionSchemeDescription = new UniformInt64RangePartitionSchemeDescription(1),
                        ServiceTypeName = Names.ProcessorTypeName,
                        ApplicationName = new System.Uri(appName),
                        ServiceName = new System.Uri(svcName)
                    });

                    ServiceEventSource.Current.ServiceMessage(this.Context, $"Processor for {workerDescription.Identifier} running on {svcName}");

                    await _processorDictionary.AddAsync(tx, workerDescription.Identifier, new ProcessorInformation()
                    {
                        Address = svcName,
                        TicksLastUpdated = DateTime.UtcNow.Ticks
                    });

                    address = svcName;

                    await tx.CommitAsync();
                }

            }

            return address;
        }

        /// <summary>
        /// Optional override to create listeners (like tcp, http) for this service instance.
        /// </summary>
        /// <returns>The collection of listeners.</returns>
        protected override IEnumerable<ServiceReplicaListener> CreateServiceReplicaListeners()
        {
            // instantiate this, and save for later
            var fabricClient = new FabricClient();

            var list = new List<ServiceReplicaListener>(this.CreateServiceRemotingReplicaListeners());

            list.Add(
                new ServiceReplicaListener(serviceContext =>
                    new KestrelCommunicationListener(serviceContext, (url, listener) =>
                    {
                        ServiceEventSource.Current.ServiceMessage(serviceContext, $"Starting Kestrel on {url}");

                        return new WebHostBuilder()
                                    .UseKestrel()
                                    .ConfigureServices(
                                        services => services
                                            .AddSingleton<StatefulServiceContext>(serviceContext)
                                            .AddSingleton<IReliableStateManager>(this.StateManager)
                                            .AddSingleton<FabricClient>(fabricClient))
                                    .UseContentRoot(Directory.GetCurrentDirectory())
                                    .UseStartup<Startup>()
                                    .UseServiceFabricIntegration(listener, ServiceFabricIntegrationOptions.UseUniqueServiceUrl)
                                    .UseUrls(url)
                                    .Build();
                    })));

            return list;
        }
    }
}
