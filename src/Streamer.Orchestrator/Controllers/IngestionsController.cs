using System.Fabric;
using System.Fabric.Description;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.EventHubs;
using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Data.Collections;
using Streamer.Common;
using Streamer.Common.Extensions;

namespace Streamer.Orchestrator.Controllers
{
    [Route("api/ingestion")]
    public class IngestionsController : Controller
    {
        private readonly FabricClient _fabricClient;
        private readonly IReliableStateManager _stateManager;
        private readonly StatefulServiceContext _serviceContext;

        private readonly IReliableDictionary<string, string> _ingestorDictionary;
        public IngestionsController(FabricClient fabricClient,
            IReliableStateManager stateManager,
            StatefulServiceContext serviceContext)
        {
            this._fabricClient = fabricClient;
            this._stateManager = stateManager;
            this._serviceContext = serviceContext;

            this._ingestorDictionary = this._stateManager
                                        .GetOrAddAsync<IReliableDictionary<string, string>>("orchestrator.IngestionDictionary").Result;
        }

        [HttpGet()]
        public async Task<ActionResult> Details()
        {
            CancellationTokenSource source = new CancellationTokenSource();
            CancellationToken token = source.Token;

            System.Collections.Generic.Dictionary<string, string> genericDictionary = new System.Collections.Generic.Dictionary<string, string>();

            using (var tx = this._stateManager.CreateTransaction())
            {
                var e = await this._ingestorDictionary.CreateEnumerableAsync(tx);

                var asyncEnumerator = e.GetAsyncEnumerator();

                while (await asyncEnumerator.MoveNextAsync(token))
                {
                    genericDictionary.Add(asyncEnumerator.Current.Key, asyncEnumerator.Current.Value);
                }

                await tx.CommitAsync();
            }

            return await Task.FromResult(Json(new { this._serviceContext.CodePackageActivationContext.ApplicationName, Data = genericDictionary }));
        }

        [HttpPost]
        [Route("{name}")]
        public async Task<IActionResult> Post([FromRoute] string name)
        {
            // establish the information for the event hub, to which we're connecting
            // load the connection string from event hub
            var ehConfiguration = this._serviceContext.CodePackageActivationContext
                .GetConfigurationPackageObject("Config")
                .Settings
                .Sections["EventHubConfiguration"];

            var eventHubConnectionString = ehConfiguration.Get("ConnectionString");
            var eventHubConsumerGroup = ehConfiguration.Get("ConsumerGroup");
            var eventHubMaxBatchSize = ehConfiguration.TryGetAsInt("MaxBatchSize", 100);

            // start connecting to the event hub
            var client = EventHubClient.CreateFromConnectionString(eventHubConnectionString);

            // verify the partition exists
            var eventHubInfo = await client.GetRuntimeInformationAsync();

            var lowKey = 0;
            var highKey = eventHubInfo.PartitionCount - 1;

            var appName = _serviceContext.CodePackageActivationContext.ApplicationName;
            var svcName = $"{appName}/{Names.IngestorSuffix}/{name}";

            await _fabricClient.ServiceManager.CreateServiceAsync(new StatefulServiceDescription()
            {
                HasPersistedState = true,
                PartitionSchemeDescription = new UniformInt64RangePartitionSchemeDescription(eventHubInfo.PartitionCount, lowKey, highKey),
                ServiceTypeName = Names.IngestorTypeName,
                ApplicationName = new System.Uri(appName),
                ServiceName = new System.Uri(svcName)
            });

            using (var tx = this._stateManager.CreateTransaction())
            {
                await _ingestorDictionary.AddAsync(tx, svcName, eventHubInfo.PartitionCount.ToString());

                await tx.CommitAsync();
            }

            return Ok($"Ingestor created: {svcName}");
        }

        [HttpDelete]
        [Route("{name}")]
        public async Task<IActionResult> Delete([FromRoute] string name)
        {
            var appName = _serviceContext.CodePackageActivationContext.ApplicationName;
            var svcName = $"{appName}/{Names.IngestorSuffix}/{name}";

            await _fabricClient.ServiceManager.DeleteServiceAsync(new DeleteServiceDescription(new System.Uri(svcName)));

            using (var tx = this._stateManager.CreateTransaction())
            {
                await _ingestorDictionary.TryRemoveAsync(tx, svcName);

                await tx.CommitAsync();
            }

            return Ok();
        }
    }
}