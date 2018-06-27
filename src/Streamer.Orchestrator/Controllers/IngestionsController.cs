using System.Fabric;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Data.Collections;
using Streamer.Orchestrator.Models;

namespace Streamer.Orchestrator.Controllers
{
    [Route("api/ingestion")]
    public class IngestionsController : Controller
    {
        private readonly FabricClient fabricClient;
        private readonly IReliableStateManager stateManager;
        private readonly StatefulServiceContext serviceContext;

        private readonly IReliableDictionary<string, string> ingestorDictionary;
        public IngestionsController(FabricClient fabricClient,
            IReliableStateManager stateManager,
            StatefulServiceContext serviceContext)
        {
            this.fabricClient = fabricClient;
            this.stateManager = stateManager;
            this.serviceContext = serviceContext;

            this.ingestorDictionary = this.stateManager.GetOrAddAsync<IReliableDictionary<string, string>>("orchestrator.IngestionDictionary").Result;
        }

        [HttpGet()]
        public async Task<ActionResult> Details()
        {
            CancellationTokenSource source = new CancellationTokenSource();
            CancellationToken token = source.Token;

            System.Collections.Generic.Dictionary<string, string> genericDictionary = new System.Collections.Generic.Dictionary<string, string>();

            using (var tx = this.stateManager.CreateTransaction())
            {
                var e = await this.ingestorDictionary.CreateEnumerableAsync(tx);

                var asyncEnumerator = e.GetAsyncEnumerator();

                while (await asyncEnumerator.MoveNextAsync(token))
                {
                    genericDictionary.Add(asyncEnumerator.Current.Key, asyncEnumerator.Current.Value);
                }

                await tx.CommitAsync();
            }

            return await Task.FromResult(Json(new { this.serviceContext.CodePackageActivationContext.ApplicationName, Data = genericDictionary }));
        }

        [HttpPost]
        [Route("{name}")]
        public async Task<IActionResult> Post([FromRoute] string name, [FromBody] IngestionApplicationParams parameters)
        {
            using (var tx = this.stateManager.CreateTransaction())
            {
                await ingestorDictionary.TryAddAsync(tx, name, parameters.ConnectionString);
                await tx.CommitAsync();
            }

            return await Task.FromResult(Content("nothing"));
        }
    }
}