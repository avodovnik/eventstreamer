
using Microsoft.ServiceFabric.Services.Remoting;

namespace Streamer.Common.Contracts
{
    public interface IOrchestrator : IService
    {
        /// <summary>
        /// Ensures that a worker service is spun up, with the correct worker description. 
        /// </summary>
        /// <param name="description"></param>
        /// <returns></returns>
        long OrchestrateWorker(WorkerDescription description);
    }
}
