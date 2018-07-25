using Microsoft.ServiceFabric.Services.Remoting;
using Microsoft.ServiceFabric.Services.Remoting.FabricTransport;
using System.Threading.Tasks;

[assembly: FabricTransportServiceRemotingProvider(
    RemotingListenerVersion = RemotingListenerVersion.V2, 
    RemotingClientVersion = RemotingClientVersion.V2)]

namespace Streamer.Common.Contracts
{
    public interface IOrchestrator : IService
    {
        /// <summary>
        /// Ensures that a worker service is spun up, with the correct worker description. 
        /// </summary>
        /// <param name="description"></param>
        /// <returns></returns>
        Task<long> OrchestrateWorker(WorkerDescription workerDescription);
    }
}
