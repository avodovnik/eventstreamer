using Microsoft.ServiceFabric.Services.Remoting;
using Microsoft.ServiceFabric.Services.Remoting.FabricTransport;
using System.Threading.Tasks;

[assembly: FabricTransportServiceRemotingProvider(RemotingListener = RemotingListener.V2Listener, RemotingClient = RemotingClient.V2Client)]

namespace Streamer.Common.Contracts
{
    public interface IOrchestrator : IService
    {
        /// <summary>
        /// Ensures that a worker service is spun up, with the correct worker description. 
        /// </summary>
        /// <param name="description"></param>
        /// <returns></returns>
        Task<long> OrchestrateWorker();
    }
}
