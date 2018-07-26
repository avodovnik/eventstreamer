using Microsoft.ServiceFabric.Services.Remoting;
using Microsoft.ServiceFabric.Services.Remoting.FabricTransport;
using System.Runtime.CompilerServices;

[assembly: FabricTransportServiceRemotingProvider(
    RemotingListenerVersion = RemotingListenerVersion.V2,
    RemotingClientVersion = RemotingClientVersion.V2)]


namespace Streamer.Common
{
    public class AssemblyInfo
    {
        [MethodImplAttribute(MethodImplOptions.NoOptimization)]
        // this class is here purely so that we can stick the assembly attribute here
        // and might be completely redundant. But for now, we'll leave it there.
        public void DoNothing()
        {

        }
    }
}
