using System;
using System.Collections.Generic;
using System.Fabric;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Remoting.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;
using Streamer.Common.Contracts;
using Streamer.Common.Models;

namespace Streamer.Processor
{
    /// <summary>
    /// An instance of this class is created for each service replica by the Service Fabric runtime.
    /// </summary>
    internal sealed class Processor : StatefulService, IProcessor
    {
        public Processor(StatefulServiceContext context)
            : base(context)
        { }

        public bool Process(DataPoint point)
        {
            ServiceEventSource.Current.ServiceMessage(this.Context,
                "Process called in Processor");

            return false;
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
            // instantiate this, and save for later
            var fabricClient = new FabricClient();

            return new List<ServiceReplicaListener>(this.CreateServiceRemotingReplicaListeners());
        }
    }
}
