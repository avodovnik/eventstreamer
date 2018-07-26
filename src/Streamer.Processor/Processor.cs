using System;
using System.Collections.Generic;
using System.Fabric;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Data.Collections;
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
        int _count = 0;
        IReliableConcurrentQueue<DataPoint> _queue;
        private readonly string QueueName;
        //private readonly string Name;

        public Processor(StatefulServiceContext context)
            : base(context)
        {
            var name = this.Context.ServiceName.ToString().Replace("fabric:/", "");
            QueueName = $"processor.queue.{name}";
        }

        public async Task<bool> Process(DataPoint point)
        {
            // TODO: we might mark this as true, and handle "poison" messages
            if (point == null) return false;

            if (_queue == null)
            {
                // we can make an interesting assumption here
                _queue = await this.StateManager.GetOrAddAsync<IReliableConcurrentQueue<DataPoint>>(QueueName);
            }

            //var myDictionary = await this.StateManager.GetOrAddAsync<IReliableDictionary<string, long>>("myDictionary");
            ServiceEventSource.Current.ServiceMessage(this.Context,
                "Process called in Processor, for point: {0} on session {1} through sensor type {2}",
                point.Timestamp,
                point.SessionId,
                point.SensorType);

            using (var tx = this.StateManager.CreateTransaction())
            {
                await _queue.EnqueueAsync(tx, point);
                await tx.CommitAsync();
            }

            _count++;

            return await Task.FromResult(true);
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

        protected override async Task RunAsync(CancellationToken cancellationToken)
        {
            if (_queue == null)
            {
                // we can make an interesting assumption here
                _queue = await this.StateManager.GetOrAddAsync<IReliableConcurrentQueue<DataPoint>>(QueueName);
            }

            // not sure this works?
            while (!cancellationToken.IsCancellationRequested)
            {
                var buffer = new List<DataPoint>();
                bool state = true;
                using (var tx = this.StateManager.CreateTransaction())
                {
                    // process the queue
                    ConditionalValue<DataPoint> point;
                    while ((point = await _queue.TryDequeueAsync(tx, cancellationToken)).HasValue)
                    {
                        buffer.Add(point.Value);

                        if(buffer.Count >= 100)
                        {
                            state = state && Flush(buffer);
                        }
                    }

                    // if all the flushes succeed
                    if (state && Flush(buffer))
                    {
                        await tx.CommitAsync();
                    }
                    else
                    {
                        tx.Abort();
                    }
                }

                Thread.Sleep(TimeSpan.FromSeconds(10));
            }
        }

        private bool Flush(List<DataPoint> buffer)
        {
            // skip empty buffers
            if (buffer.Count == 0) return true;

            ServiceEventSource.Current.ServiceMessage(this.Context,
                 "Flushing buffer of {0} messages, in {1}",
                 buffer.Count,
                 this.QueueName);

            var state = DoProcessing(buffer);
            buffer.Clear();
            return state;
        }

        private bool DoProcessing(List<DataPoint> buffer)
        {
            // DO PROCESSING HERE
            return true;
        }
    }
}
