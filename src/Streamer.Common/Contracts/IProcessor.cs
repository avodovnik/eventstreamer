using Microsoft.ServiceFabric.Services.Remoting;
using Streamer.Common.Models;
using System.Collections.Generic;

namespace Streamer.Common.Contracts
{
    public interface IProcessor : IService
    {
        /// <summary>
        /// Processes the data points in whatever fashion is required.
        /// </summary>
        /// <param name="points"></param>
        /// <returns>Returns true if the processing is successful.</returns>
        bool Process(DataPoint point);
    }
}
