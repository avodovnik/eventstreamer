using Streamer.Common.Contracts;
using Streamer.Common.Models;
using System;
using System.Collections.Generic;
using System.Text;

namespace Streamer.Ingestor
{
    /// <summary>
    /// Implements the router based on the session id.
    /// </summary>
    public class SessionIdRouter : IRouter
    {
        public string GetRoutableIdentifier(DataPoint point)
        {
            if (point == null)
                throw new Exception("Point is null. No routable endpoint found.");

            if (string.IsNullOrEmpty(point.SessionId))
                throw new Exception("Point is null. No routable endpoint found.");

            return point.SessionId;
        }
    }
}
