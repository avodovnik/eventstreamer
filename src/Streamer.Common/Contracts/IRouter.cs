using Streamer.Common.Models;
using System;
using System.Collections.Generic;
using System.Text;

namespace Streamer.Common.Contracts
{
    /// <summary>
    /// Provides an abstraction to extract the identifier from the data point.
    /// This should be implemented by customer implementations, depending on what
    /// the preference is.
    /// </summary>
    public interface IRouter
    {
        string GetRoutableIdentifier(DataPoint point);
    }
}
