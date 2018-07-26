using System;
using System.Collections.Generic;
using System.Text;

namespace Streamer.Common.Models
{
    // TODO: Move this to a generic project
    public class DataPoint
    {
        public DateTime Timestamp { get; set; }

        public string DeviceId { get; set; }

        public string SessionId { get; set; }

        public string SensorType { get; set; }

        public List<string> Names { get; set; } = new List<string>();

        public List<string> Values { get; set; } = new List<string>();

        public void AddPoint(string name, string value)
        {
            this.Names.Add(name);

            this.Values.Add(value);
        }

    }
}
