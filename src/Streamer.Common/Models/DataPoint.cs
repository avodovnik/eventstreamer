using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;

namespace Streamer.Common.Models
{
    [DataContract]
    // TODO: Move this to a generic project
    public class DataPoint
    {
        [DataMember]
        public DateTime Timestamp { get; set; }

        [DataMember]
        public string DeviceId { get; set; }

        [DataMember]
        public string SessionId { get; set; }

        [DataMember]
        public string SensorType { get; set; }

        [DataMember]
        public List<string> Names { get; set; } = new List<string>();

        [DataMember]
        public List<string> Values { get; set; } = new List<string>();

        public void AddPoint(string name, string value)
        {
            this.Names.Add(name);

            this.Values.Add(value);
        }

    }
}
