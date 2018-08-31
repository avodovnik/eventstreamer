using System;
using System.Collections.Generic;
using System.Runtime.Serialization;

namespace Streamer.Common.Models
{
    [DataContract]
    public class DataPoint
    {
        public string Key { get; set; }

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
