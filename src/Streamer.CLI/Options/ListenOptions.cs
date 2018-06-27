using CommandLine;
using System;
using System.Collections.Generic;
using System.Text;

namespace Streamer.CLI.Options
{
    [Verb("listen", HelpText = "Listen to data on the event hub", Hidden = false)]
    public class ListenOptions
    {
        [Option('e', "eh", Required = true, HelpText = "EventHub Connection string")]
        public string EventHubConnectionString { get; set; }

        [Option('c', "group", Required = true, HelpText = "ConsumerGroup of the Event Hub")]
        public string ConsumerGroup { get; set; }

        [Option('t', "timeout", Required = false, HelpText = "The timeout for the receiver async operation to wait.")]
        public int TimeoutReciever { get; set; } = 500;
    }

}
