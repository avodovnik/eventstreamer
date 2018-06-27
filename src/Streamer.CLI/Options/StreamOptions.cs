using CommandLine;
using System;
using System.Collections.Generic;
using System.Text;

namespace Streamer.CLI.Options
{
    [Verb("stream", HelpText = "Streams test data to an event hub", Hidden = false)]
    public class StreamOptions
    {
        [Option('n', "num", HelpText = "Defines howm many events you want to send to EH.", Required = true)]
        public double NumberOfEvents { get; set; }

        [Option('i', "interval", HelpText = "Defines the interval for the event timestamp property between individual events.", Required = true)]
        public double Interval { get; set; }

        [Option('p', "pause", Required = false, HelpText = "Defines the time between each message the sender should wait. This can be used to simulate latency.")]
        public int Pause { get; set; }

        [Option('e', "eh", Required = true, HelpText = "Connection String")]
        public string EventHubConnectionString { get; set; }
    }
}
