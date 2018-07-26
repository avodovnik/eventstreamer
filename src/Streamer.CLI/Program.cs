using CommandLine;
using Microsoft.Azure.EventHubs;
using Newtonsoft.Json;
using Streamer.CLI.Options;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

/// <summary>
/// Enables some common CLI operations, like streaming to the event hub, and reading from it.
/// </summary>
namespace Streamer.CLI
{
    class Program
    {
        private const int PADDING_WIDTH = 7;
        static void Main(string[] args)
        {
            var result = Parser.Default.ParseArguments<StreamOptions, ListenOptions>(args)
                .WithParsed<StreamOptions>(x => DoStream(x))
                .WithParsed<ListenOptions>(x => DoListen(x))
                .WithNotParsed(y => Console.WriteLine("Required parameters missing."));
        }

        #region Listening
        private static void DoListen(ListenOptions x)
        {
            Console.WriteLine($"Listening started on {x.EventHubConnectionString}");
            var client = EventHubClient.CreateFromConnectionString(x.EventHubConnectionString);

            var info = client.GetRuntimeInformationAsync().Result;

            Console.WriteLine($"Currently looking at {info.PartitionCount} partitions, created at {info.CreatedAt}");

            var listeners = new Dictionary<string, ListenerInfo>();

            Console.WriteLine("Listeners starting... Press <enter> to stop listening.");

            var ct = new ConsoleTable(new string[] { "partitionId", "rcv Count", "offset", "enqueueUtc" }, info.PartitionCount, 20);
            ct.Render();

            int row = 0;
            foreach (var partition in info.PartitionIds)
            {
                var lRow = row;
                listeners.Add(partition, CreatePartitionListener(client, partition, x,
                    (active) =>
                    {
                        if (active)
                        {
                            ct.SetValue(0, lRow, partition);
                        }
                        else
                        {
                            ct.SetValue(0, lRow, String.Empty);
                        }
                    },

                    (li) =>
                    {
                        // TODO: write this
                        ct.SetValue(1, lRow, li.RecieveCount.ToString());
                        ct.SetValue(2, lRow, li.Offset.ToString());
                        ct.SetValue(3, lRow, li.EnqueuedTimeUtc.ToLongTimeString());
                    }));

                row++;
            }

            Console.ReadLine();

            listeners.Values.AsParallel().ForAll(e => e.KillEvent.Set());

            ManualResetEvent.WaitAll(listeners.Values.Select(e => e.FinishedEvent).ToArray());

            Console.Clear();
        }

        private static ListenerInfo CreatePartitionListener(EventHubClient client, string partitionId, ListenOptions x, Action<bool> onListenerActivity, Action<ListenerInfo> onEventsRecieved)
        {
            var li = new ListenerInfo()
            {
                PartitionId = partitionId
            };

            li.Thread = new Thread(() =>
            {
                // TODO: reconsider this
                var reciever = client.CreateReceiver(x.ConsumerGroup, partitionId, EventPosition.FromStart());

                onListenerActivity?.Invoke(true);

                while (!li.KillEvent.WaitOne(1))
                {
                    var events = reciever.ReceiveAsync(1000, TimeSpan.FromMilliseconds(x.TimeoutReciever)).Result;
                    if (events?.Count() > 0)
                    {
                        //Console.WriteLine($"{partitionId} partition, read {events.Count()} events");
                        li.RecieveCount += events.Count();

                        var lastEvent = events.Last();
                        li.Offset = lastEvent.SystemProperties.Offset;
                        li.EnqueuedTimeUtc = lastEvent.SystemProperties.EnqueuedTimeUtc;

                        onEventsRecieved?.Invoke(li);
                    }
                }

                onListenerActivity?.Invoke(false);

                li.FinishedEvent.Set();
            });

            li.Thread.Start();

            return li;
        }

        private class ListenerInfo
        {
            public ListenerInfo()
            {
                this.KillEvent = new ManualResetEvent(false);
                this.FinishedEvent = new ManualResetEvent(false);
                this.RecieveCount = 0;
            }

            public string PartitionId { get; set; }
            public Thread Thread { get; set; }
            public ManualResetEvent KillEvent { get; set; }
            public ManualResetEvent FinishedEvent { get; set; }
            public double RecieveCount { get; set; }
            public string Offset { get; internal set; }
            public DateTime EnqueuedTimeUtc { get; internal set; }
        }
        #endregion

        #region Streaming
        private static void DoStream(StreamOptions x)
        {
            Console.WriteLine($"Streaming called with {x.NumberOfEvents} events, emitting to {x.EventHubConnectionString}.");

            var client = EventHubClient.CreateFromConnectionString(x.EventHubConnectionString);

            var info = client.GetRuntimeInformationAsync();
            info.Wait();

            // we will create as many threads as there are paritions, and just send all partitions data
            var threads = new Dictionary<string, SenderInfo>();
            var countPerPartition = Math.Round((x.NumberOfEvents / info.Result.PartitionCount), 0);

            foreach (var partitionId in info.Result.PartitionIds)
            {
                threads.Add(partitionId,
                    CreatePartitionSender(countPerPartition, x, partitionId, client.CreatePartitionSender(partitionId)));
            }

            ManualResetEvent monitoringEvent = new ManualResetEvent(false);

            // start monitoring thread
            new Thread(() =>
            {
                var s = new StringBuilder();
                foreach (var tx in threads.Values)
                {
                    s.Append(tx.PartitionId.PadRight(PADDING_WIDTH));
                }

                s.Append("total".PadRight(PADDING_WIDTH));

                Console.WriteLine(s.ToString());

                s.Clear();

                Console.WriteLine("".PadRight(PADDING_WIDTH * threads.Count, '_'));


                while (!monitoringEvent.WaitOne(500))
                {
                    double total = 0;
                    foreach (var tx in threads.Values)
                    {
                        total = total + tx.SendCount;

                        s.Append(tx.SendCount.ToString().PadRight(PADDING_WIDTH));
                    }

                    s.Append(total.ToString().PadRight(PADDING_WIDTH));
                    Console.WriteLine(s.ToString());
                    s.Clear();
                }

            }).Start();

            ManualResetEvent.WaitAll(threads.Select(y => y.Value.Event).ToArray());
            monitoringEvent.Set();

            Console.WriteLine("Finished on all threads.");
        }

        private static SenderInfo CreatePartitionSender(double n, StreamOptions x, string id, PartitionSender sender)
        {
            var si = new SenderInfo()
            {
                PartitionId = id,
            };

            si.Thread = new Thread(() =>
            {
                Console.WriteLine($"Thread started to send to {id}, sending {n} events...");

                var epoch = DateTime.UtcNow;
                var sessionId = Guid.NewGuid();
                var deviceId = Guid.NewGuid();

                EventDataBatch batch = null;
                while (n >= 0)
                {
                    if (batch == null)
                    {
                        batch = sender.CreateBatch(new BatchOptions());
                    }

                    var e = GenerateEvent(epoch, si.SendCount, sessionId, deviceId);

                    if (!batch.TryAdd(e))
                    {
                        // flush
                        sender.SendAsync(batch).Wait();

                        batch = null;

                        // and go to continue, because we need to resend the event that failed
                        continue;
                    }

                    // looks like that went through, yay
                    epoch = epoch.AddMilliseconds(x.Interval);

                    Thread.Sleep(x.Pause);
                    n--;
                    si.SendCount++;
                }

                if (batch != null)
                {
                    sender.SendAsync(batch).Wait();
                }

                si.Event.Set();
            });

            si.Thread.Start();

            return si;
        }

        private static EventData GenerateEvent(DateTime timestamp, double index, Guid sessionId, Guid deviceId)
        {
            return new EventData(System.Text.Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(
                new Streamer.Common.Models.DataPoint()
                {
                    Timestamp = timestamp,
                    SessionId = sessionId.ToString(),
                    DeviceId = deviceId.ToString(),
                    SensorType = "Streamer.CLI.v1"
                }
               )));
        }

        private class SenderInfo
        {
            public SenderInfo()
            {
                this.Event = new ManualResetEvent(false);
                this.SendCount = 0;
            }

            public string PartitionId { get; set; }

            public Thread Thread { get; set; }

            public ManualResetEvent Event { get; set; }

            public double SendCount { get; set; }
        }

        #endregion
    }
}
