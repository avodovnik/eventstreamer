using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.ServiceBus;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using StackExchange.Redis;
using Streamer.Common.Models;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DispatcherFunction
{
    public static class DispatcherFunction
    {
        private static string RedisPrefix = "streamer:";

        [FunctionName("DispatcherFunction")]
        public static async Task Run([EventHubTrigger("final-stream", Connection = "incomingEventHub")]
                EventData[] messages, ILogger log)
        {
            log.LogInformation($"{messages.Length} Events received.");

            var stopwatch = Stopwatch.StartNew();

            // let's parse the points
            var points = messages
                .Select(x => SafelyConvertToDataPoint(x.Body.Array, log))
                .AsParallel()
                .Where(x => x.point != null)
                .ToList()
                .OrderBy(x => x.point.Timestamp);

            log.LogMetric($"Converted messages: {points.Count()} / {messages.Length}", stopwatch.ElapsedMilliseconds);

            var pointsPerPlayer = points.GroupBy(x => x.point.Key);
            foreach (var item in pointsPerPlayer)
            {
                //log.LogInformation($"Player {item.Key} has {item.Count()} messages in this batch.");
                var list = item.ToList();
                await ProcessPlayerAsync(item.Key, item.ToArray(), log);
            }

            stopwatch.Stop();

            log.LogMetric($"Finished processing messages: {points.Count()} / {messages.Length}", stopwatch.ElapsedMilliseconds);
        }

        private static Lazy<ConnectionMultiplexer> lazyConnection = new Lazy<ConnectionMultiplexer>(() =>
        {
            string cacheConnection = GetEnvironmentVariable("RedisConnectionString");
            return ConnectionMultiplexer.Connect(cacheConnection);
        });

        public static ConnectionMultiplexer RedisConnection
        {
            get
            {
                return lazyConnection.Value;
            }
        }

        private static async Task ProcessPlayerAsync(string playerId, (DataPoint point, string strRepresentation)[] messages, ILogger log)
        {

            var cache = RedisConnection.GetDatabase();
            string queueKey = $"{RedisPrefix}player:{playerId}:queue";
            string startKey = $"{RedisPrefix}player:{playerId}:start";

            // check if we have a start
            var start = await cache.StringGetAsync(startKey);

            if (start == RedisValue.Null)
            {
                await cache.StringSetAsync(startKey, messages[0].point.Timestamp.Ticks, TimeSpan.FromDays(1));
                start = messages[0].point.Timestamp.Ticks;
            }

            // we can try the processing
            var startTimeStamp = new DateTime((long)start);
            bool pushTime = false;
            foreach (var (px, kx) in messages)
            {
                if ((px.Timestamp - startTimeStamp).TotalSeconds >= 1)
                {
                    // start the countdown! 
                    await cache.StringSetAsync(startKey, px.Timestamp.Ticks);
                    pushTime = true;
                    startTimeStamp = px.Timestamp;
                }
            }

            // put them into the redis queue
            await cache.ListRightPushAsync(queueKey, messages.Select(x => (RedisValue)x.strRepresentation).ToArray());

            if (pushTime)
            {
                // call push time
                log.LogInformation("Pushing time!");
                await PushTimeAsync(playerId, log);
            }
        }

        private static async Task PushTimeAsync(string playerId, ILogger log)
        {
            // get the playerId queue and stuff
            var cache = RedisConnection.GetDatabase();

            string queueKey = $"{RedisPrefix}player:{playerId}:queue";


            DateTime? start = null;

            // get stuff from the queue into the buffer
            List<DataPoint> buffer = new List<DataPoint>();
            while (true)
            {
                var value = await cache.ListLeftPopAsync(queueKey);

                if (value == RedisValue.Null)
                {
                    log.LogCritical("We've run out of queue and lost some messages! :(");
                    throw new Exception("We've run out of queue.");
                };

                var point = JsonConvert.DeserializeObject<DataPoint>(value);
                if (start == null) start = point.Timestamp;

                buffer.Add(point);
                if ((point.Timestamp - start.Value).TotalSeconds >= 1)
                {
                    // we can process, and stop whatever we're doing
                    break;
                }
            }

            // at this point we have the object, let's average the values for the second
            var dp = new DataPoint();
            var first = buffer.First();
            var countOfFields = first.Values.Count;

            var values = new string[first.Values.Count];

            for (int i = 0; i < countOfFields; i++)
            {
                // TODO: this is horrible, but is a quick way to fix the errors
                values[i] = buffer.Average(x => InternalParse(x.Values[i])).ToString();
            }

            dp.Names = first.Names;
            dp.Values = new List<string>(values);


            // Anze: may god help us
            var jo = new JObject();
            for (int i = 0; i < countOfFields; i++)
            {
                jo[first.Names[i]] = values[i];
            }

            var o = new
            {
                ts = first.Timestamp,
                deviceid = first.DeviceId,
                sessionid = first.SessionId,
                sessionstart = "",
                allvalues = jo
            };

            log.LogInformation($"Row for {playerId}: {JsonConvert.SerializeObject(o)}");
        }

        private static decimal InternalParse(string incoming)
        {
            decimal result;
            if (!decimal.TryParse(incoming, out result))
                return 0;
            return result;
        }

        private static (DataPoint point, string message) SafelyConvertToDataPoint(byte[] data, ILogger log)
        {
            try
            {
                var str = Encoding.UTF8.GetString(data);
                var point = JsonConvert.DeserializeObject<DataPoint>(str);
                // generate the key
                // this is a total hack, but we *assume* the value for player id is second in the array
                var key = $"{point.SessionId}:{point.Values[1]}";
                point.Key = key;
                return (point, str);
            }
            catch (Exception e)
            {
                log.LogCritical("Could not process a message ({1}) due to exception: {0}",
                    e.Message,
                    Encoding.UTF8.GetString(data));
                return (null, null);
            }
        }

        private static string GetEnvironmentVariable(string name)
        {
            return System.Environment.GetEnvironmentVariable(name, EnvironmentVariableTarget.Process);
        }
    }
}
