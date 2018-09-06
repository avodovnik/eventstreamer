# EventStreamer - Azure Function & Redis 

This is the Redis implementation of the event streaming through Azure Functions and Redis. 

[![Build status](https://ci.appveyor.com/api/projects/status/90vp8weadevbwf3m/branch/master?svg=true)](https://ci.appveyor.com/project/avodovnik/eventstreamer/branch/master)

## What is it? 

We're using an Azure Function to listen to events, coming into _Event Hub_ in a _cannonical format_. Each event contains a `DataPoint`, which looks like this:

```csharp
public class DataPoint
{
    public string Key { get; set; }
    public DateTime Timestamp { get; set; }
    public string DeviceId { get; set; }
    public string SessionId { get; set; }
    public string SensorType { get; set; }

    public List<string> Names { get; set; } = new List<string>();
    public List<string> Values { get; set; } = new List<string>();
}
```

The reason for this structure is simple: we believed that there are many use cases where we will be collecting similar structured data from sensors, and we knew there will be some similarities (i.e. `SessionId`, etc.). The `Names` and `Values` fields are split so that it makes processing them with something like _Azure Stream Analytics_ easier, should we choose to use it in the future. 

Events come in from multiple _players_, or uniquely identifiable streams. The function separates them based on SessionId. Because this is a specific implementation that is based on a real-world example of one of the partners we're working with, we've also made an assumption on a specific field being there - we build the `Key` based on `SessionId` and `AL` which is the field that we can _assume_ exists. 

This happens in the following snippet of code:

```csharp
private static (DataPoint point, string message) SafelyConvertToDataPoint(byte[] data, ILogger log)
{
    try
    {
    ///     ...
        var key = $"{point.SessionId}:{point.Values[1]}";
    ///     ...
    } 
    /// ...
}
```

You can think of this function as a sort of [demultiplexer](https://en.wikipedia.org/wiki/Multiplexer). It looks at a continuous stream of data, and splits out multiple individual streams (in our case, per player). 

## What does it do with the data? 

When data comes in, the function buffers them into _Redis_ and tries to aggregate until it gets **a full second** of data; the reason here is simple, the customer uses sensors which emit data at 100 Hz. To make meaningful processing of this data possible, we need to aggregate it into a wider time period. To achieve this, we use the `Task ProcessPlayerAsync(string playerId, (DataPoint point, string strRepresentation)[] messages, ILogger log)` method, which looks at the buffer that we have available for each player, and makes a decision to _push time forward_ or not. The method `Task PushTimeAsync(string playerId, ILogger log)`, when called, then pops all the data from the buffer (Redis) queue, and aggregates the values within the `DataPoint`. 

The result of this is an aggregated row that can be sent forward for processing, or used for calculations. 

## How to Run it?

The function needs **two configuration settings** to function properly:

- `incomingEventHub` which is the connection string to the event hub onto which the events are being sent to, and
- `RedisConnectionString` which of course, is the connection string of the _Redis_ cache that is used as a buffer for the events inside Azure Function.

When deploying _locally_, edit the `local.settings.json` file, and add the above fileds into `Values`. 

> Note, even though they are connection strings, they don't go into the `ConnectionStrings` set of values, as that's reserved for SQL connections only. 

When running on Azure, make sure to add the above settings into the `Application Settings` section of the Function. 

## Generating sample data

If you want to generate sample data, you can use the [Streamer.CLI](https://github.com/avodovnik/servicefabric-eventstreamer/tree/master/src/Streamer.CLI) which is a part of this project's `master` branch (for now). The streamer generates random events that satisfy the above assumption. It allows streaming to, and listening from, an `EventHub` with multiple partitions, and ensures each `player` (for our definition of such an entity) is only ever streamed to a single partition. 

You can run the streamer using the following command:

```powershell
dotnet run stream --eh "event hub connection string" --num 5000 --interval 10 -s "98ef1baf-02d3-4122-8d54-02f83577d38f"
```

The `stream` tells the application to _stream_ data into the EventHub, `num` defines the amount of events it produces, the `interval` specifies the frequency it simulates (i.e. `10` means 10ms, or 100 Hz), and the optional `-s` allows you to specify a `Session ID` (this way you can test without flooding Redis with too many new keys).

## Known Problems

1) At the moment, Redis is never cleaned, which means that we are getting a ton of unused keys. [Issue #9](https://github.com/avodovnik/eventstreamer/issues/9). 
2) Sometimes we see timeout exceptions, but the recent addition of `async` code seems to have fixed that. 
