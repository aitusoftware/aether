# Aether

Monitoring for the [Aeron](https://github.com/real-logic/aeron) messaging system.

## Usage

An instance of the Aether client reads counters from one or more Aeron `MediaDriver`s, and
publishes the counter data to a collector. The collector's function is to build a system-wide
view of the Aeron message flows, matching publishers to subscribers.

### Running an Aether Collector

```java
// launch a MediaDriver for publishing counters
final MediaDriver driver = MediaDriver.launchEmbedded(new MediaDriver.Context()
    .threadingMode(ThreadingMode.SHARED)
    .sharedIdleStrategy(new SleepingMillisIdleStrategy(1L)));

// create a publish that will send out counter snapshots
final CounterSnapshotPublisher snapshotPublisher = new CounterSnapshotPublisher(
    new CounterSnapshotPublisher.Context()
        .aeronDirectoryName(driver.aeronDirectoryName()));

// create an Aether agent that will periodically read and publish the counters
final Aether aether = Aether.launch(new Aether.Context()
    .monitoringLocations(Collections.singletonList(
        new Aether.MonitoringLocation("default", "/path/to/monitored-media-driver")))
    .counterSnapshotListener(snapshotPublisher)
    .aeronDirectoryName(driver.aeronDirectoryName())
    .threadingMode(Aether.ThreadingMode.THREADED));
```

### Running an Aether Aggregator

```java
// create a MediaDriver to receive counter snapshots
final MediaDriver mediaDriver = MediaDriver.launchEmbedded(new MediaDriver.Context()
    .threadingMode(ThreadingMode.SHARED)
    .sharedIdleStrategy(new SleepingMillisIdleStrategy(1L)));

// create a subscription to deserialise received snapshots
final CounterSnapshotSubscriber counterSnapshotSubscriber =
    new CounterSnapshotSubscriber(new CounterSnapshotSubscriber.Context()
    .aeronDirectoryName(mediaDriver.aeronDirectoryName())
    .counterSnapshotListener(new ConsolePrinter()));

// poll subscription to update system snapshot
counterSnapshotSubscriber.doWork();
```

## Output

The default `ConsolePrinter` will display aggregate information for all configured contexts:

```
-----------------------------------------
===== System counters for "server" =====
Bytes sent:                         0
Bytes received:                 27584
NAKs sent:                          0
NAKs received:                      0
Errors:                             0
Client timeouts:                    0
===== System counters for "client" =====
Bytes sent:                     27584
Bytes received:                     0
NAKs sent:                          0
NAKs received:                      0
Errors:                             0
Client timeouts:                    0
===== Monitoring 3 channels =====
==== aeron:udp?endpoint=localhost:54567/37 ====

---- Publisher Session 2033564099 ----
| publisher position:                 19584
| publisher limit:                    26240
| sender position:                    26240
| sender limit:                      133440
---- Subscriber ----
| receiver position:                  26240
| receiver HWM:                       26240
| position (1):                        3520
| position (2):                        2432
| position (3):                        2368
-----------------------------------------
==== aeron:udp?endpoint=localhost:54587/37 ====

---- Publisher Session 2033564101 ----
| publisher position:                   320
| publisher limit:                      576
| sender position:                      576
| sender limit:                      131264
---- Subscriber ----
| receiver position:                    576
| receiver HWM:                         576
| position (6):                         192
-----------------------------------------
```

## As an Application

### Collector

To run Aether as a command-line application, runtime configuration can be used:

#### aether-collector.properties

```
# Describes the locations of MediaDrivers that should be monitored
aether.monitoringLocations=client:/path/to/client/media-driver;server:/path/to/server/media-driver
# Tells Aether to publish snapshot over an Aeron Publication
aether.transport=AERON
# Tells Aether to monitor locations and publish
aether.mode=PUBLISHER
# Describes the location where Aether should launch its own MediaDriver for publishing
aeron.dir=/path/to/aether-publisher-media-driver
# Describe the endpoint to publish data to
aether.transport.channel=aeron:udp?endpoint=monitoring-host:18996
```

Then run the collector:

```
java -cp /path/to/aeron-driver.jar:/path/to/aether.jar \
    com.aitusoftware.aether.Aether /path/to/aether-collector.properties
```

### Aggregator

#### aether-aggregator.properties

```
# Tells Aether to receive snapshot over an Aeron Publication
aether.transport=AERON
# Tells Aether to subscribe to events
aether.mode=SUBSCRIBER
# Describes the location where Aether should launch its own MediaDriver for subscribing
aeron.dir=/path/to/aether-subscriber-media-driver
# Describe the endpoint to receive data on
aether.transport.channel=aeron:udp?endpoint=monitoring-host:18996
```

Then run the aggregator:

```
java -cp /path/to/aeron-driver.jar:/path/to/aether.jar \
    com.aitusoftware.aether.Aether /path/to/aether-aggregator.properties
```


## Visualising snapshot data

[Aether-Net](https://github.com/aitusoftware/aether-net) provides a simple UI to display the counter snapshots.
