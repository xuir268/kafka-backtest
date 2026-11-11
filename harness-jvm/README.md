# harness-jvm (Java 21+)
fixture-io has zero dependencies beyond slf4j-api. Framework adapters depend on the customer's own Kafka Streams / Flink version.

## Single-segment replay

From this directory, run the sample through the identity processor:

```bash
./gradlew :cli:run --args='sample /tmp/kbt-sample'
./gradlew :cli:run --args='replay /tmp/kbt-sample/partition-0.seg orders /tmp/kbt-output'
./gradlew :cli:run --args='compare /tmp/kbt-sample/partition-0.seg /tmp/kbt-output/orders-0.seg'
```

`replay` reads one input segment and writes one output segment per partition.
The output directory must not already exist. Each replay writes `metrics.json`
with record counters, elapsed time, observed throughput, JVM GC collection time,
and peak heap observed at record boundaries. These are local process
measurements, so short runs and concurrent workloads can skew them. A full run
manifest and interval-based gauges are not implemented yet.

## Capture from a local Kafka broker

`capture` takes a snapshot of one partition from its current beginning offset
through its end offset at capture start. It reads committed records and does
not commit consumer offsets:

```bash
./gradlew :cli:run --args='capture 127.0.0.1:9092 orders 0 /tmp/orders-0.seg'
./gradlew :cli:run --args='replay /tmp/orders-0.seg orders /tmp/orders-replayed'
./gradlew :cli:run --args='compare /tmp/orders-0.seg /tmp/orders-replayed/orders-0.seg'
```

The output path must not exist. Capture preserves Kafka keys, values, record
timestamps, offsets, and headers. The fixture format currently cannot represent
a null Kafka header value, so capture reports an error for such a record.
Capture also writes `<file.seg>.source.json` with bootstrap address, topic,
partition, and the snapshot offset bounds. Kafka records do not identify their
producing client, so producer identity remains `not recorded`.

## Compare Kafka Streams job versions

`replay-streams` runs a `TopologyProvider` through Kafka Streams'
`TopologyTestDriver`. The supplied demo providers are two versions of a small
job: V1 copies values and V2 uppercases non-null UTF-8 values. Run both on the
same captured segment, using different output directories:

```bash
./gradlew :cli:run --args='replay-streams /tmp/orders-0.seg orders orders-out kbt.streams.DemoV1Topology /tmp/orders-v1'
./gradlew :cli:run --args='replay-streams /tmp/orders-0.seg orders orders-out kbt.streams.DemoV2Topology /tmp/orders-v2'
./gradlew :cli:run --args='compare /tmp/orders-v1/orders-out-0.seg /tmp/orders-v2/orders-out-0.seg'
```

The compare command exits with code 1 when it finds differences; Gradle then
reports `:cli:run` as failed. To test your own job, implement
`kbt.streams.TopologyProvider` with a public no-argument constructor and return
your job's `Topology` from `build(inputTopic, outputTopic)`. Put that class and
its dependencies on the CLI runtime classpath, then pass its fully qualified
class name as `provider-class`. An optional final `provider.jar` argument loads
the provider from a local JAR; its dependencies must also be on the CLI runtime
classpath. Use separate provider classes or builds for baseline and candidate
versions.

This adapter replays partition 0 only. The test driver retains local topology
state during one replay, but it does not run a Kafka broker or model a full
multi-partition deployment. Fixture and run manifests, interval metrics, Flink,
side-effect cassettes, and conformance vectors are still planned.

### External stateful job example

`example-orders-job` is built as a separate JAR. It counts orders by key using
a Kafka Streams state store. V1 emits the running count; V2 applies a changed
business rule and adds one pre-existing order per key. After capturing an input
segment, build the JAR and replay both versions:

```bash
./gradlew :example-orders-job:jar
./gradlew :cli:run --args='replay-streams /tmp/orders-0.seg orders order-count kbt.example.OrderCountV1 /tmp/count-v1 example-orders-job/build/libs/example-orders-job.jar'
./gradlew :cli:run --args='replay-streams /tmp/orders-0.seg orders order-count kbt.example.OrderCountV2 /tmp/count-v2 example-orders-job/build/libs/example-orders-job.jar'
./gradlew :cli:run --args='compare /tmp/count-v1/order-count-0.seg /tmp/count-v2/order-count-0.seg'
```

Each output directory contains `metrics.json`. The compare command returns 1
for the intentional version difference. Throughput from a three-record replay
is useful only to verify measurement plumbing; use a representative fixture,
warmup, and repeated runs before drawing performance conclusions.

For a repeatable traffic check, `load-sample` writes deterministic records with
four alternating keys. For example, generate 2,000 and 4,000 records with
1 KiB values, then replay the same provider against each fixture in separate
output directories:

```bash
./gradlew :cli:run --args='load-sample 2000 1024 /tmp/orders-low.seg'
./gradlew :cli:run --args='load-sample 4000 1024 /tmp/orders-high.seg'
./gradlew :cli:run --args='replay-streams /tmp/orders-low.seg orders order-count kbt.example.OrderCountV1 /tmp/count-low example-orders-job/build/libs/example-orders-job.jar'
./gradlew :cli:run --args='replay-streams /tmp/orders-high.seg orders order-count kbt.example.OrderCountV1 /tmp/count-high example-orders-job/build/libs/example-orders-job.jar'
```

Compare `input_records_processed`, `elapsed_ns`,
`throughput_records_per_second`, `gc_collections`, `gc_collection_ms`, and
`peak_observed_heap_bytes` in the two `metrics.json` files. GC time is the JVM's
reported collection time, not a per-record latency or a diagnosis by itself.
The current sink buffers all output in memory, so its heap use rises with
traffic; large fixtures can expose that limitation.

## Local dashboard

Generate a self-contained HTML comparison from any two replay output
directories. Open the resulting file in a browser; it needs no server:

```bash
./gradlew :cli:run --args='dashboard /tmp/count-low /tmp/count-high /tmp/count-comparison.html'
```

The page shows input volume, throughput, elapsed time, GC collections and
collection time, peak observed heap, and a bounded preview of output records
(first five and last five per run). It reads `metrics.json` and the output
`.seg` files without rerunning either job. The command refuses to overwrite an
existing HTML report.

The dashboard's Kafka path panel shows fixture source, input topic/partition,
offset range, consumer topology, logical output topic and observed output
partitions. Output offsets are synthetic, and offline replay creates no Kafka
topic or partition. A single captured partition cannot establish whether a
live topic will have a hot partition. Broker restart and Streams state restore
time, Kingman queue-delay inputs, and memory alignment also require separate
staging instrumentation; the UI marks them unmeasured and lists the needed
checks. The dashboard includes a Kingman G/G/1 calculator for measured
per-partition arrival rate, mean service time, and arrival/service coefficients
of variation. It reports utilization and an approximate mean queue wait; it is
not a cluster safety verdict.

Each dashboard command also writes a sibling Prometheus-format metrics
snapshot. See [observability/README.md](observability/README.md) for the local
Prometheus and Grafana preview. The Grafana panels show replay throughput,
GC, heap, measured per-record service time, and explicitly modeled Kingman
arrival and queue-wait scenarios.

Each replay report also records the topology provider class, SHA-256 of its
bytecode, SHA-256 of an external provider JAR when supplied, JVM version and
vendor, and max JVM heap. The dashboard shows these side by side. Git revision
is shown as `not recorded` unless the CLI JVM is launched with
`-Dkbt.vcsRevision=<commit>`; a JAR hash is the reproducible identity when no
commit metadata is available.
