# Run output specification

Status: draft
Run format version: 1
Last updated: 2026-10-18

One *run* is one replay of one fixture through one job version. A run produces
output records and a metrics record. This document defines both, and the
comparison contract that `kbt compare` relies on.

The fixture format (spec/fixture-format.md) is the input. Nothing in this
document changes how a fixture is read.

## 1. Terminology

- **Run** — one replay. Identified by `run_id`.
- **Role** — `baseline` (the currently deployed version) or `candidate` (the
  version under test). A comparison is always baseline against candidate.
- **Sink** — the harness component that writes a job's output records.
- **Queue depth** — records delivered by the source but not yet acknowledged
  by the job (section 5.2). In a replay this is the harness's own backlog,
  not broker consumer lag.

The key words MUST, MUST NOT, SHOULD and MAY are normative.

## 2. Layout

```
<run-id>/
├── run.json                 run manifest (section 3)
├── metrics.json             counters and gauges summary (section 5)
├── metrics-samples.csv      optional time series, one row per sample interval
├── output/
│   └── <topic>-<partition>.seg   output records, segment framing (section 4)
└── diagnostics/             optional: GC log, JFR recording, job logs
```

`run_id` is a random 128-bit value written as 32 lowercase hex characters,
chosen when the run starts. It is not content-addressed, because the metrics
and timestamps are not known until the run finishes.

A run directory MUST NOT be modified after `outcome` is set in `run.json`.

## 3. Run manifest

```json
{
  "run_format_version": 1,
  "run_id": "3f9a0c1e8b7d4a2f9e6c5b4a39281706",
  "role": "candidate",
  "fixture_id": "sha256 hex of the input fixture manifest",
  "code": {
    "artifact": "orders-job-1.4.2.jar",
    "sha256": "sha256 hex of the job artifact",
    "vcs_revision": "optional, free text"
  },
  "harness": { "name": "kbt-harness-kafka-streams", "version": "0.1.0" },
  "runtime": { "jvm": "21.0.5", "os": "linux-aarch64", "clock": "event-time" },
  "started_at": "2026-10-18T17:00:00Z",
  "finished_at": "2026-10-18T17:04:10Z",
  "outcome": "completed",
  "outputs": [
    {
      "topic": "orders-enriched",
      "topic_id": "optional, Kafka topic UUID if the output topic exists",
      "partition": 0,
      "file": "output/orders-enriched-0.seg",
      "record_count": 9873,
      "crc32c": 2384756019
    }
  ],
  "config": {
    "diff_boundary_ms": 1759795200000,
    "state_mode": "cold"
  }
}
```

Rules:

- `fixture_id` MUST match the fixture the run read. `kbt compare` MUST refuse
  to compare two runs whose `fixture_id` values differ.
- `code.sha256` identifies the version. "Previous version" means the
  baseline run's `code.sha256`.
- `outcome` is one of `completed`, `failed`, `aborted`. A run whose
  `metrics.json` shows any `sink_write_failures` MUST be `failed`.
- `topic_id` is OPTIONAL. Output topics may not exist on any broker, and a
  replay MUST NOT require one.

## 4. Output segments

Output segments use the framing in fixture-format.md section 5 exactly. A
reader for input fixtures reads output segments without changes.

Field meanings differ from the input:

- `partition` is the output partition the sink wrote to.
- `offset` is a sequence number the sink assigns within each output partition,
  starting at 0 and increasing by 1 per record. Output offsets are synthetic.
  They carry no broker meaning.
- `timestamp_ms` is the timestamp the job set on the output record.
- Null keys and null values MUST be preserved with the flags from section 5.2.
- Header order and duplicates MUST be preserved.

The sink MUST write records to each output partition in the order the job
emitted them. A sink that reorders records is a harness defect and the run
MUST be marked `failed`.

## 5. Metrics

### 5.1 Counters

Counters only increase during a run.

| Counter | Meaning |
|---|---|
| `input_records_delivered` | records handed from the source to the job |
| `input_records_processed` | records the job acknowledged as processed |
| `output_records_written` | records the sink wrote successfully |
| `sink_write_failures` | sink writes that failed and were not retried successfully |
| `sink_write_retries` | sink write attempts repeated after a failure |
| `unmatched_side_effects` | outbound calls with no recorded cassette match |

### 5.2 Gauges

Gauges are sampled at a fixed interval, `interval_ms`, and summarised in
`metrics.json`.

| Gauge | Definition |
|---|---|
| `queue_depth` | `input_records_delivered - input_records_processed` at the sample time |
| `heap_used_bytes` | JVM heap used, from `MemoryMXBean` |
| `cpu_process` | process CPU time delta divided by wall time delta, divided by available cores, as a fraction from 0 to 1 |
| `gc_pause_ms` | total GC pause time since the previous sample |
| `thread_count` | live JVM threads |

### 5.3 metrics.json

```json
{
  "metrics_format_version": 1,
  "run_id": "3f9a0c1e8b7d4a2f9e6c5b4a39281706",
  "interval_ms": 1000,
  "counters": {
    "input_records_delivered": 9873,
    "input_records_processed": 9873,
    "output_records_written": 9650,
    "sink_write_failures": 0,
    "sink_write_retries": 2,
    "unmatched_side_effects": 0
  },
  "gauges": {
    "queue_depth": { "max": 412, "p99": 388, "last": 0 },
    "heap_used_bytes": { "max": 734003200, "avg": 512000000 },
    "cpu_process": { "max": 0.92, "avg": 0.61 },
    "gc_pause_ms": { "total": 340, "max": 45 },
    "thread_count": { "max": 48 }
  }
}
```

Metrics are wall-clock measurements. They MUST NOT influence the job's output,
the delivery order, or the output segments. They MUST NOT be compared as
pass/fail by `kbt compare`, except through the counters in 5.1.

## 6. Comparison contract

`kbt compare <baseline-run> <candidate-run>` MUST:

1. Refuse runs with different `fixture_id`.
2. Refuse runs whose `role` values are not one `baseline` and one `candidate`.
3. Compare output segments partition by partition, by `topic` and `partition`.
   A topic or partition present in one run and absent in the other MUST be
   reported as a difference, not skipped.
4. Use the record delivery order from fixture-format.md section 7 when it
   reports differences, so the first difference reported is well defined.
5. Print the counters and gauge summaries of both runs side by side.

Identical runs MUST produce an empty record diff. The harness self-check is
two baseline runs of the same fixture, which MUST compare as identical.

## 7. Conformance

| Vector | Covers |
|---|---|
| 009 | run manifest round trip, including optional `topic_id` absent |
| 010 | two identical runs produce an empty diff |
| 011 | a run with `sink_write_failures > 0` is marked `failed` |
| 012 | output offsets are synthetic and sequential per partition |
| 013 | a topic present in only one run is reported, not skipped |

## 8. Open questions

1. Should a run that completes with a few retried-then-successful writes be
   `completed` or a separate outcome? Current rule: `completed`, with
   `sink_write_retries` recorded.
2. Is per-record queue depth needed, or are interval samples enough? Current
   rule: interval samples only.
3. Should `metrics-samples.csv` be part of v1 or deferred?
4. Where should GC and JFR files live when a run is shared with another
   machine? Current rule: `diagnostics/`, optional, never compared.
