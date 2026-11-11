# Backtest capabilities

Status key: **specified** (spec exists), **designed** (described, no spec yet),
**planned** (named, not designed), **open** (decision pending).

## Input and replay

| Capability | Status | Where it's defined |
|---|---|---|
| Fixture format, segments, framing | specified | spec/fixture-format.md §5 |
| Deterministic delivery order | specified | spec/fixture-format.md §7 |
| Fixture manifest and content-addressed ID | designed | spec/fixture-format.md §4 (vector 001) |
| Capture from production topics | designed | capture agent, Java client |
| Stateless job replay, cold start | designed | harness-core (vector 002) |
| Multi-partition merge | specified | spec/fixture-format.md §7 (vector 003) |
| Non-contiguous offsets, compacted topics | specified | spec/fixture-format.md §4.2 (vector 006) |
| Negative and boundary timestamps | specified | spec/fixture-format.md §6 (vector 007) |
| Replay a time range or offset range | planned | needs a run config spec |
| Replay several topics together | specified | spec/fixture-format.md §7 |

## State and time

| Capability | Status | Where it's defined |
|---|---|---|
| State bootstrap: cold start | specified | spec/fixture-format.md §8 |
| State bootstrap: warm-up prefix | specified | spec/fixture-format.md §8 |
| State bootstrap: changelog restore | designed | spec/fixture-format.md §8 |
| State bootstrap: savepoint restore (Flink) | designed | spec/fixture-format.md §8 |
| Watermarks and windows, computed in the JVM layer | designed | docs/determinism.md |
| Late-arrival policy | planned | needs a semantics file |
| Transactional control records (`read_committed`) | open | spec/fixture-format.md §11, question 1 |

## Side effects

| Capability | Status | Where it's defined |
|---|---|---|
| Record outbound HTTP, gRPC, and DB calls | designed | cassette-agent |
| Replay recorded calls by request signature | designed | cassette-agent |
| Unmatched call reported as a finding | specified | spec/run-output.md §5.1 |

## Output and comparison

| Capability | Status | Where it's defined |
|---|---|---|
| Run output segments and run manifest | specified | spec/run-output.md §3–4 |
| Metrics: counters and gauges | specified | spec/run-output.md §5 |
| `kbt compare` between two runs | specified | spec/run-output.md §6 |
| Exact-match record diff | designed | diff-engine |
| Field-level diff with tolerance | planned | needs a tolerance config spec |
| Divergence classes (clustering) | designed | diff-engine |
| Aggregate drift (per-key counts, sums, percentiles) | planned | diff-engine |
| Schema resolution for Avro and Protobuf | planned | fixture schemas/ folder |

## Gates and reporting

| Capability | Status | Where it's defined |
|---|---|---|
| CI gate: non-zero exit on divergence | planned | needs a thresholds spec |
| Static HTML report | planned | after `kbt compare` |
| Trend across runs over time | planned | after the control-plane decision |

## Decisions still open

1. Transactional control records: capture them, or drop them at capture time.
2. Whether time-range replay belongs in the run config or the fixture manifest.
3. Tolerance config format for the field-level diff.
4. Gate thresholds: per counter, per gauge, or per divergence class.
