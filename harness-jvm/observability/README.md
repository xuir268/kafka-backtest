# Local Grafana preview

The CLI dashboard command writes an HTML page and a sibling
`<page>.metrics.txt` Prometheus snapshot. This is a completed-run snapshot,
not a live Kafka broker exporter. The Compose preview serves that snapshot to
Prometheus and provisions an eight-panel Grafana dashboard.

Copy the selected snapshot to `index.html.metrics.txt` in the directory set by
`KBT_METRICS_DIR` (default `/private/tmp/kbt-dashboard-preview`), then run:

```bash
docker compose up -d
```

Open Grafana at `http://127.0.0.1:3000/d/kbt-replay/kafka-backtest-c2b7-replay-comparison`.
Prometheus is at `http://127.0.0.1:9090`. Both ports bind to localhost, and
Grafana permits anonymous read-only viewing for this local preview. Stop the
stack with `docker compose down`.

The Kingman panels are **modeled scenarios**: arrival CV is assumed to be 1,
and arrival rates are set to 50% and 80% of the candidate's measured
single-worker service capacity. They are not observed producer rates, p99
latency, or a safe deployment threshold. The source replay also omits broker
traffic, task rebalancing, and state restoration.
