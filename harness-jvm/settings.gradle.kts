rootProject.name = "kafka-backtest-jvm"
include("fixture-io", "diff-engine", "cli", "harness-core", "harness-kafka-streams", "harness-flink", "cassette-agent", "conformance-runner")
include("example-orders-job")
