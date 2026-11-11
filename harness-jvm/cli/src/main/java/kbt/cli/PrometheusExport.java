package kbt.cli;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Snapshot metrics in Prometheus' text exposition format for Grafana ingestion. */
final class PrometheusExport {
    private PrometheusExport() { }

    static void write(Path firstRun, Path secondRun, Path file) throws IOException {
        if (Files.exists(file)) throw new IOException("refusing to overwrite " + file);
        String a = Files.readString(firstRun.resolve("metrics.json"));
        String b = Files.readString(secondRun.resolve("metrics.json"));
        StringBuilder out = new StringBuilder();
        metric(out, "kbt_input_records", "Processed input records in the completed replay", "gauge",
                a, b, "input_records_processed", 1);
        metric(out, "kbt_output_records", "Written output records in the completed replay", "gauge",
                a, b, "output_records_written", 1);
        metric(out, "kbt_replay_elapsed_seconds", "Total replay duration including fixture writes", "gauge",
                a, b, "elapsed_ns", 1e9);
        metric(out, "kbt_replay_throughput_records_per_second", "Replay throughput including fixture writes", "gauge",
                a, b, "throughput_records_per_second", 1);
        metric(out, "kbt_gc_collection_seconds", "JVM GC collection time during replay", "gauge",
                a, b, "gc_collection_ms", 1000);
        metric(out, "kbt_gc_collections", "JVM GC collections during replay", "gauge",
                a, b, "gc_collections", 1);
        metric(out, "kbt_peak_observed_heap_bytes", "Peak heap observed at replay record boundaries", "gauge",
                a, b, "peak_observed_heap_bytes", 1);
        metric(out, "kbt_mean_service_seconds", "Mean per-record processor and sink accept time", "gauge",
                a, b, "mean_service_ns", 1e9);
        metric(out, "kbt_service_cv", "Coefficient of variation of per-record service time", "gauge",
                a, b, "service_cv", 1);
        kingman(out, b);
        Files.writeString(file, out.toString());
    }

    private static void kingman(StringBuilder out, String candidate) throws IOException {
        double serviceSeconds = number(candidate, "mean_service_ns") / 1e9;
        double serviceCv = number(candidate, "service_cv");
        if (serviceSeconds <= 0 || serviceCv < 0) return;
        out.append("# HELP kbt_kingman_assumed_arrival_records_per_second Hypothetical arrival rate at 50 or 80 percent single-worker utilization\n")
                .append("# TYPE kbt_kingman_assumed_arrival_records_per_second gauge\n");
        for (double rho : new double[]{0.5, 0.8}) {
            String scenario = rho == 0.5 ? "50pct" : "80pct";
            out.append("kbt_kingman_assumed_arrival_records_per_second{scenario=\"").append(scenario)
                    .append("\"} ").append(rho / serviceSeconds).append('\n');
        }
        out.append("# HELP kbt_kingman_estimated_queue_wait_seconds Modeled G/G/1 mean queue wait assuming arrival CV of one\n")
                .append("# TYPE kbt_kingman_estimated_queue_wait_seconds gauge\n");
        for (double rho : new double[]{0.5, 0.8}) {
            String scenario = rho == 0.5 ? "50pct" : "80pct";
            double wait = rho / (1 - rho) * (1 + serviceCv * serviceCv) / 2 * serviceSeconds;
            out.append("kbt_kingman_estimated_queue_wait_seconds{scenario=\"").append(scenario)
                    .append("\"} ").append(wait).append('\n');
        }
    }

    private static void metric(StringBuilder out, String name, String help, String type,
                               String a, String b, String key, double divisor) throws IOException {
        double left = number(a, key);
        double right = number(b, key);
        if (left < 0 || right < 0) return;
        out.append("# HELP ").append(name).append(' ').append(help).append('\n');
        out.append("# TYPE ").append(name).append(' ').append(type).append('\n');
        sample(out, name, "A", provider(a), left / divisor);
        sample(out, name, "B", provider(b), right / divisor);
    }

    private static void sample(StringBuilder out, String name, String run, String provider, double value) {
        out.append(name).append("{run=\"").append(run).append("\",provider=\"")
                .append(escape(provider)).append("\"} ").append(value).append('\n');
    }

    private static double number(String json, String key) throws IOException {
        Matcher match = Pattern.compile("\\\"" + key + "\\\"\\s*:\\s*([0-9]+(?:\\.[0-9]+)?(?:[eE][+-]?[0-9]+)?)").matcher(json);
        if (match.find()) return Double.parseDouble(match.group(1));
        if (key.equals("mean_service_ns") || key.equals("service_cv")) return -1;
        throw new IOException("missing metric: " + key);
    }

    private static String provider(String json) {
        Matcher match = Pattern.compile("\\\"provider_class\\\"\\s*:\\s*\\\"([^\\\"]*)\\\"").matcher(json);
        return match.find() ? match.group(1) : "unknown";
    }

    private static String escape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n");
    }
}
