package kbt.cli;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class DashboardMetricsTest {
    @TempDir Path temp;

    @Test
    void scientificNotationServiceTimeFeedsKingmanAndPrometheus() throws Exception {
        Path a = Files.createDirectory(temp.resolve("a"));
        Path b = Files.createDirectory(temp.resolve("b"));
        String metrics = """
                {"input_records_processed":1000,"output_records_written":1000,
                 "throughput_records_per_second":57.0,"elapsed_ns":17500000000,
                 "gc_collection_ms":4,"gc_collections":2,"peak_observed_heap_bytes":47000000,
                 "mean_service_ns":1.72E7,"service_cv":0.25,"provider_class":"example.Job"}
                """;
        Files.writeString(a.resolve("metrics.json"), metrics);
        Files.writeString(b.resolve("metrics.json"), metrics);
        Path page = temp.resolve("dashboard.html");
        Dashboard.write(a, b, page);
        String html = Files.readString(page);
        assertTrue(html.contains("Measured mean service 17.2 ms/record"));
        assertTrue(html.contains("mean queue wait ≈ 9.1 ms"));
        String prometheus = Files.readString(temp.resolve("dashboard.html.metrics.txt"));
        assertTrue(prometheus.contains("kbt_mean_service_seconds{run=\"B\",provider=\"example.Job\"} 0.0172"));
        assertTrue(prometheus.contains("kbt_kingman_estimated_queue_wait_seconds{scenario=\"80pct\"}"));
    }
}
