package kbt.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class MainTest {

    @TempDir
    Path tempDir;

    @Test
    void sampleReplayAndCompareRoundTrip() throws IOException {
        Path sampleDir = tempDir.resolve("sample");
        Path outputDir = tempDir.resolve("output");

        assertEquals(0, Main.sample(sampleDir));
        Path input = sampleDir.resolve("partition-0.seg");
        assertEquals(0, Main.replay(input, "orders", outputDir));
        assertEquals(0, Main.compare(input, outputDir.resolve("orders-0.seg")));
        String metrics = Files.readString(outputDir.resolve("metrics.json"));
        assertTrue(metrics.contains("\"input_records_processed\": 4"));
        assertTrue(metrics.contains("\"output_records_written\": 4"));
        assertTrue(metrics.contains("\"elapsed_ns\": "));
        assertTrue(metrics.contains("\"jvm_version\": \""));
        assertTrue(metrics.contains("\"provider_class\": \"identity\""));
        assertTrue(metrics.contains("\"topics_created\": false"));
        assertTrue(metrics.contains("\"output_offset_type\": \"synthetic"));
        assertTrue(metrics.contains("\"mean_service_ns\": "));
        assertTrue(metrics.contains("\"service_cv\": "));
        assertEquals(1, Main.replay(input, "orders", outputDir));
    }

    @Test
    void missingInputDoesNotCreateOutput() {
        Path outputDir = tempDir.resolve("output");

        assertEquals(3, Main.replay(tempDir.resolve("missing.seg"), "orders", outputDir));
        assertFalse(Files.exists(outputDir));
    }

    @Test
    void invalidTopicDoesNotCreateOutput() throws IOException {
        Path inputDir = Files.createDirectory(tempDir.resolve("input"));
        assertEquals(0, Main.sample(inputDir));
        Path outputDir = tempDir.resolve("output");

        assertEquals(1, Main.replay(inputDir.resolve("partition-0.seg"), "../outside", outputDir));
        assertFalse(Files.exists(outputDir));
    }

    @Test
    void generatedLoadFixtureReplaysAtRequestedSize() throws IOException {
        Path input = tempDir.resolve("load.seg");
        Path output = tempDir.resolve("load-output");
        assertEquals(0, Main.loadSample(12, 64, input));
        assertEquals(0, Main.replay(input, "orders", output));
        assertTrue(Files.readString(output.resolve("metrics.json"))
                .contains("\"input_records_processed\": 12"));
    }

    @Test
    void dashboardEmbedsBothRunsAndRejectsOverwrite() throws IOException {
        Path input = tempDir.resolve("load.seg");
        assertEquals(0, Main.loadSample(12, 64, input));
        Path runA = tempDir.resolve("run-a");
        Path runB = tempDir.resolve("run-b");
        assertEquals(0, Main.replay(input, "orders", runA));
        assertEquals(0, Main.replay(input, "orders", runB));
        Path page = tempDir.resolve("report.html");
        assertEquals(0, Main.dashboard(runA, runB, page));
        String html = Files.readString(page);
        assertTrue(html.contains("run-a"));
        assertTrue(html.contains("run-b"));
        assertTrue(html.contains("GC collection time"));
        assertTrue(html.contains("Output records"));
        assertTrue(html.contains("key-0"));
        assertTrue(html.contains("12 output records"));
        assertTrue(html.contains("Job &amp; JVM"));
        assertTrue(html.contains("Git revision"));
        assertTrue(html.contains("Kafka path &amp; partition safety"));
        assertTrue(html.contains("Hot-partition verdict"));
        assertTrue(html.contains("Broker or cluster restart"));
        assertTrue(html.contains("Kingman per-partition queue estimate"));
        assertTrue(html.contains("assumed arrivals"));
        assertTrue(Files.readString(page.resolveSibling("report.html.metrics.txt"))
                .contains("kbt_mean_service_seconds"));
        assertEquals(1, Main.dashboard(runA, runB, page));
    }

    @Test
    void differentStreamsTopologyVersionsProduceARecordDiff() throws IOException {
        Path inputDir = Files.createDirectory(tempDir.resolve("input"));
        assertEquals(0, Main.sample(inputDir));
        Path input = inputDir.resolve("partition-0.seg");
        Path baseline = tempDir.resolve("baseline");
        Path candidate = tempDir.resolve("candidate");

        assertEquals(0, Main.replayStreams(input, "orders-in", "orders-out",
                "kbt.streams.DemoV1Topology", baseline));
        assertEquals(0, Main.replayStreams(input, "orders-in", "orders-out",
                "kbt.streams.DemoV2Topology", candidate));
        assertEquals(1, Main.compare(baseline.resolve("orders-out-0.seg"),
                candidate.resolve("orders-out-0.seg")));
    }
}
