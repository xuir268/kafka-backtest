package kbt.harness;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import kbt.fixture.KafkaRecord;
import kbt.fixture.SegmentDecoder;
import kbt.fixture.SegmentEncoder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class ReplayRunnerTest {

    @TempDir
    Path tempDir;

    @Test
    void runsAllInputsAndAssignsOffsetsToEmittedOutputs() throws IOException {
        InputSource source = source();
        RecordProcessor processor = input -> switch (text(input.value())) {
            case "one" -> List.of(output(0, "A"));
            case "two" -> List.of();
            case "three" -> List.of(output(0, "B"), output(1, "C"));
            default -> throw new IllegalArgumentException("unexpected input");
        };

        ReplayRunner.Result result = ReplayRunner.run(source, processor,
                new OutputSink("orders"), tempDir.resolve("output"));

        assertEquals(ReplayRunner.Outcome.COMPLETED, result.outcome());
        assertEquals(3, result.inputRecordsDelivered());
        assertEquals(3, result.inputRecordsProcessed());
        assertEquals(3, result.outputRecordsAccepted());
        assertEquals(3, result.outputRecordsWritten());
        assertEquals(2, result.writeResult().filesWritten());
        assertEquals(List.of(0L, 1L), decode("output/orders-0.seg").stream()
                .map(KafkaRecord::offset).toList());
        assertEquals(List.of(0L), decode("output/orders-1.seg").stream()
                .map(KafkaRecord::offset).toList());
    }

    @Test
    void processingFailureStopsBeforeWritingSegments() throws IOException {
        InputSource source = source();
        RecordProcessor processor = input -> {
            if (text(input.value()).equals("two")) {
                throw new IllegalStateException("processor failed");
            }
            return List.of(output(0, "A"));
        };

        ReplayRunner.Result result = ReplayRunner.run(source, processor,
                new OutputSink("orders"), tempDir.resolve("output"));

        assertEquals(ReplayRunner.Outcome.FAILED, result.outcome());
        assertEquals(2, result.inputRecordsDelivered());
        assertEquals(1, result.inputRecordsProcessed());
        assertEquals(1, result.outputRecordsAccepted());
        assertEquals(0, result.outputRecordsWritten());
        assertNotNull(result.processingFailure());
        assertNull(result.writeResult());
        assertFalse(Files.exists(tempDir.resolve("output")));
    }

    @Test
    void segmentWriteFailureMarksRunFailed() throws IOException {
        Path blocked = Files.createFile(tempDir.resolve("blocked"));
        ReplayRunner.Result result = ReplayRunner.run(source(), input -> List.of(output(0, "A")),
                new OutputSink("orders"), blocked);

        assertEquals(ReplayRunner.Outcome.FAILED, result.outcome());
        assertEquals(3, result.inputRecordsProcessed());
        assertEquals(3, result.outputRecordsAccepted());
        assertEquals(0, result.outputRecordsWritten());
        assertEquals(1, result.writeResult().failureCount());
        assertEquals(blocked, result.writeResult().failures().getFirst().file());
    }

    private InputSource source() throws IOException {
        Path input = tempDir.resolve("input.seg");
        Files.write(input, SegmentEncoder.encode(0, List.of(
                record(0, 0, "one"), record(0, 1, "two"), record(0, 2, "three"))));
        return new InputSource(input);
    }

    private List<KafkaRecord> decode(String file) throws IOException {
        return SegmentDecoder.decode(ByteBuffer.wrap(Files.readAllBytes(tempDir.resolve(file)))).records();
    }

    private static KafkaRecord output(int partition, String value) {
        return record(partition, 99, value);
    }

    private static KafkaRecord record(int partition, long offset, String value) {
        return new KafkaRecord(partition, offset, 1000, false,
                ByteBuffer.wrap("key".getBytes(StandardCharsets.UTF_8)), false,
                ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8)), false, List.of());
    }

    private static String text(ByteBuffer bytes) {
        return StandardCharsets.UTF_8.decode(bytes.duplicate()).toString();
    }
}
