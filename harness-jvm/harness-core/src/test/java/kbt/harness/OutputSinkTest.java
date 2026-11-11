package kbt.harness;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import kbt.fixture.KafkaRecord;
import kbt.fixture.Segment;
import kbt.fixture.SegmentDecoder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class OutputSinkTest {

    @TempDir
    Path tempDir;

    private static KafkaRecord record(int partition, long offset, String value) {
        return new KafkaRecord(partition, offset, 1000, false,
                ByteBuffer.wrap("key".getBytes(StandardCharsets.UTF_8)), false,
                ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8)), false, List.of());
    }

    @Test
    void offsetsAreIndependentPerPartitionAndFilesRoundTrip() throws IOException {
        OutputSink sink = new OutputSink("orders");
        sink.accept(record(1, 99, "one"));
        sink.accept(record(0, 99, "first"));
        sink.accept(record(1, 99, "two"));
        sink.accept(record(1, 99, "three"));

        assertEquals(4, sink.recordsAccepted());
        assertEquals(0, sink.recordsWritten());
        OutputSink.WriteResult result = sink.writeSegments(tempDir.resolve("output"));
        assertTrue(result.succeeded());
        assertEquals(2, result.filesWritten());
        assertEquals(4, sink.recordsWritten());

        Segment partition0 = decode(tempDir.resolve("output/orders-0.seg"));
        Segment partition1 = decode(tempDir.resolve("output/orders-1.seg"));
        assertEquals(List.of(0L), partition0.records().stream().map(KafkaRecord::offset).toList());
        assertEquals(List.of(0L, 1L, 2L), partition1.records().stream().map(KafkaRecord::offset).toList());
        assertEquals(List.of("one", "two", "three"),
                partition1.records().stream().map(r -> text(r.value())).toList());
        assertEquals("first", text(partition0.records().getFirst().value()));
        try (var files = Files.list(tempDir.resolve("output"))) {
            assertFalse(files.anyMatch(path -> path.getFileName().toString().endsWith(".tmp")));
        }
    }

    @Test
    void writeFailureIsReportedWithItsPathAndCause() throws IOException {
        Path fileInsteadOfDirectory = Files.createFile(tempDir.resolve("blocked"));
        OutputSink sink = new OutputSink("orders");
        sink.accept(record(0, 99, "first"));

        OutputSink.WriteResult result = sink.writeSegments(fileInsteadOfDirectory);

        assertFalse(result.succeeded());
        assertEquals(1, result.failureCount());
        assertEquals(fileInsteadOfDirectory, result.failures().getFirst().file());
        assertTrue(result.failures().getFirst().cause() instanceof IOException);
        assertEquals(0, sink.recordsWritten());
        assertEquals(1, sink.recordsAccepted());
    }

    @Test
    void partitionFileFailureDoesNotHideOtherSuccessfulWrites() throws IOException {
        Path blockedFile = Files.createDirectory(tempDir.resolve("orders-0.seg"));
        OutputSink sink = new OutputSink("orders");
        sink.accept(record(0, 99, "blocked"));
        sink.accept(record(1, 99, "written"));

        OutputSink.WriteResult result = sink.writeSegments(tempDir);

        assertEquals(1, result.filesWritten());
        assertEquals(1, result.failureCount());
        assertEquals(blockedFile, result.failures().getFirst().file());
        assertEquals(1, sink.recordsWritten());
        assertEquals("written", text(decode(tempDir.resolve("orders-1.seg")).records().getFirst().value()));
    }

    @Test
    void existingSegmentIsPreserved() throws IOException {
        Path existing = tempDir.resolve("orders-0.seg");
        byte[] original = "existing file".getBytes(StandardCharsets.UTF_8);
        Files.write(existing, original);
        OutputSink sink = new OutputSink("orders");
        sink.accept(record(0, 99, "new"));

        OutputSink.WriteResult result = sink.writeSegments(tempDir);

        assertFalse(result.succeeded());
        assertEquals(1, result.failureCount());
        assertArrayEquals(original, Files.readAllBytes(existing));
    }

    @Test
    void acceptedBytesAreUnaffectedByCallerMutation() throws IOException {
        KafkaRecord output = record(0, 99, "old");
        OutputSink sink = new OutputSink("orders");
        sink.accept(output);
        output.value().put(0, (byte) 'N');

        sink.writeSegments(tempDir);

        assertEquals("old", text(decode(tempDir.resolve("orders-0.seg")).records().getFirst().value()));
        assertThrows(IllegalStateException.class, () -> sink.accept(record(0, 0, "late")));
    }

    private static Segment decode(Path path) throws IOException {
        return SegmentDecoder.decode(ByteBuffer.wrap(Files.readAllBytes(path)));
    }

    private static String text(ByteBuffer buffer) {
        return StandardCharsets.UTF_8.decode(buffer.duplicate()).toString();
    }
}
