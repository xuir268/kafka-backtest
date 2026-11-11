package kbt.harness;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import kbt.fixture.KafkaRecord;
import kbt.fixture.SegmentEncoder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class InputSourceTest {

    @TempDir
    Path tempDir;

    @Test
    void preservesPartitionOffsetOrderWhenTimestampsGoBackward() throws IOException {
        List<KafkaRecord> records = List.of(
                record(0, 2000, "later"),
                record(1, 1000, "first"),
                record(2, 1000, "second"));
        Path file = tempDir.resolve("input.seg");
        Files.write(file, SegmentEncoder.encode(0, records));

        InputSource source = new InputSource(file);

        assertEquals(List.of("later", "first", "second"), source.records().stream()
                .map(r -> StandardCharsets.UTF_8.decode(r.value().duplicate()).toString()).toList());
        assertThrows(UnsupportedOperationException.class, () -> source.records().clear());
    }

    @Test
    void missingSegmentReportsReadFailure() {
        assertThrows(IOException.class, () -> new InputSource(tempDir.resolve("missing.seg")));
    }

    private static KafkaRecord record(long offset, long timestamp, String value) {
        return new KafkaRecord(0, offset, timestamp, false,
                ByteBuffer.wrap("key".getBytes(StandardCharsets.UTF_8)), false,
                ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8)), false, List.of());
    }
}
