package kbt.harness;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import kbt.fixture.KafkaRecord;
import org.junit.jupiter.api.Test;

final class RecordProcessorTest {

    private static KafkaRecord record(String value) {
        return new KafkaRecord(0, 1, 1000, false,
                ByteBuffer.wrap("k".getBytes(StandardCharsets.UTF_8)), false,
                ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8)), false, List.of());
    }

    @Test
    void identityReturnsTheInputRecordUnchanged() {
        KafkaRecord in = record("a");
        List<KafkaRecord> out = RecordProcessor.identity().process(in);

        assertEquals(1, out.size());
        assertSame(in, out.get(0));
    }
}
