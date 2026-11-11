package kbt.fixture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

final class SegmentDecoderTest {

    private static ByteBuffer buf(String s) {
        return ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8));
    }

    private static String text(ByteBuffer b) {
        ByteBuffer d = b.duplicate();
        byte[] bytes = new byte[d.remaining()];
        d.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static KafkaRecord rec(int partition, long offset, long ts, String value) {
        return new KafkaRecord(partition, offset, ts, false, buf("k"), false, buf(value), false, List.of());
    }

    private static byte[] encodeSingle() {
        return SegmentEncoder.encode(0, List.of(rec(0, 100, 1000, "a")));
    }

    private static void putU32(byte[] b, int pos, int value) {
        ByteBuffer.wrap(b).order(ByteOrder.LITTLE_ENDIAN).putInt(pos, value);
    }

    @Test
    void decodesSingleRecord() {
        Segment seg = SegmentDecoder.decode(ByteBuffer.wrap(encodeSingle()));
        assertEquals(0, seg.partition());
        assertEquals(100, seg.firstOffset());
        assertEquals(1, seg.records().size());

        KafkaRecord r = seg.records().get(0);
        assertEquals(100, r.offset());
        assertEquals(1000, r.timestampMs());
        assertEquals("k", text(r.key()));
        assertEquals("a", text(r.value()));
    }

    /** Vector 003: timestamp ties across two partitions, plus a LogAppendTime record. */
    @Test
    void vector003DeliveryOrder() {
        KafkaRecord c = new KafkaRecord(1, 500, 1000, true, buf("k"), false, buf("c"), false, List.of());
        byte[] p0 = SegmentEncoder.encode(0,
                List.of(rec(0, 100, 1000, "a"), rec(0, 101, 1005, "b"), rec(0, 102, 1000, "e")));
        byte[] p1 = SegmentEncoder.encode(1, List.of(c, rec(1, 501, 1002, "d")));

        List<KafkaRecord> all = new ArrayList<>();
        all.addAll(SegmentDecoder.decode(ByteBuffer.wrap(p0)).records());
        all.addAll(SegmentDecoder.decode(ByteBuffer.wrap(p1)).records());

        List<String> got = DeliveryOrder.sort(all).stream()
                .map(r -> text(r.value()))
                .collect(Collectors.toList());
        assertEquals(List.of("a", "e", "c", "d", "b"), got);
    }

    @Test
    void nullValueIsDistinctFromEmptyValue() {
        KafkaRecord nullValue = new KafkaRecord(
                0, 1, 1, false, buf("k"), false, ByteBuffer.allocate(0), true, List.of());
        KafkaRecord emptyValue = new KafkaRecord(
                0, 2, 2, false, buf("k"), false, buf(""), false, List.of());

        Segment seg = SegmentDecoder.decode(
                ByteBuffer.wrap(SegmentEncoder.encode(0, List.of(nullValue, emptyValue))));

        assertTrue(seg.records().get(0).valueNull());
        assertFalse(seg.records().get(1).valueNull());
    }

    @Test
    void duplicateHeaderKeysKeepTheirOrder() {
        KafkaRecord r = new KafkaRecord(0, 1, 1, false, buf("k"), false, buf("x"), false,
                List.of(new Header(buf("h"), buf("1")), new Header(buf("h"), buf("2"))));

        Segment seg = SegmentDecoder.decode(ByteBuffer.wrap(SegmentEncoder.encode(0, List.of(r))));
        List<Header> headers = seg.records().get(0).headers();

        assertEquals(2, headers.size());
        assertEquals("1", text(headers.get(0).value()));
        assertEquals("2", text(headers.get(1).value()));
    }

    @Test
    void logAppendTimeFlagIsPreserved() {
        KafkaRecord c = new KafkaRecord(0, 1, 1, true, buf("k"), false, buf("c"), false, List.of());
        Segment seg = SegmentDecoder.decode(ByteBuffer.wrap(SegmentEncoder.encode(0, List.of(c))));
        assertTrue(seg.records().get(0).logAppendTime());
    }

    @Test
    void rejectsBadMagic() {
        byte[] b = encodeSingle();
        b[0] = 'X';
        assertThrows(MalformedFixtureException.class, () -> SegmentDecoder.decode(ByteBuffer.wrap(b)));
    }

    @Test
    void rejectsFrameLenBelowMinimum() {
        byte[] b = encodeSingle();
        putU32(b, 32, 4);
        assertThrows(MalformedFixtureException.class, () -> SegmentDecoder.decode(ByteBuffer.wrap(b)));
    }

    @Test
    void rejectsFrameLenPastEndOfSegment() {
        byte[] b = encodeSingle();
        putU32(b, 32, 4096);
        assertThrows(MalformedFixtureException.class, () -> SegmentDecoder.decode(ByteBuffer.wrap(b)));
    }

    @Test
    void rejectsTamperedRecordCountViaHeaderCrc() {
        byte[] b = encodeSingle();
        b[12]++;
        assertThrows(MalformedFixtureException.class, () -> SegmentDecoder.decode(ByteBuffer.wrap(b)));
    }

    @Test
    void rejectsTruncatedSegment() {
        byte[] b = Arrays.copyOf(encodeSingle(), encodeSingle().length - 1);
        assertThrows(MalformedFixtureException.class, () -> SegmentDecoder.decode(ByteBuffer.wrap(b)));
    }
}
