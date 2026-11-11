package kbt.diff;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import kbt.fixture.KafkaRecord;
import kbt.fixture.Segment;
import org.junit.jupiter.api.Test;

final class SegmentComparerTest {

    private static ByteBuffer buf(String s) {
        return ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8));
    }

    private static KafkaRecord rec(long offset, long ts, String value) {
        return rec(offset, ts, "k", false, value);
    }

    private static KafkaRecord rec(long offset, long ts, String key, boolean keyNull, String value) {
        return new KafkaRecord(0, offset, ts, false, buf(key), keyNull, buf(value), false, List.of());
    }

    private static Segment seg(KafkaRecord... records) {
        return new Segment(0, records.length == 0 ? 0 : records[0].offset(), List.of(records));
    }

    @Test
    void identicalSegmentsHaveNoDivergence() {
        Segment a = seg(rec(0, 1000, "a"), rec(1, 1005, "b"));
        Segment b = seg(rec(0, 1000, "a"), rec(1, 1005, "b"));
        assertTrue(SegmentComparer.compare(a, b).isEmpty());
    }

    @Test
    void outputOffsetsAreNotCompared() {
        Segment a = seg(rec(0, 1000, "a"), rec(1, 1005, "b"));
        Segment b = seg(rec(40, 1000, "a"), rec(41, 1005, "b"));
        assertTrue(SegmentComparer.compare(a, b).isEmpty());
    }

    @Test
    void changedValueIsReportedAsFieldDivergence() {
        Segment a = seg(rec(0, 1000, "a"));
        Segment b = seg(rec(0, 1000, "x"));

        List<Divergence> d = SegmentComparer.compare(a, b);

        assertEquals(1, d.size());
        assertEquals(Divergence.Kind.FIELD, d.get(0).kind());
        assertEquals("value", d.get(0).field());
        assertEquals("a", d.get(0).baseline());
        assertEquals("x", d.get(0).candidate());
    }

    @Test
    void recordMissingFromCandidateIsReported() {
        Segment a = seg(rec(0, 1000, "a"), rec(1, 1005, "b"));
        Segment b = seg(rec(0, 1000, "a"));

        List<Divergence> d = SegmentComparer.compare(a, b);

        assertEquals(1, d.size());
        assertEquals(Divergence.Kind.MISSING_IN_CANDIDATE, d.get(0).kind());
        assertEquals(1, d.get(0).index());
    }

    @Test
    void recordExtraInCandidateIsReported() {
        Segment a = seg(rec(0, 1000, "a"));
        Segment b = seg(rec(0, 1000, "a"), rec(1, 1005, "b"));

        List<Divergence> d = SegmentComparer.compare(a, b);

        assertEquals(1, d.size());
        assertEquals(Divergence.Kind.EXTRA_IN_CANDIDATE, d.get(0).kind());
        assertEquals(1, d.get(0).index());
    }

    @Test
    void differentKeyDeliveryOrderDoesNotChangeComparison() {
        Segment baseline = seg(rec(0, 1000, "a", false, "first"),
                rec(1, 1000, "b", false, "middle"),
                rec(2, 1000, "a", false, "last"));
        Segment candidate = seg(rec(10, 1000, "b", false, "middle"),
                rec(11, 1000, "a", false, "first"),
                rec(12, 1000, "a", false, "last"));

        assertTrue(SegmentComparer.compare(baseline, candidate).isEmpty());
    }

    @Test
    void missingMiddleKeyDoesNotShiftFollowingRecords() {
        Segment baseline = seg(rec(0, 1000, "a", false, "first"),
                rec(1, 1001, "b", false, "middle"),
                rec(2, 1002, "c", false, "last"));
        Segment candidate = seg(rec(10, 1000, "a", false, "first"),
                rec(11, 1002, "c", false, "last"));

        List<Divergence> divergences = SegmentComparer.compare(baseline, candidate);
        assertEquals(1, divergences.size());
        assertEquals(Divergence.Kind.MISSING_IN_CANDIDATE, divergences.get(0).kind());
        assertEquals(1, divergences.get(0).index());
    }

    @Test
    void extraMiddleKeyDoesNotShiftFollowingRecords() {
        Segment baseline = seg(rec(0, 1000, "a", false, "first"),
                rec(1, 1000, "c", false, "last"));
        Segment candidate = seg(rec(10, 1000, "a", false, "first"),
                rec(11, 1000, "b", false, "middle"),
                rec(12, 1000, "c", false, "last"));

        List<Divergence> divergences = SegmentComparer.compare(baseline, candidate);
        assertEquals(1, divergences.size());
        assertEquals(Divergence.Kind.EXTRA_IN_CANDIDATE, divergences.get(0).kind());
        assertEquals(1, divergences.get(0).index());
    }

    @Test
    void repeatedKeysPairInDeliveryOrder() {
        Segment baseline = seg(rec(0, 1000, "a", false, "first"),
                rec(1, 1001, "a", false, "second"));
        Segment candidate = seg(rec(10, 1000, "a", false, "first"),
                rec(11, 1001, "a", false, "changed"));

        List<Divergence> divergences = SegmentComparer.compare(baseline, candidate);
        assertEquals(1, divergences.size());
        assertEquals(Divergence.Kind.FIELD, divergences.get(0).kind());
        assertEquals("value", divergences.get(0).field());
        assertEquals(1, divergences.get(0).index());
    }

    @Test
    void nullKeyAndEmptyKeyDoNotPair() {
        Segment baseline = seg(rec(0, 1000, "", true, "value"));
        Segment candidate = seg(rec(10, 1000, "", false, "value"));

        List<Divergence> divergences = SegmentComparer.compare(baseline, candidate);
        assertEquals(2, divergences.size());
        assertEquals(Divergence.Kind.MISSING_IN_CANDIDATE, divergences.get(0).kind());
        assertEquals(Divergence.Kind.EXTRA_IN_CANDIDATE, divergences.get(1).kind());
    }
}
