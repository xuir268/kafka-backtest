package kbt.diff;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import kbt.fixture.DeliveryOrder;
import kbt.fixture.Header;
import kbt.fixture.KafkaRecord;
import kbt.fixture.Segment;

/**
 * Compares output segments by key. Records with the same key are paired in
 * delivery order (fixture-format.md section 7).
 *
 * Output offsets are synthetic (run-output.md section 4), so they are not
 * compared. A divergence index is the record's position in the baseline's
 * delivery order, or the candidate's delivery order for an extra record.
 */
public final class SegmentComparer {

    private SegmentComparer() {
    }

    public static List<Divergence> compare(Segment baseline, Segment candidate) {
        Map<Key, List<IndexedRecord>> b = groupByKey(DeliveryOrder.sort(baseline.records()));
        Map<Key, List<IndexedRecord>> c = groupByKey(DeliveryOrder.sort(candidate.records()));
        List<Divergence> out = new ArrayList<>();

        for (Map.Entry<Key, List<IndexedRecord>> entry : b.entrySet()) {
            List<IndexedRecord> baselineRecords = entry.getValue();
            List<IndexedRecord> candidateRecords = c.getOrDefault(entry.getKey(), List.of());
            int common = Math.min(baselineRecords.size(), candidateRecords.size());
            for (int i = 0; i < common; i++) {
                IndexedRecord left = baselineRecords.get(i);
                compareRecord(left.index(), left.record(), candidateRecords.get(i).record(), out);
            }
            for (int i = common; i < baselineRecords.size(); i++) {
                IndexedRecord left = baselineRecords.get(i);
                out.add(new Divergence(Divergence.Kind.MISSING_IN_CANDIDATE,
                        left.index(), null, describe(left.record()), null));
            }
            for (int i = common; i < candidateRecords.size(); i++) {
                IndexedRecord right = candidateRecords.get(i);
                out.add(new Divergence(Divergence.Kind.EXTRA_IN_CANDIDATE,
                        right.index(), null, null, describe(right.record())));
            }
        }
        for (Map.Entry<Key, List<IndexedRecord>> entry : c.entrySet()) {
            if (!b.containsKey(entry.getKey())) {
                for (IndexedRecord right : entry.getValue()) {
                    out.add(new Divergence(Divergence.Kind.EXTRA_IN_CANDIDATE,
                            right.index(), null, null, describe(right.record())));
                }
            }
        }
        return out;
    }

    static Map<Key, List<IndexedRecord>> groupByKey(List<KafkaRecord> records) {
        Map<Key, List<IndexedRecord>> groups = new LinkedHashMap<>();
        for (int i = 0; i < records.size(); i++) {
            KafkaRecord record = records.get(i);
            groups.computeIfAbsent(keyOf(record), ignored -> new ArrayList<>())
                    .add(new IndexedRecord(record, i));
        }
        return groups;
    }

    private static Key keyOf(KafkaRecord record) {
        if (record.keyNull()) {
            return new Key(true, new byte[0]);
        }
        ByteBuffer copy = record.key().duplicate();
        byte[] bytes = new byte[copy.remaining()];
        copy.get(bytes);
        return new Key(false, bytes);
    }

    record IndexedRecord(KafkaRecord record, int index) {
    }

    record Key(boolean isNull, byte[] bytes) {
        @Override
        public boolean equals(Object other) {
            return other instanceof Key key
                    && isNull == key.isNull
                    && Arrays.equals(bytes, key.bytes);
        }

        @Override
        public int hashCode() {
            return 31 * Boolean.hashCode(isNull) + Arrays.hashCode(bytes);
        }
    }

    private static void compareRecord(int i, KafkaRecord b, KafkaRecord c, List<Divergence> out) {
        field(out, i, "timestamp_ms", b.timestampMs(), c.timestampMs());
        field(out, i, "log_append_time", b.logAppendTime(), c.logAppendTime());
        field(out, i, "key_null", b.keyNull(), c.keyNull());
        bytes(out, i, "key", b.key(), c.key());
        field(out, i, "value_null", b.valueNull(), c.valueNull());
        bytes(out, i, "value", b.value(), c.value());

        List<Header> bh = b.headers();
        List<Header> ch = c.headers();
        field(out, i, "header_count", bh.size(), ch.size());
        int n = Math.min(bh.size(), ch.size());
        for (int j = 0; j < n; j++) {
            bytes(out, i, "header[" + j + "].key", bh.get(j).key(), ch.get(j).key());
            bytes(out, i, "header[" + j + "].value", bh.get(j).value(), ch.get(j).value());
        }
    }

    private static void field(List<Divergence> out, int i, String name, Object b, Object c) {
        if (!Objects.equals(b, c)) {
            out.add(new Divergence(Divergence.Kind.FIELD, i, name, String.valueOf(b), String.valueOf(c)));
        }
    }

    private static void bytes(List<Divergence> out, int i, String name, ByteBuffer b, ByteBuffer c) {
        if (!b.equals(c)) {
            out.add(new Divergence(Divergence.Kind.FIELD, i, name, text(b), text(c)));
        }
    }

    static String describe(KafkaRecord r) {
        return "ts=" + r.timestampMs()
                + " partition=" + r.partition()
                + " key=" + (r.keyNull() ? "null" : text(r.key()))
                + " value=" + (r.valueNull() ? "null" : text(r.value()));
    }

    private static String text(ByteBuffer bytes) {
        ByteBuffer copy = bytes.duplicate();
        byte[] raw = new byte[copy.remaining()];
        copy.get(raw);
        return new String(raw, StandardCharsets.UTF_8);
    }
}
