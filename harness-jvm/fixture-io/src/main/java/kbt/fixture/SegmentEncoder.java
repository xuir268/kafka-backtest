package kbt.fixture;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32C;

/**
 * Writes segment bytes per spec section 5. Used by the sample command and by
 * tests, so both produce exactly the format the decoder reads.
 */
public final class SegmentEncoder {

    private SegmentEncoder() {
    }

    /**
     * Encodes one partition's records. Offsets must be strictly ascending and
     * every record must belong to the given partition.
     */
    public static byte[] encode(int partition, List<KafkaRecord> records) {
        for (int i = 0; i < records.size(); i++) {
            KafkaRecord r = records.get(i);
            if (r.partition() != partition) {
                throw new IllegalArgumentException("record from partition " + r.partition()
                        + " in segment for " + partition);
            }
            if (i > 0 && r.offset() <= records.get(i - 1).offset()) {
                throw new IllegalArgumentException("offsets not strictly ascending at index " + i);
            }
        }

        List<byte[]> frames = new ArrayList<>();
        int total = SegmentDecoder.HEADER_SIZE;
        for (KafkaRecord r : records) {
            byte[] f = frame(r);
            frames.add(f);
            total += f.length;
        }

        ByteBuffer out = ByteBuffer.allocate(total).order(ByteOrder.LITTLE_ENDIAN);
        out.put("BTFX".getBytes(StandardCharsets.US_ASCII));
        out.putShort(4, (short) SegmentDecoder.FORMAT_VERSION);
        out.putShort(6, (short) SegmentDecoder.SEG_FLAG_SORTED);
        out.putInt(8, partition);
        out.putInt(12, records.size());
        out.putLong(16, records.isEmpty() ? 0 : records.get(0).offset());
        CRC32C crc = new CRC32C();
        crc.update(out.array(), 0, 24);
        out.putInt(24, (int) crc.getValue());

        out.position(SegmentDecoder.HEADER_SIZE);
        for (byte[] f : frames) {
            out.put(f);
        }
        return out.array();
    }

    private static byte[] frame(KafkaRecord r) {
        int keyLen = r.keyNull() ? 0 : r.key().remaining();
        int valueLen = r.valueNull() ? 0 : r.value().remaining();
        int used = SegmentDecoder.FIXED_FRAME_SIZE + keyLen + valueLen;
        for (Header h : r.headers()) {
            used += 8 + h.key().remaining() + h.value().remaining();
        }
        int len = (used + SegmentDecoder.ALIGN - 1) & ~(SegmentDecoder.ALIGN - 1);

        int flags = (r.valueNull() ? SegmentDecoder.REC_NULL_VALUE : 0)
                | (r.keyNull() ? SegmentDecoder.REC_NULL_KEY : 0)
                | (r.logAppendTime() ? SegmentDecoder.REC_LOG_APPEND_TIME : 0);

        ByteBuffer b = ByteBuffer.allocate(len).order(ByteOrder.LITTLE_ENDIAN);
        b.putInt(0, len);
        b.putShort(4, (short) flags);
        b.putShort(6, (short) r.headers().size());
        b.putLong(8, r.offset());
        b.putLong(16, r.timestampMs());
        b.putInt(24, keyLen);
        b.putInt(28, valueLen);

        b.position(SegmentDecoder.FIXED_FRAME_SIZE);
        if (!r.keyNull()) {
            b.put(r.key().duplicate());
        }
        if (!r.valueNull()) {
            b.put(r.value().duplicate());
        }
        for (Header h : r.headers()) {
            b.putInt(h.key().remaining());
            b.putInt(h.value().remaining());
            b.put(h.key().duplicate());
            b.put(h.value().duplicate());
        }
        return b.array();
    }
}
