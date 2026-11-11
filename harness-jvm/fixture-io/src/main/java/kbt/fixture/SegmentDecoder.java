package kbt.fixture;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32C;

/**
 * Turns the bytes of one segment file into a {@link Segment}.
 *
 * Fixtures are untrusted input. Every length read from the file is checked
 * before it is used, and any violation throws {@link MalformedFixtureException}.
 * The decoder reads the whole buffer from index 0 to its limit, and returned
 * key and value buffers are read-only views, not copies.
 */
public final class SegmentDecoder {

    static final int HEADER_SIZE = 32;
    static final int FIXED_FRAME_SIZE = 32;
    static final int ALIGN = 8;
    static final int FORMAT_VERSION = 1;
    static final byte[] MAGIC = {'B', 'T', 'F', 'X'};

    static final int SEG_FLAG_SORTED = 1;
    static final int SEG_FLAG_RESERVED_MASK = 0xFFFE;

    static final int REC_NULL_VALUE = 1 << 0;
    static final int REC_NULL_KEY = 1 << 1;
    static final int REC_LOG_APPEND_TIME = 1 << 2;
    static final int REC_RESERVED_MASK = 0xFFF8;

    private SegmentDecoder() {
    }

    /** Decodes a whole segment file. */
    public static Segment decode(ByteBuffer input) {
        ByteBuffer data = input.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        checkHeader(data);

        int partition = (int) u32(data, 8);
        long count = u32(data, 12);
        long firstOffset = i64(data, 16);

        List<KafkaRecord> records = new ArrayList<>();
        long pos = HEADER_SIZE;
        long end = data.limit();
        while (pos < end) {
            KafkaRecord rec = readFrame(data, pos, partition);
            if (!records.isEmpty() && rec.offset() <= records.get(records.size() - 1).offset()) {
                throw malformed("offsets not strictly ascending");
            }
            records.add(rec);
            pos += frameLength(data, pos);
        }

        if (records.size() != count) {
            throw malformed("record_count does not match frames");
        }
        if (!records.isEmpty() && records.get(0).offset() != firstOffset) {
            throw malformed("first_offset does not match first record");
        }
        return new Segment(partition, firstOffset, records);
    }

    /** Checks magic, version, flags, reserved bytes and the header CRC (spec 5.1). */
    static void checkHeader(ByteBuffer data) {
        requireRange(data, 0, HEADER_SIZE, "segment shorter than header");

        for (int i = 0; i < MAGIC.length; i++) {
            if (data.get(i) != MAGIC[i]) {
                throw malformed("bad magic");
            }
        }

        int version = u16(data, 4);
        if (version != FORMAT_VERSION) {
            throw malformed("unsupported format version " + version);
        }

        int flags = u16(data, 6);
        if ((flags & SEG_FLAG_RESERVED_MASK) != 0 || (flags & SEG_FLAG_SORTED) == 0) {
            throw malformed("unknown or missing segment flags");
        }

        if (u32(data, 28) != 0) {
            throw malformed("reserved header bytes are non-zero");
        }

        CRC32C crc = new CRC32C();
        crc.update(data.slice(0, 24));
        if (crc.getValue() != u32(data, 24)) {
            throw malformed("header crc mismatch");
        }
    }

    /** Parses the record frame at pos, including its headers (spec 5.2 and 5.3). */
    static KafkaRecord readFrame(ByteBuffer data, long pos, int partition) {
        long len = frameLength(data, pos);
        long end = pos + len;

        int flags = u16(data, pos + 4);
        if ((flags & REC_RESERVED_MASK) != 0) {
            throw malformed("reserved record flags set");
        }
        int headerCount = u16(data, pos + 6);
        long offset = i64(data, pos + 8);
        long timestampMs = i64(data, pos + 16);
        long keyLen = u32(data, pos + 24);
        long valueLen = u32(data, pos + 28);

        boolean keyNull = (flags & REC_NULL_KEY) != 0;
        boolean valueNull = (flags & REC_NULL_VALUE) != 0;
        if (keyNull && keyLen != 0) {
            throw malformed("null key with non-zero key_len");
        }
        if (valueNull && valueLen != 0) {
            throw malformed("null value with non-zero value_len");
        }
        if (FIXED_FRAME_SIZE + keyLen + valueLen > len) {
            throw malformed("key and value exceed frame");
        }

        long p = pos + FIXED_FRAME_SIZE;
        ByteBuffer key = slice(data, p, keyLen);
        p += keyLen;
        ByteBuffer value = slice(data, p, valueLen);
        p += valueLen;

        List<Header> headers = new ArrayList<>(headerCount);
        for (int i = 0; i < headerCount; i++) {
            if (p + 8 > end) {
                throw malformed("truncated header entry");
            }
            long hkLen = u32(data, p);
            long hvLen = u32(data, p + 4);
            p += 8;
            if (p + hkLen + hvLen > end) {
                throw malformed("header entry exceeds frame");
            }
            ByteBuffer hk = slice(data, p, hkLen);
            p += hkLen;
            ByteBuffer hv = slice(data, p, hvLen);
            p += hvLen;
            headers.add(new Header(hk, hv));
        }

        if (align8(p - pos) != len) {
            throw malformed("frame_len is not the minimal padded size");
        }
        for (long i = p; i < end; i++) {
            if (data.get((int) i) != 0) {
                throw malformed("non-zero padding");
            }
        }

        return new KafkaRecord(
                partition,
                offset,
                timestampMs,
                (flags & REC_LOG_APPEND_TIME) != 0,
                key,
                keyNull,
                value,
                valueNull,
                headers);
    }

    /** Reads and validates frame_len at pos. Returns the padded frame size. */
    static int frameLength(ByteBuffer data, long pos) {
        requireRange(data, pos, FIXED_FRAME_SIZE, "truncated frame header");
        long len = u32(data, pos);
        if (len < FIXED_FRAME_SIZE || len % ALIGN != 0) {
            throw malformed("frame_len below minimum or misaligned");
        }
        if (pos + len > data.limit()) {
            throw malformed("frame extends past end of segment");
        }
        return (int) len;
    }

    static long u32(ByteBuffer data, long pos) {
        requireRange(data, pos, 4, "truncated segment");
        return data.getInt((int) pos) & 0xFFFFFFFFL;
    }

    static int u16(ByteBuffer data, long pos) {
        requireRange(data, pos, 2, "truncated segment");
        return data.getShort((int) pos) & 0xFFFF;
    }

    static long i64(ByteBuffer data, long pos) {
        requireRange(data, pos, 8, "truncated segment");
        return data.getLong((int) pos);
    }

    static ByteBuffer slice(ByteBuffer data, long pos, long len) {
        requireRange(data, pos, len, "field exceeds segment");
        return data.slice((int) pos, (int) len).asReadOnlyBuffer();
    }

    static void requireRange(ByteBuffer data, long pos, long len, String message) {
        if (pos < 0 || len < 0 || pos + len > data.limit()) {
            throw malformed(message);
        }
    }

    static long align8(long n) {
        return (n + ALIGN - 1) & ~(long) (ALIGN - 1);
    }

    private static MalformedFixtureException malformed(String message) {
        return new MalformedFixtureException(message);
    }
}
