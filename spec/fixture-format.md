# Fixture format v1 (implemented segment subset)

This documents the `.seg` bytes produced and consumed by the JVM fixture
encoder/decoder and the native reader. Manifest, bootstrap, and transactional
sections referenced elsewhere in the repository are still unwritten. All
integers are little-endian. Lengths are unsigned; offsets and timestamps are
signed. Files contain one partition.

## 5.1 Segment header (32 bytes)

| Byte offset | Type | Meaning |
| --- | --- | --- |
| 0 | 4 bytes | ASCII `BTFX` |
| 4 | u16 | Format version `1` |
| 6 | u16 | Flags: bit 0 set (sorted); all other bits zero |
| 8 | u32 | Partition |
| 12 | u32 | Record count |
| 16 | i64 | First offset; the encoder writes zero for an empty segment |
| 24 | u32 | CRC32C of bytes 0–23 |
| 28 | u32 | Reserved, zero |

## 5.2 Record frames

Frames follow the header without gaps. Each starts with a 32-byte fixed part:

| Relative offset | Type | Meaning |
| --- | --- | --- |
| 0 | u32 | Total frame length, including padding |
| 4 | u16 | Flags: bit 0 null value, bit 1 null key, bit 2 LogAppendTime |
| 6 | u16 | Header count |
| 8 | i64 | Offset |
| 16 | i64 | Timestamp in milliseconds |
| 24 | u32 | Key byte length |
| 28 | u32 | Value byte length |

The fixed part is followed by key bytes, value bytes, then the header entries.
A null key or value has length zero; a non-null empty one also has length zero
but its null flag is clear. Offsets within a segment are strictly ascending,
though not necessarily contiguous. The first record offset equals the segment
header's first offset. The number of frames equals record count.

## 5.3 Header entries and padding

Each header entry has `u32 key_len`, `u32 value_len`, key bytes, then value
bytes. Order and duplicate keys are retained. Each frame ends with zero bytes
to make its total length the smallest multiple of eight that fits its fields.

## 5.4 Hostile input

Reject malformed magic, version, flags, reserved bytes, CRC, count, offsets,
lengths, padding, or truncated data. Before any field or slice access, verify
its position and length against the enclosing frame and file, using subtraction
so a hostile length cannot overflow an end calculation. The native ABI copies
the input into an owned handle; returned slices are valid only until close.

## 7 Delivery order

Across segments, replay records in ascending `(timestamp_ms, partition,
offset)` order. The merge is single-threaded and deterministic.
