#include "kbt_core.h"

#include <cstring>
#include <memory>
#include <new>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

struct stored_record {
    kbt_record value{};
    std::vector<std::pair<kbt_slice, kbt_slice>> headers;
};

struct kbt_segment {
    std::vector<uint8_t> bytes;
    uint32_t partition{};
    int64_t first_offset{};
    std::vector<stored_record> records;
};

namespace {

struct malformed : std::runtime_error {
    using std::runtime_error::runtime_error;
};

void require(size_t pos, size_t len, size_t end, const char *message) {
    if (pos > end || len > end - pos) throw malformed(message);
}

uint16_t u16(const uint8_t *p) {
    return static_cast<uint16_t>(p[0]) | static_cast<uint16_t>(p[1]) << 8;
}

uint32_t u32(const uint8_t *p) {
    return static_cast<uint32_t>(p[0]) | static_cast<uint32_t>(p[1]) << 8 |
           static_cast<uint32_t>(p[2]) << 16 | static_cast<uint32_t>(p[3]) << 24;
}

int64_t i64(const uint8_t *p) {
    uint64_t bits = 0;
    for (unsigned i = 0; i < 8; ++i) bits |= static_cast<uint64_t>(p[i]) << (i * 8);
    int64_t result;
    std::memcpy(&result, &bits, sizeof result);
    return result;
}

uint32_t crc32c(const uint8_t *p, size_t size) {
    uint32_t crc = ~uint32_t{0};
    for (size_t i = 0; i < size; ++i) {
        crc ^= p[i];
        for (unsigned bit = 0; bit < 8; ++bit)
            crc = (crc >> 1) ^ ((crc & 1) ? 0x82f63b78u : 0u);
    }
    return ~crc;
}

kbt_slice slice(const kbt_segment &segment, size_t pos, size_t len, size_t end) {
    require(pos, len, end, "field exceeds frame");
    return {segment.bytes.data() + pos, len};
}

void parse(kbt_segment &segment) {
    const auto &bytes = segment.bytes;
    const size_t size = bytes.size();
    require(0, 32, size, "segment shorter than header");
    const uint8_t *data = bytes.data();
    if (std::memcmp(data, "BTFX", 4) != 0) throw malformed("bad magic");
    if (u16(data + 4) != 1) throw malformed("unsupported format version");
    if (u16(data + 6) != 1) throw malformed("unknown or missing segment flags");
    if (u32(data + 28) != 0) throw malformed("reserved header bytes are non-zero");
    if (crc32c(data, 24) != u32(data + 24)) throw malformed("header crc mismatch");

    segment.partition = u32(data + 8);
    const uint32_t count = u32(data + 12);
    segment.first_offset = i64(data + 16);
    if (count > (size - 32) / 32) throw malformed("record_count exceeds possible frames");
    segment.records.reserve(count);

    size_t pos = 32;
    while (pos < size) {
        require(pos, 32, size, "truncated frame header");
        const uint32_t len = u32(data + pos);
        if (len < 32 || (len & 7) != 0) throw malformed("frame_len below minimum or misaligned");
        require(pos, len, size, "frame extends past end of segment");
        const size_t end = pos + len;
        const uint16_t flags = u16(data + pos + 4);
        if ((flags & ~uint16_t{7}) != 0) throw malformed("reserved record flags set");
        const uint16_t header_count = u16(data + pos + 6);
        const int64_t offset = i64(data + pos + 8);
        const int64_t timestamp = i64(data + pos + 16);
        const uint32_t key_len = u32(data + pos + 24);
        const uint32_t value_len = u32(data + pos + 28);
        if ((flags & 2) && key_len != 0) throw malformed("null key with non-zero key_len");
        if ((flags & 1) && value_len != 0) throw malformed("null value with non-zero value_len");
        if (!segment.records.empty() && offset <= segment.records.back().value.offset)
            throw malformed("offsets not strictly ascending");
        if (segment.records.empty() && offset != segment.first_offset)
            throw malformed("first_offset does not match first record");

        size_t cursor = pos + 32;
        stored_record record;
        record.value = {segment.partition, offset, timestamp, flags, header_count,
                        slice(segment, cursor, key_len, end), {}};
        cursor += key_len;
        record.value.value = slice(segment, cursor, value_len, end);
        cursor += value_len;
        if (header_count > (end - cursor) / 8) throw malformed("truncated header entry");
        record.headers.reserve(header_count);
        for (uint16_t i = 0; i < header_count; ++i) {
            require(cursor, 8, end, "truncated header entry");
            const uint32_t hk_len = u32(data + cursor);
            const uint32_t hv_len = u32(data + cursor + 4);
            cursor += 8;
            kbt_slice hk = slice(segment, cursor, hk_len, end);
            cursor += hk_len;
            kbt_slice hv = slice(segment, cursor, hv_len, end);
            cursor += hv_len;
            record.headers.emplace_back(hk, hv);
        }
        const size_t used = cursor - pos;
        if (used > len || len - used >= 8) throw malformed("frame_len is not minimal padded size");
        for (; cursor < end; ++cursor)
            if (data[cursor] != 0) throw malformed("non-zero padding");
        segment.records.push_back(std::move(record));
        pos = end;
    }
    if (segment.records.size() != count) throw malformed("record_count does not match frames");
}

void diagnostic(char *error, size_t size, const char *message) noexcept {
    if (error == nullptr || size == 0) return;
    const size_t n = std::strlen(message);
    const size_t copied = n < size - 1 ? n : size - 1;
    std::memcpy(error, message, copied);
    error[copied] = '\0';
}

}  // namespace

extern "C" {

kbt_status kbt_segment_open(const uint8_t *bytes, size_t size, kbt_segment **out,
                            char *error, size_t error_size) {
    if (out == nullptr) return KBT_INVALID_ARGUMENT;
    *out = nullptr;
    if (bytes == nullptr || (error == nullptr && error_size != 0)) {
        diagnostic(error, error_size, "invalid argument");
        return KBT_INVALID_ARGUMENT;
    }
    try {
        auto segment = std::make_unique<kbt_segment>();
        segment->bytes.assign(bytes, bytes + size);
        parse(*segment);
        *out = segment.release();
        diagnostic(error, error_size, "");
        return KBT_OK;
    } catch (const malformed &e) {
        diagnostic(error, error_size, e.what());
        return KBT_MALFORMED_SEGMENT;
    } catch (const std::bad_alloc &) {
        diagnostic(error, error_size, "out of memory");
        return KBT_OUT_OF_MEMORY;
    } catch (...) {
        diagnostic(error, error_size, "native decoder failed");
        return KBT_MALFORMED_SEGMENT;
    }
}

void kbt_segment_close(kbt_segment *segment) { delete segment; }
uint32_t kbt_segment_partition(const kbt_segment *segment) { return segment ? segment->partition : 0; }
uint32_t kbt_segment_record_count(const kbt_segment *segment) {
    return segment ? static_cast<uint32_t>(segment->records.size()) : 0;
}
int64_t kbt_segment_first_offset(const kbt_segment *segment) {
    return segment ? segment->first_offset : 0;
}
kbt_status kbt_segment_record_at(const kbt_segment *segment, uint32_t index, kbt_record *out) {
    if (!segment || !out || index >= segment->records.size()) return KBT_INVALID_ARGUMENT;
    *out = segment->records[index].value;
    return KBT_OK;
}
kbt_status kbt_segment_header_at(const kbt_segment *segment, uint32_t record_index,
                                 uint16_t header_index, kbt_slice *key, kbt_slice *value) {
    if (!segment || !key || !value || record_index >= segment->records.size() ||
        header_index >= segment->records[record_index].headers.size()) return KBT_INVALID_ARGUMENT;
    const auto &header = segment->records[record_index].headers[header_index];
    *key = header.first;
    *value = header.second;
    return KBT_OK;
}

}  // extern "C"
