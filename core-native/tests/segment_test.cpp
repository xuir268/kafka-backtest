#include "kbt_core.h"

#include <cassert>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

namespace {

void u16(std::vector<uint8_t> &b, size_t p, uint16_t n) {
    b[p] = static_cast<uint8_t>(n);
    b[p + 1] = static_cast<uint8_t>(n >> 8);
}
void u32(std::vector<uint8_t> &b, size_t p, uint32_t n) {
    for (unsigned i = 0; i < 4; ++i) b[p + i] = static_cast<uint8_t>(n >> (8 * i));
}
void i64(std::vector<uint8_t> &b, size_t p, int64_t n) {
    const uint64_t bits = static_cast<uint64_t>(n);
    for (unsigned i = 0; i < 8; ++i) b[p + i] = static_cast<uint8_t>(bits >> (8 * i));
}
uint32_t crc32c(const uint8_t *p, size_t size) {
    uint32_t crc = ~uint32_t{0};
    for (size_t i = 0; i < size; ++i) {
        crc ^= p[i];
        for (unsigned j = 0; j < 8; ++j)
            crc = (crc >> 1) ^ ((crc & 1) ? 0x82f63b78u : 0u);
    }
    return ~crc;
}
void seal(std::vector<uint8_t> &b) { u32(b, 24, crc32c(b.data(), 24)); }

std::vector<uint8_t> sample() {
    std::vector<uint8_t> b(32 + 48, 0);
    std::memcpy(b.data(), "BTFX", 4);
    u16(b, 4, 1);
    u16(b, 6, 1);
    u32(b, 8, 3);
    u32(b, 12, 1);
    i64(b, 16, 100);
    seal(b);
    u32(b, 32, 48);
    u16(b, 36, 4);  // LogAppendTime
    u16(b, 38, 1);  // one header
    i64(b, 40, 100);
    i64(b, 48, -7);
    u32(b, 56, 1);
    u32(b, 60, 1);
    b[64] = 'k';
    b[65] = 'v';
    u32(b, 66, 1);
    u32(b, 70, 1);
    b[74] = 'h';
    b[75] = 'x';
    return b;
}

void rejected(std::vector<uint8_t> b) {
    kbt_segment *segment = reinterpret_cast<kbt_segment *>(1);
    char error[64]{};
    assert(kbt_segment_open(b.data(), b.size(), &segment, error, sizeof error) == KBT_MALFORMED_SEGMENT);
    assert(segment == nullptr);
    assert(error[0] != '\0');
}

}  // namespace

int main() {
    auto empty = sample();
    empty.resize(32);
    u32(empty, 12, 0);
    i64(empty, 16, 0);
    seal(empty);
    kbt_segment *empty_segment = nullptr;
    assert(kbt_segment_open(empty.data(), empty.size(), &empty_segment, nullptr, 0) == KBT_OK);
    assert(kbt_segment_record_count(empty_segment) == 0);
    kbt_segment_close(empty_segment);

    auto b = sample();
    kbt_segment *segment = nullptr;
    char error[64]{};
    assert(kbt_segment_open(b.data(), b.size(), &segment, error, sizeof error) == KBT_OK);
    b[64] = 'X';  // the native handle owns its input bytes
    assert(kbt_segment_partition(segment) == 3);
    assert(kbt_segment_first_offset(segment) == 100);
    assert(kbt_segment_record_count(segment) == 1);
    kbt_record record{};
    assert(kbt_segment_record_at(segment, 0, &record) == KBT_OK);
    assert(record.offset == 100 && record.timestamp_ms == -7 && record.flags == 4);
    assert(record.key.size == 1 && record.key.data[0] == 'k');
    assert(record.value.size == 1 && record.value.data[0] == 'v');
    kbt_slice key{}, value{};
    assert(kbt_segment_header_at(segment, 0, 0, &key, &value) == KBT_OK);
    assert(key.size == 1 && key.data[0] == 'h' && value.data[0] == 'x');
    assert(kbt_segment_header_at(segment, 0, 1, &key, &value) == KBT_INVALID_ARGUMENT);
    kbt_segment_close(segment);

    b = sample();
    u16(b, 36, 3);  // null key and value remain distinct from empty fields
    u32(b, 56, 0);
    u32(b, 60, 0);
    u32(b, 32, 40);
    b.resize(72);
    u32(b, 64, 0);
    u32(b, 68, 0);
    assert(kbt_segment_open(b.data(), b.size(), &segment, error, sizeof error) == KBT_OK);
    assert(kbt_segment_record_at(segment, 0, &record) == KBT_OK);
    assert((record.flags & 3) == 3 && record.key.size == 0 && record.value.size == 0);
    kbt_segment_close(segment);

    b = sample(); b[0] = 'X'; rejected(b);
    b = sample(); b.resize(31); rejected(b);
    b = sample(); u32(b, 32, 4096); rejected(b);
    b = sample(); u32(b, 56, 0xffffffffu); rejected(b);
    b = sample(); u32(b, 70, 0xffffffffu); rejected(b);
    b = sample(); b[79] = 1; rejected(b);
    b = sample(); u16(b, 36, 0x8000); rejected(b);
    b = sample(); u32(b, 12, 2); seal(b); rejected(b);
    b = sample(); i64(b, 16, 99); seal(b); rejected(b);
    b = sample(); b.resize(79); rejected(b);
}
