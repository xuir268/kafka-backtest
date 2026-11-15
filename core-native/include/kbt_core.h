#ifndef KBT_CORE_H
#define KBT_CORE_H

#include <stddef.h>
#include <stdint.h>

#if defined(_WIN32)
#  if defined(kbt_core_EXPORTS)
#    define KBT_API __declspec(dllexport)
#  else
#    define KBT_API __declspec(dllimport)
#  endif
#else
#  define KBT_API __attribute__((visibility("default")))
#endif

#ifdef __cplusplus
extern "C" {
#endif

/* ABI v1. Input is copied into the handle. Returned slices remain valid until close. */
typedef struct kbt_segment kbt_segment;
typedef enum kbt_status {
    KBT_OK = 0,
    KBT_INVALID_ARGUMENT = 1,
    KBT_MALFORMED_SEGMENT = 2,
    KBT_OUT_OF_MEMORY = 3
} kbt_status;

typedef struct kbt_slice {
    const uint8_t *data;
    size_t size;
} kbt_slice;

typedef struct kbt_record {
    uint32_t partition;
    int64_t offset;
    int64_t timestamp_ms;
    uint16_t flags;       /* bit 0 null value, bit 1 null key, bit 2 LogAppendTime */
    uint16_t header_count;
    kbt_slice key;
    kbt_slice value;
} kbt_record;

/* A non-OK open leaves *out null. error may be null; otherwise it receives a
 * NUL-terminated diagnostic truncated to error_size. Never throws across C. */
KBT_API kbt_status kbt_segment_open(const uint8_t *bytes, size_t size,
                                    kbt_segment **out, char *error, size_t error_size);
KBT_API void kbt_segment_close(kbt_segment *segment);
KBT_API uint32_t kbt_segment_partition(const kbt_segment *segment);
KBT_API uint32_t kbt_segment_record_count(const kbt_segment *segment);
KBT_API int64_t kbt_segment_first_offset(const kbt_segment *segment);
KBT_API kbt_status kbt_segment_record_at(const kbt_segment *segment, uint32_t index,
                                         kbt_record *out);
KBT_API kbt_status kbt_segment_header_at(const kbt_segment *segment, uint32_t record_index,
                                         uint16_t header_index, kbt_slice *key, kbt_slice *value);

#ifdef __cplusplus
}
#endif
#endif
