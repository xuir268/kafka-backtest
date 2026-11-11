package kbt.fixture;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * One captured Kafka record, in the form the fixture stores it.
 *
 * When keyNull or valueNull is true, the matching buffer is empty and the flag
 * is the only thing that says the value is null. A null and an empty value
 * are different in the spec and must not be confused.
 */
public record KafkaRecord(
        int partition,
        long offset,
        long timestampMs,
        boolean logAppendTime,
        ByteBuffer key,
        boolean keyNull,
        ByteBuffer value,
        boolean valueNull,
        List<Header> headers) {

    /** Copies the header list so later changes to the caller's list cannot affect this record. */
    public KafkaRecord {
        headers = List.copyOf(headers);
    }
}
