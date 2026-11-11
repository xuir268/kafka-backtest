package kbt.fixture;

import java.util.List;

/**
 * The decoded records of one source partition, in offset order.
 *
 * Every record must belong to the same partition. The constructor checks this,
 * so a segment built by mistake fails immediately instead of producing a
 * replay that quietly mixes partitions.
 */
public record Segment(int partition, long firstOffset, List<KafkaRecord> records) {

    public Segment {
        records = List.copyOf(records);
        for (KafkaRecord r : records) {
            if (r.partition() != partition)  
                throw new IllegalArgumentException(
                        "record from partition " + r.partition() + " in segment for " + partition);
            }
        }
}
