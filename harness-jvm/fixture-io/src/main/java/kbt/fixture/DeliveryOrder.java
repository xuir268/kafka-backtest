package kbt.fixture;

import java.util.Comparator;
import java.util.List;

/**
 * Orders records for replay, per spec section 7: ascending by
 * (timestamp_ms, partition, offset).
 */
public final class DeliveryOrder {

    private static final Comparator<KafkaRecord> ORDER =
            Comparator.comparingLong(KafkaRecord::timestampMs)
                    .thenComparingInt(KafkaRecord::partition)
                    .thenComparingLong(KafkaRecord::offset);

    private DeliveryOrder() {
    }

    /** Returns a new sorted list. The input list is not modified. */
    public static List<KafkaRecord> sort(List<KafkaRecord> records) {
        return records.stream().sorted(ORDER).toList();
    }
}
