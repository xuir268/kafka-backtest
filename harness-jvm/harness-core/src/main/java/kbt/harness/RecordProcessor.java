package kbt.harness;

import java.util.List;
import kbt.fixture.KafkaRecord;

/**
 * The job under test, reduced to its essentials: one input record in, zero or
 * more output records out.
 *
 * Contract for this first slice:
 * - Deterministic. The same input must always produce the same output.
 * - Stateless. Each call must depend only on its input record.
 * - The output record's partition is used as the output partition. Its offset
 *   is ignored, because the sink assigns output offsets (run-output.md section 4).
 */
@FunctionalInterface
public interface RecordProcessor {

    List<KafkaRecord> process(KafkaRecord input);

    /** A processor that passes every record through unchanged. */
    static RecordProcessor identity() {
        return input -> List.of(input);
    }
}
