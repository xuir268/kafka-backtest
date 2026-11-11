package kbt.harness;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import kbt.fixture.KafkaRecord;
import kbt.fixture.Segment;
import kbt.fixture.SegmentDecoder;

/**
 * Reads one input segment and exposes its records in partition offset order.
 * This first version loads the whole segment into memory before replay.
 */
public final class InputSource {

    private final List<KafkaRecord> records;

    public InputSource(Path segmentFile) throws IOException {
        Objects.requireNonNull(segmentFile, "segmentFile");
        byte[] bytes = Files.readAllBytes(segmentFile);
        Segment segment = SegmentDecoder.decode(ByteBuffer.wrap(bytes));
        records = segment.records();
    }

    /** Returns the decoded records in partition offset order. */
    public List<KafkaRecord> records() {
        return records;
    }
}
