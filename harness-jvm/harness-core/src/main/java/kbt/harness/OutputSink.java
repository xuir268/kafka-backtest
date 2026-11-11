package kbt.harness;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import kbt.fixture.Header;
import kbt.fixture.KafkaRecord;
import kbt.fixture.SegmentEncoder;

/**
 * Collects one output topic's records and writes one segment per partition.
 * The caller must invoke accept and writeSegments from a single owner thread.
 */
public final class OutputSink {

    private final String topic;
    private final Map<Integer, List<KafkaRecord>> byPartition = new TreeMap<>();
    private long recordsAccepted;
    private long recordsWritten;
    private boolean writeAttempted;

    public OutputSink(String topic) {
        this.topic = Objects.requireNonNull(topic, "topic");
        if (topic.isBlank() || topic.equals(".") || topic.equals("..")
                || !topic.matches("[A-Za-z0-9._-]+")) {
            throw new IllegalArgumentException("invalid output topic: " + topic);
        }
    }

    /** Assigns the next offset within the record's output partition. */
    public void accept(KafkaRecord out) {
        Objects.requireNonNull(out, "out");
        if (writeAttempted) {
            throw new IllegalStateException("cannot accept records after writing segments");
        }
        List<KafkaRecord> records = byPartition.computeIfAbsent(out.partition(), ignored -> new ArrayList<>());
        long offset = records.size();
        List<Header> headers = new ArrayList<>(out.headers().size());
        for (Header header : out.headers()) {
            headers.add(new Header(copy(header.key()), copy(header.value())));
        }
        records.add(new KafkaRecord(out.partition(), offset, out.timestampMs(), out.logAppendTime(),
                copy(out.key()), out.keyNull(), copy(out.value()), out.valueNull(), headers));
        recordsAccepted++;
    }

    /** Number of records held in memory, including records whose files have not yet been written. */
    public long recordsAccepted() {
        return recordsAccepted;
    }

    /** Number of records in segment files successfully written by the most recent write attempt. */
    public long recordsWritten() {
        return recordsWritten;
    }

    /**
     * Writes one file per partition. Failures retain their paths and causes for
     * the runner to report. This operation can be attempted only once.
     */
    public WriteResult writeSegments(Path dir) {
        Objects.requireNonNull(dir, "dir");
        if (writeAttempted) {
            throw new IllegalStateException("segments have already been written");
        }
        writeAttempted = true;

        try {
            Files.createDirectories(dir);
        } catch (IOException error) {
            return new WriteResult(0, List.of(new WriteFailure(dir, error)));
        }

        int filesWritten = 0;
        List<WriteFailure> failures = new ArrayList<>();
        for (Map.Entry<Integer, List<KafkaRecord>> entry : byPartition.entrySet()) {
            int partition = entry.getKey();
            List<KafkaRecord> records = entry.getValue();
            Path file = dir.resolve(topic + "-" + partition + ".seg");
            byte[] encoded = SegmentEncoder.encode(partition, records);
            Path temporary = null;
            try {
                if (Files.exists(file)) {
                    throw new IOException("refusing to overwrite existing segment " + file);
                }
                temporary = Files.createTempFile(dir, topic + "-" + partition + "-", ".tmp");
                Files.write(temporary, encoded);
                Files.move(temporary, file, StandardCopyOption.ATOMIC_MOVE);
                filesWritten++;
                recordsWritten += records.size();
            } catch (IOException error) {
                failures.add(new WriteFailure(file, error));
            } finally {
                if (temporary != null) {
                    try {
                        Files.deleteIfExists(temporary);
                    } catch (IOException error) {
                        failures.add(new WriteFailure(temporary, error));
                    }
                }
            }
        }
        return new WriteResult(filesWritten, List.copyOf(failures));
    }

    private static ByteBuffer copy(ByteBuffer source) {
        ByteBuffer view = Objects.requireNonNull(source, "buffer").duplicate();
        byte[] bytes = new byte[view.remaining()];
        view.get(bytes);
        return ByteBuffer.wrap(bytes).asReadOnlyBuffer();
    }

    public record WriteFailure(Path file, IOException cause) {
    }

    public record WriteResult(int filesWritten, List<WriteFailure> failures) {
        public boolean succeeded() {
            return failures.isEmpty();
        }

        public int failureCount() {
            return failures.size();
        }
    }
}
