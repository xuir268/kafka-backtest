package kbt.cli;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import kbt.fixture.Header;
import kbt.fixture.KafkaRecord;
import kbt.fixture.SegmentEncoder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

/** Captures one Kafka topic partition up to its end offset at capture start. */
public final class KafkaCapture {

    private KafkaCapture() {
    }

    public static int capture(String bootstrapServers, String topic, int partition, Path outputFile)
            throws IOException {
        if (partition < 0) {
            throw new IllegalArgumentException("partition must be non-negative");
        }
        if (Files.exists(outputFile)) {
            throw new IllegalArgumentException("refusing to overwrite " + outputFile);
        }

        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        List<KafkaRecord> captured = new ArrayList<>();
        long begin;
        long end;

        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(config)) {
            consumer.assign(List.of(topicPartition));
            begin = consumer.beginningOffsets(List.of(topicPartition)).get(topicPartition);
            end = consumer.endOffsets(List.of(topicPartition)).get(topicPartition);
            consumer.seek(topicPartition, begin);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);

            while (consumer.position(topicPartition) < end) {
                if (System.nanoTime() > deadline) {
                    throw new IllegalStateException("timed out capturing " + topicPartition
                            + " before end offset " + end);
                }
                ConsumerRecords<byte[], byte[]> batch = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<byte[], byte[]> record : batch.records(topicPartition)) {
                    if (record.offset() < end) {
                        captured.add(convert(record));
                    }
                }
            }
        }

        Path parent = outputFile.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        Files.write(outputFile, SegmentEncoder.encode(partition, captured));
        String source = "{\n  \"bootstrap_servers\": \"" + escape(bootstrapServers) + "\",\n"
                + "  \"topic\": \"" + escape(topic) + "\",\n"
                + "  \"partition\": " + partition + ",\n"
                + "  \"beginning_offset\": " + begin + ",\n"
                + "  \"end_offset_exclusive\": " + end + "\n}\n";
        Files.writeString(outputFile.resolveSibling(outputFile.getFileName() + ".source.json"), source);
        return captured.size();
    }

    private static String escape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"")
                .replace("\n", "\\n").replace("\r", "\\r");
    }

    private static KafkaRecord convert(ConsumerRecord<byte[], byte[]> record) {
        List<Header> headers = new ArrayList<>();
        for (org.apache.kafka.common.header.Header header : record.headers()) {
            if (header.value() == null) {
                throw new IllegalArgumentException("null Kafka header value at offset " + record.offset()
                        + " is unsupported by the fixture format");
            }
            headers.add(new Header(ByteBuffer.wrap(header.key().getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                    ByteBuffer.wrap(header.value())));
        }
        return new KafkaRecord(record.partition(), record.offset(), record.timestamp(),
                record.timestampType() == TimestampType.LOG_APPEND_TIME,
                bytes(record.key()), record.key() == null,
                bytes(record.value()), record.value() == null, headers);
    }

    private static ByteBuffer bytes(byte[] value) {
        return ByteBuffer.wrap(value == null ? new byte[0] : value);
    }
}
