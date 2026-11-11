package kbt.streams;

import java.nio.charset.StandardCharsets;
import java.util.Locale;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

/** Candidate example: uppercases non-null UTF-8 values. */
public final class DemoV2Topology implements TopologyProvider {
    @Override
    public Topology build(String inputTopic, String outputTopic) {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic, Consumed.with(Serdes.ByteArray(), Serdes.ByteArray()))
                .mapValues(value -> value == null ? null
                        : new String(value, StandardCharsets.UTF_8).toUpperCase(Locale.ROOT)
                                .getBytes(StandardCharsets.UTF_8))
                .to(outputTopic, Produced.with(Serdes.ByteArray(), Serdes.ByteArray()));
        return builder.build();
    }
}
