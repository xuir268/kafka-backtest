package kbt.example;

import java.nio.charset.StandardCharsets;
import kbt.streams.TopologyProvider;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

/** Emits the cumulative number of non-null orders seen for each key. */
public final class OrderCountV1 implements TopologyProvider {
    @Override
    public Topology build(String inputTopic, String outputTopic) {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic, Consumed.with(Serdes.ByteArray(), Serdes.ByteArray()))
                .filter((key, value) -> key != null && value != null)
                .groupByKey()
                .count()
                .toStream()
                .mapValues(count -> Long.toString(count).getBytes(StandardCharsets.UTF_8))
                .to(outputTopic, Produced.with(Serdes.ByteArray(), Serdes.ByteArray()));
        return builder.build();
    }
}
