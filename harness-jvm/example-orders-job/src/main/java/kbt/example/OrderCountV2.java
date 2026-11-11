package kbt.example;

import java.nio.charset.StandardCharsets;
import kbt.streams.TopologyProvider;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

/** Candidate rule: reports the count with one pre-existing order per key. */
public final class OrderCountV2 implements TopologyProvider {
    @Override
    public Topology build(String inputTopic, String outputTopic) {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic, Consumed.with(Serdes.ByteArray(), Serdes.ByteArray()))
                .filter((key, value) -> key != null && value != null)
                .groupByKey()
                .count()
                .toStream()
                .mapValues(count -> Long.toString(count + 1).getBytes(StandardCharsets.UTF_8))
                .to(outputTopic, Produced.with(Serdes.ByteArray(), Serdes.ByteArray()));
        return builder.build();
    }
}
