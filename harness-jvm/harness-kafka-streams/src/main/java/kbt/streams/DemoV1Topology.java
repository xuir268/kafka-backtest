package kbt.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

/** Baseline example: copies input records to the output topic. */
public final class DemoV1Topology implements TopologyProvider {
    @Override
    public Topology build(String inputTopic, String outputTopic) {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic, Consumed.with(Serdes.ByteArray(), Serdes.ByteArray()))
                .to(outputTopic, Produced.with(Serdes.ByteArray(), Serdes.ByteArray()));
        return builder.build();
    }
}
