package kbt.streams;

import org.apache.kafka.streams.Topology;

/** Supplies one version of a Kafka Streams topology for isolated replay. */
@FunctionalInterface
public interface TopologyProvider {
    Topology build(String inputTopic, String outputTopic);
}
