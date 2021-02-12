package com.bakdata.fluent_kafka_streams_tests.test_applications;

import java.util.Properties;
import lombok.Getter;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;

@Getter
public class TopicExtractorApplication {
    private final String inputTopic = "input";
    private final String outputTopic = "output";


    public Topology getTopology() {
        final var builder = new StreamsBuilder();
        builder.stream(this.inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
                .to((key, value, recordContext) -> this.outputTopic);
        return builder.build();
    }

    public Properties getProperties() {
        final Properties properties = new Properties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "dynamic-test-stream");
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:123");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        return properties;
    }
}
