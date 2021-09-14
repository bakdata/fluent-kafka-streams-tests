package com.bakdata.fluent_kafka_streams_tests.test_applications;

import java.util.Properties;
import lombok.Getter;
import lombok.experimental.UtilityClass;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;

@UtilityClass
public class TopicExtractorApplication {
    public static final String INPUT_TOPIC = "input";
    public static final String OUTPUT_TOPIC = "output";


    public static Topology getTopology() {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
                .to((key, value, recordContext) -> OUTPUT_TOPIC);
        return builder.build();
    }

    public static Properties getProperties() {
        final Properties properties = new Properties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "dynamic-test-stream");
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:123");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        return properties;
    }
}
