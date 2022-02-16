package com.bakdata.fluent_kafka_streams_tests.test_applications;

import java.util.Properties;
import lombok.Getter;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

public class Mirror {
    @Getter
    private final String inputTopic = "input";

    @Getter
    private final String outputTopic = "output";

    public static void main(final String[] args) {
        final Mirror wordCount = new Mirror();
        final KafkaStreams streams = new KafkaStreams(wordCount.getTopology(), getKafkaProperties());
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    public static Properties getKafkaProperties() {
        final String brokers = "localhost:9092";
        final Properties kafkaConfig = new Properties();
        kafkaConfig.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "mirror");
        kafkaConfig.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        kafkaConfig.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        kafkaConfig.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        return kafkaConfig;
    }

    public Topology getTopology() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> input = builder.stream(this.inputTopic);

        input.to(this.outputTopic);
        return builder.build();
    }
}
