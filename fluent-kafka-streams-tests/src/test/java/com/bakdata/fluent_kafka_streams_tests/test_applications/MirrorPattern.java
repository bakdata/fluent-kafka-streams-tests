package com.bakdata.fluent_kafka_streams_tests.test_applications;

import java.util.Properties;
import java.util.regex.Pattern;
import lombok.Getter;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

public class MirrorPattern {
    @Getter
    private final String inputPattern1 = ".*-input1";
    @Getter
    private final String inputPattern2 = ".*-input2";

    @Getter
    private final String outputTopic = "output";

    public static void main(final String[] args) {
        final MirrorPattern wordCount = new MirrorPattern();
        final KafkaStreams streams = new KafkaStreams(wordCount.getTopology(), getKafkaProperties());
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    public static Properties getKafkaProperties() {
        final String brokers = "localhost:9092";
        final Properties kafkaConfig = new Properties();
        kafkaConfig.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount");
        kafkaConfig.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        kafkaConfig.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        kafkaConfig.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        return kafkaConfig;
    }

    public Topology getTopology() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> input1 = builder.stream(Pattern.compile(this.inputPattern1));
        final KStream<String, String> input2 = builder.stream(Pattern.compile(this.inputPattern2));

        input1.merge(input2).to(this.outputTopic);
        return builder.build();
    }
}
