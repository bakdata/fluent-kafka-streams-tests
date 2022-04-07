package com.bakdata.fluent_kafka_streams_tests.test_applications;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.Properties;
import lombok.experimental.UtilityClass;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

@UtilityClass
public class MirrorAvro {
    private static final String INPUT_TOPIC = "input";
    private static final String OUTPUT_TOPIC = "output";

    public static Properties getKafkaProperties() {
        final String brokers = "localhost:9092";
        final Properties kafkaConfig = new Properties();
        kafkaConfig.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "mirror");
        kafkaConfig.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        kafkaConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        kafkaConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        return kafkaConfig;
    }

    public static Topology getTopology() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<SpecificRecord, SpecificRecord> input = builder.stream(INPUT_TOPIC);

        input.to(OUTPUT_TOPIC);
        return builder.build();
    }
}
