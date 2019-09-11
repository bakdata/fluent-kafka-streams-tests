package com.bakdata.fluent_kafka_streams_tests.test_applications;

import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serdes.LongSerde;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

public class NameJoinGlobalKTable {
    public static final String INPUT_TOPIC = "id-input";
    public static final String NAME_INPUT = "name-input";
    public static final String INTERMEDIATE_TOPIC = "upper-case-input";
    public static final String OUTPUT_TOPIC = "join-output";


    public static void main(final String[] args) {
        final NameJoinGlobalKTable kTableJoin = new NameJoinGlobalKTable();
        final KafkaStreams streams = new KafkaStreams(kTableJoin.getTopology(), kTableJoin.getKafkaProperties());
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    public Properties getKafkaProperties() {
        final String brokers = "localhost:9092";
        final Properties kafkaConfig = new Properties();
        kafkaConfig.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "globalKTableJoin");
        kafkaConfig.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        kafkaConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, LongSerde.class);
        kafkaConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, StringSerde.class);
        return kafkaConfig;
    }

    public Topology getTopology() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<Long, Long> inputStream =
                builder.stream(INPUT_TOPIC, Consumed.with(Serdes.Long(), Serdes.Long()));

        final GlobalKTable<Long, String> joinTable = builder.globalTable(NAME_INPUT);

        inputStream
                .join(joinTable,
                        (id, valueId) -> valueId,
                        (id, name) -> name)
                .to(OUTPUT_TOPIC, Produced.with(Serdes.Long(), Serdes.String()));

        return builder.build();
    }

    public Topology getTopologyWithIntermediateTopic() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<Long, Long> inputStream =
                builder.stream(INPUT_TOPIC, Consumed.with(Serdes.Long(), Serdes.Long()));

        builder.stream(NAME_INPUT, Consumed.with(Serdes.Long(), Serdes.String()))
                .mapValues(name -> name.toUpperCase())
                .to(INTERMEDIATE_TOPIC);

        final GlobalKTable<Long, String> joinTable = builder.globalTable(INTERMEDIATE_TOPIC);

        inputStream
                .join(joinTable,
                        (id, valueId) -> valueId,
                        (id, name) -> name)
                .to(OUTPUT_TOPIC, Produced.with(Serdes.Long(), Serdes.String()));

        return builder.build();
    }

}
