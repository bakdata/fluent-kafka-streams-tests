package com.bakdata.fluent_kafka_streams_tests.test_applications;

import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Pattern;
import lombok.Getter;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

public class WordCount {
    @Getter
    private final String inputTopic = "wordcount-input";

    @Getter
    private final String outputTopic = "wordcount-output";

    public static void main(final String[] args) {
        final WordCount wordCount = new WordCount();
        final KafkaStreams streams = new KafkaStreams(wordCount.getTopology(), wordCount.getKafkaProperties());
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    public Properties getKafkaProperties() {
        final String brokers = "localhost:9092";
        final Properties kafkaConfig = new Properties();
        kafkaConfig.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount");
        kafkaConfig.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        kafkaConfig.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        kafkaConfig.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        return kafkaConfig;
    }

    public Topology getTopology() {
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> textLines = builder.stream(this.inputTopic);

        final Pattern pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS);
        final KTable<String, Long> wordCounts = textLines
                .flatMapValues(value -> Arrays.asList(pattern.split(value.toLowerCase())))
                .groupBy((key, word) -> word)
                .count();

        wordCounts.toStream().to(this.outputTopic, Produced.with(stringSerde, longSerde));
        return builder.build();
    }
}
