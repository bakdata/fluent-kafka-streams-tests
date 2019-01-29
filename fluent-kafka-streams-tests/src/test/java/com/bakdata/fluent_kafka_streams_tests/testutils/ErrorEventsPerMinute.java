package com.bakdata.fluent_kafka_streams_tests.testutils;

import com.bakdata.fluent_kafka_streams_tests.testutils.serde.JsonSerde;
import lombok.Getter;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class ErrorEventsPerMinute {
    @Getter
    final String clickInputTopic = "user-click-input";

    @Getter
    final String statusInputTopic = "status-input";

    @Getter
    final String errorOutputTopic = "user-error-output";

    @Getter
    final String alertTopic = "error-alert-output";

    public Properties getKafkaProperties() {
        final String brokers = "localhost:9092";
        final Properties kafkaConfig = new Properties();
        kafkaConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "errors-per-minute");
        kafkaConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        kafkaConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
        kafkaConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class);
        return kafkaConfig;
    }

    public Topology getTopology() {
        final StreamsBuilder builder = new StreamsBuilder();

        // Click Events
        final KStream<Integer, ClickEvent> clickEvents = builder.stream(clickInputTopic,
                Consumed.with(Serdes.Integer(), new JsonSerde<>(ClickEvent.class)));

        final KTable<Windowed<Integer>, Long> counts = clickEvents
                .selectKey(((key, value) -> value.getStatus()))
                .filter(((key, value) -> key >= 400))
                .groupByKey(Serialized.with(Serdes.Integer(), new JsonSerde<>(ClickEvent.class)))
                .windowedBy(TimeWindows.of(TimeUnit.MINUTES.toMillis(1)))  // 1 Minute in ms
                .count();

        // Status codes
        final KTable<Integer, StatusCode> statusCodes = builder.table(statusInputTopic,
                Consumed.with(Serdes.Integer(), new JsonSerde<>(StatusCode.class)));

        // Join
        final KStream<Integer, ErrorOutput> errors = counts.toStream()
                .map((key, value) -> KeyValue.pair(
                        key.key(),
                        new ErrorOutput(key.key(), value, key.window().start(), null /*empty definition*/)))
                .join(statusCodes,
                        (countRecord, code) -> new ErrorOutput(
                                countRecord.statusCode, countRecord.count, countRecord.time, code.getDefinition()),
                        Joined.valueSerde(new JsonSerde<>(ErrorOutput.class)));
        errors.to(errorOutputTopic);

        // Send alert if more than 5x a certain error code per minute
        errors.filter((key, errorOutput) -> errorOutput.count > 5L).to(alertTopic);

        return builder.build();
    }
}

