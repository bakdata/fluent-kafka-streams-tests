package com.bakdata.fluent_kafka_streams_tests.test_applications;

import com.bakdata.fluent_kafka_streams_tests.serde.JsonSerde;
import java.time.Duration;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serdes.LongSerde;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.WindowStore;

public class SuppressionApplication {
    public static final String INPUT_TOPIC = "event-input";
    public static final String OUTPUT_TOPIC = "aggregate-output";

    public Properties getKafkaProperties() {
        final Properties kafkaConfig = new Properties();
        kafkaConfig.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "suppressionTest");
        kafkaConfig.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, LongSerde.class);
        kafkaConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, StringSerde.class);
        return kafkaConfig;
    }

    public Topology getTopology() {
        final StreamsBuilder builder = new StreamsBuilder();
        final Duration graceDuration = Duration.ofSeconds(30);
        final InputEventSerde inputEventSerde = new InputEventSerde();
        final Serde<String> stringSerde = Serdes.String();
        final AggEventSerde aggEventSerde = new AggEventSerde();

        final KStream<String, InputEvent> input = builder.stream(INPUT_TOPIC, Consumed.with(stringSerde, inputEventSerde));
        final KStream<String, AggEvent> eventStream = input
                .map((key, value) -> KeyValue.pair(value.getKey(), AggEvent.fromValue(value)))
                .groupByKey(Grouped.with(stringSerde, aggEventSerde))
                .windowedBy(TimeWindows.of(Duration.ofMinutes(5L)).grace(graceDuration))
                .reduce(AggEvent::combine, Materialized.<String, AggEvent, WindowStore<Bytes, byte[]>>as(
                        "aggregated_events")
                        .withCachingDisabled()
                        .withKeySerde(stringSerde)
                        .withValueSerde(aggEventSerde)
                        .withRetention(Duration.ofMinutes(10L).plus(graceDuration))
                )
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream()
                .map((window, value) -> KeyValue.pair(window.key(), value));
        eventStream.to(OUTPUT_TOPIC, Produced.with(stringSerde, aggEventSerde));
        return builder.build();
    }

    public static class InputEventSerde extends JsonSerde<InputEvent> {
        public InputEventSerde() {
            super(InputEvent.class);
        }
    }

    public static class AggEventSerde extends JsonSerde<AggEvent> {
        public AggEventSerde() {
            super(AggEvent.class);
        }
    }


    @Data
    @NoArgsConstructor
    public static class InputEvent {
        private String value;
        private String key;

        public InputEvent(final String key, final String value) {
            this.key = key;
            this.value = value;
        }
    }

    @Data
    public static class AggEvent {
        private Set<String> events;

        public AggEvent() {
            this.events = new HashSet<>();
        }

        public AggEvent(final Set<String> events) {
            this.events = events;
        }

        public AggEvent combine(final AggEvent other) {
            final HashSet<String> set = new HashSet<>();
            set.addAll(other.getEvents());
            set.addAll(this.getEvents());
            return new AggEvent(set);
        }

        public AggEvent add(final InputEvent inputEvent) {
            this.events.add(inputEvent.getValue());
            return this;
        }

        public static AggEvent fromValue(final InputEvent i) {
            return new AggEvent().add(i);
        }
    }
}
