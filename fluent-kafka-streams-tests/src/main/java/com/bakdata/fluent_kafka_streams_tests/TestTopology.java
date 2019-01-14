package com.bakdata.fluent_kafka_streams_tests;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import lombok.*;
import lombok.experimental.Delegate;
import lombok.experimental.Wither;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import static java.util.Optional.ofNullable;

public class TestTopology<DefaultK, DefaultV> implements BeforeEachCallback, AfterEachCallback {
    @NonNull
    private final Properties properties;

    private final SchemaRegistryMock schemaRegistry = new SchemaRegistryMock();

    @Delegate
    private TopologyTestDriver testDriver;

    @NonNull
    private Function<Properties, Topology> topologyCreator;

    @Getter(lazy = true)
    private final StreamsConfig streamsConfig = new StreamsConfig(properties);

    private final Map<String, Input<DefaultK, DefaultV>> inputs = new HashMap<>();
    private final Map<String, Output<DefaultK, DefaultV>> outputs = new HashMap<>();

    private Serde<DefaultK> defaultKeySerde;
    private Serde<DefaultV> defaultValueSerde;

    @Builder
    private TestTopology(Properties properties, @Singular Map<String, String> overrideProperties,
                         Function<Properties, Topology> topologyCreator, Serde<DefaultK> defaultKeySerde,
                         Serde<DefaultV> defaultValueSerde) {
        this.properties = new Properties();
        if(properties != null) {
            this.properties.putAll(properties);
        }
        this.properties.putAll(overrideProperties);
        this.topologyCreator = topologyCreator;
        this.defaultKeySerde = defaultKeySerde;
        this.defaultValueSerde = defaultValueSerde;

    }

    private Serde<DefaultK> getDefaultKeySerde() {
        return getStreamsConfig().defaultKeySerde();
    }

    private Serde<DefaultV> getDefaultValueSerde() {
        return getStreamsConfig().defaultValueSerde();
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        testDriver.close();
        schemaRegistry.afterEach(context);
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        schemaRegistry.beforeEach(context);
        properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistry.url());
        final Topology topology = topologyCreator.apply(properties);
        testDriver = new TopologyTestDriver(topology, properties);

        this.inputs.clear();
        this.outputs.clear();
        for (TopologyDescription.Subtopology subtopology : topology.describe().subtopologies()) {
            for (TopologyDescription.Node node : subtopology.nodes()) {
                if(node instanceof TopologyDescription.Source) {
                    final String topics = ((TopologyDescription.Source) node).topics();
                    if(topics.startsWith("[")) {
                        for (String topic : topics.substring(1, topics.length() - 1).split(",")) {
                            inputs.put(topic, new Input<>(null, topic, defaultKeySerde, defaultValueSerde));
                        }
                    } else {
                        // should be pattern; should be handled differently
                        throw new UnsupportedOperationException();
                    }
                } else if(node instanceof TopologyDescription.Sink) {
                    outputs.put(node.name(), new Output<>(null, ((TopologyDescription.Sink) node).topic(), defaultKeySerde, defaultValueSerde));
                }
            }
        }
    }

    public Input<DefaultK, DefaultV> input() {
        return input(Iterables.getOnlyElement(inputs.keySet()));
    }

    public Input<DefaultK, DefaultV> input(String topic) {
        return Objects.requireNonNull(inputs.get(topic))
                .withTestDriver(testDriver)
                .withDefaultSerde(this::getDefaultKeySerde, this::getDefaultValueSerde);
    }

    public Output<DefaultK, DefaultV> output() {
        return output(Iterables.getOnlyElement(outputs.keySet()));
    }

    public Output<DefaultK, DefaultV> output(String topic) {
        return Objects.requireNonNull(outputs.get(topic))
                .withTestDriver(testDriver)
                .withDefaultSerde(this::getDefaultKeySerde, this::getDefaultValueSerde);
    }

    public SchemaRegistryClient getSchemaRegistry() {
        return schemaRegistry.getSchemaRegistryClient();
    }

    public String getSchemaRegistryUrl() {
        return schemaRegistry.url();
    }

    public static class Input<K, V> extends ConsumerRecordFactory<K, V> {
        @Wither(AccessLevel.PRIVATE)
        private final TopologyTestDriver testDriver;
        private final String topic;
        private final Serde<K> keySerde;
        private final Serde<V> valueSerde;

        public Input(TopologyTestDriver testDriver, String topic, Serde<K> keySerde, Serde<V> valueSerde) {
            super(topic, keySerde == null ? new DummySerializer<>() : keySerde.serializer(),
                    valueSerde == null ? new DummySerializer<>() : valueSerde.serializer());
            this.testDriver = testDriver;
            this.topic = topic;
            this.keySerde = keySerde;
            this.valueSerde = valueSerde;
        }

        public <KR, VR> Input<KR, VR> withSerde(Serde<KR> keySerde, Serde<VR> valueSerde) {
            return new Input<>(testDriver, topic, keySerde, valueSerde);
        }

        public Input<K, V> withDefaultSerde(Supplier<Serde<K>> keySerdeSupplier, Supplier<Serde<V>> valueSerdeSupplier) {
            return withSerde(ofNullable(keySerde).orElseGet(keySerdeSupplier),
                    ofNullable(valueSerde).orElseGet(valueSerdeSupplier));
        }

        public Input<K, V> and() {
            return this;
        }

        public ConsumerRecord<byte[], byte[]> create(final String topicName,
                                                     final K key,
                                                     final V value,
                                                     final Headers headers,
                                                     final long timestampMs) {
            final ConsumerRecord<byte[], byte[]> record = super.create(topicName, key, value, headers, timestampMs);
            testDriver.pipeInput(record);
            return record;
        }

        private static class DummySerializer<V> implements Serializer<V> {
            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
            }

            @Override
            public byte[] serialize(String topic, V data) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void close() {
            }
        }
    }

    @RequiredArgsConstructor
    public static class Output<K, V> {
        @Wither(AccessLevel.PRIVATE)
        private final TopologyTestDriver testDriver;
        private final String topic;
        private final Serde<K> keySerde;
        private final Serde<V> valueSerde;

        public <KR, VR> Output<KR, VR> withSerde(Serde<KR> keySerde, Serde<VR> valueSerde) {
            return new Output<>(testDriver, topic, keySerde, valueSerde);
        }

        public ProducerRecord<K, V> readOneRecord() {
            return testDriver.readOutput(topic, keySerde.deserializer(), valueSerde.deserializer());
        }

        public Output<K, V> withDefaultSerde(Supplier<Serde<K>> keySerdeSupplier, Supplier<Serde<V>> valueSerdeSupplier) {
            return withSerde(ofNullable(keySerde).orElseGet(keySerdeSupplier),
                    ofNullable(valueSerde).orElseGet(valueSerdeSupplier));
        }

        public Expectation<K, V> expectNextRecord() {
            return new Expectation<>(readOneRecord(), this);
        }

        public Expectation<K, V> expectNoMoreRecord() {
            return expectNextRecord().toBeEmpty();
        }
    }

    @RequiredArgsConstructor
    public static class Expectation<K, V> {
        private final ProducerRecord<K, V> record;
        private final Output<K, V> output;

        public Expectation<K, V>  isPresent() {
            Assertions.assertNotNull(record, "No more records found");
            return and();
        }

        public Expectation<K, V>  hasKey(K key) {
            isPresent();
            Assertions.assertEquals(key, record.key(), "Record key does not match");
            return and();
        }

        public Expectation<K, V>  hasValue(V value) {
            isPresent();
            Assertions.assertEquals(value, record.value(), "Record value does not match");
            return and();
        }

        public Expectation<K, V> and() {
            return this;
        }

        public Expectation<K, V> expectNextRecord() {
            return output.expectNextRecord();
        }

        public Expectation<K, V> expectNoMoreRecord() {
            return output.expectNoMoreRecord();
        }

        public Expectation<K, V> toBeEmpty() {
            Assertions.assertNull(record);
            return and();
        }
    }
}
