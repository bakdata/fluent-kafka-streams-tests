package com.bakdata.fluent_kafka_streams_tests;

import com.bakdata.schemaregistrymock.SchemaRegistryMock;
import com.google.common.collect.Iterables;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Singular;
import lombok.experimental.Delegate;
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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Optional.ofNullable;

public class TestTopology<DefaultK, DefaultV> implements BeforeEachCallback, AfterEachCallback {
    @NonNull
    private final Properties properties;

    private final SchemaRegistryMock schemaRegistry = new SchemaRegistryMock();

    @Delegate
    private TopologyTestDriver testDriver;

    @NonNull
    private Function<Properties, Topology> topologyFactory;

    @Getter(lazy = true)
    private final StreamsConfig streamsConfig = new StreamsConfig(properties);

    private final Set<String> inputTopics = new HashSet<>();
    private final Set<String> outputTopics = new HashSet<>();

    private Serde<DefaultK> defaultKeySerde;
    private Serde<DefaultV> defaultValueSerde;

    @Builder
    private TestTopology(Properties properties, @Singular Map<String, String> overrideProperties,
                         Function<Properties, Topology> topologyFactory, Serde<DefaultK> defaultKeySerde,
                         Serde<DefaultV> defaultValueSerde) {
        this.properties = new Properties();
        if (properties != null) {
            this.properties.putAll(properties);
        }
        this.properties.putAll(overrideProperties);
        this.topologyFactory = topologyFactory;

        this.defaultKeySerde = (defaultKeySerde != null) ? defaultKeySerde : getStreamsConfig().defaultKeySerde();
        this.defaultValueSerde = (defaultValueSerde != null) ? defaultValueSerde : getStreamsConfig().defaultValueSerde();
    }

    private Serde<DefaultK> getDefaultKeySerde() {
        return defaultKeySerde;
    }

    private Serde<DefaultV> getDefaultValueSerde() {
        return defaultValueSerde;
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
        final Topology topology = topologyFactory.apply(properties);
        testDriver = new TopologyTestDriver(topology, properties);

        this.inputTopics.clear();
        this.outputTopics.clear();
        for (TopologyDescription.Subtopology subtopology : topology.describe().subtopologies()) {
            for (TopologyDescription.Node node : subtopology.nodes()) {
                if(node instanceof TopologyDescription.Source) {
                    final String topics = ((TopologyDescription.Source) node).topics();
                    if(topics.startsWith("[")) {
                        inputTopics.addAll(Arrays.asList(topics.substring(1, topics.length() - 1).split(",")));
                    } else {
                        // should be pattern; should be handled differently
                        throw new UnsupportedOperationException();
                    }
                } else if(node instanceof TopologyDescription.Sink) {
                    String topic = ((TopologyDescription.Sink) node).topic();
                    outputTopics.add(topic);
                }
            }
        }
    }

    public Input<DefaultK, DefaultV> input() {
        return input(Iterables.getOnlyElement(inputTopics));
    }

    public Input<DefaultK, DefaultV> input(String topic) {
        if (!inputTopics.contains(topic)) {
            throw new NoSuchElementException(String.format("Input topic '%s' not found", topic));
        }
        return new Input<>(testDriver, topic, getDefaultKeySerde(), getDefaultValueSerde());
    }

    public TestOutput<DefaultK, DefaultV> streamOutput() {
        return streamOutput(Iterables.getOnlyElement(outputTopics));
    }

    public TestOutput<DefaultK, DefaultV> streamOutput(String topic) {
        if (!outputTopics.contains(topic)) {
            throw new NoSuchElementException(String.format("Output topic '%s' not found", topic));
        }
        return new StreamOutput<>(testDriver, topic, getDefaultKeySerde(), getDefaultValueSerde());
    }

    public TestOutput<DefaultK, DefaultV> tableOutput() {
        return streamOutput().asTable();
    }

    public TestOutput<DefaultK, DefaultV> tableOutput(String topic) {
        return streamOutput(topic).asTable();
    }

    public SchemaRegistryClient getSchemaRegistry() {
        return schemaRegistry.getSchemaRegistryClient();
    }

    public String getSchemaRegistryUrl() {
        return schemaRegistry.url();
    }

    public static class TestTopologyBuilder<DefaultK, DefaultV> {
        public TestTopologyBuilder<DefaultK, DefaultV> topologyFactory(Function<Properties, Topology> topologyFactory) {
            this.topologyFactory = topologyFactory;
            return this;
        }

        public TestTopologyBuilder<DefaultK, DefaultV> topologyFactory(Topology topology) {
            Properties dummyProperties = new Properties();
            this.topologyFactory = (Properties) -> topology;
            return this;
        }

        public TestTopologyBuilder<DefaultK, DefaultV> topologyFactory(Supplier<Topology> topologyFactory) {
            this.topologyFactory = (Properties) -> topologyFactory.get();
            return this;
        }
    }

    public static class Input<K, V> extends ConsumerRecordFactory<K, V> {
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
    public static class Expectation<K, V> {
        private final ProducerRecord<K, V> record;
        private final TestOutput<K, V> output;

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
