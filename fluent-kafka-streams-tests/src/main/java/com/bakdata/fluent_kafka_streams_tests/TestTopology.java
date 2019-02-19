package com.bakdata.fluent_kafka_streams_tests;

import com.bakdata.schemaregistrymock.SchemaRegistryMock;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.experimental.Wither;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.function.Function;
import java.util.function.Supplier;

@Getter
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class TestTopology<DefaultK, DefaultV> implements BeforeEachCallback, AfterEachCallback {
    private final SchemaRegistryMock schemaRegistry = new SchemaRegistryMock();
    private final Function<? super Properties, ? extends Topology> topologyFactory;
    private final Collection<String> inputTopics = new HashSet<>();
    private final Collection<String> outputTopics = new HashSet<>();
    @Wither
    private final Serde<DefaultK> defaultKeySerde;
    @Wither
    private final Serde<DefaultV> defaultValueSerde;
    private final Properties properties = new Properties();
    private TopologyTestDriver testDriver;

    public TestTopology(final Function<? super Properties, ? extends Topology> topologyFactory, final Map<?, ?> properties) {
        this.topologyFactory = topologyFactory;
        this.properties.putAll(properties);
        this.defaultKeySerde = null;
        this.defaultValueSerde = null;
    }

    public TestTopology(final Topology topology, final Map<?, ?> properties) {
        this((Properties) -> topology, properties);
    }

    public TestTopology(final Supplier<? extends Topology> topologyFactory, final Map<?, ?> properties) {
        this((Properties) -> topologyFactory.get(), properties);
    }

    private static void addExternalTopics(final Collection<String> allTopics, final String topics) {
        if (topics.contains("KSTREAM-") || topics.contains("KTABLE-")) {
            // Internal node created by Kafka. Not relevant for testing.
            return;
        }

        // TODO: support wildcards
        if (topics.startsWith("[")) {
            // a list of topics in the form of [topic1,...,topicN]
            allTopics.addAll(Arrays.asList(topics.substring(1, topics.length() - 1).split(",")));
        } else {
            // Only one topic present without leading '['
            allTopics.add(topics);
        }
    }

    public StreamsConfig getStreamsConfig() {
        return new StreamsConfig(this.properties);
    }

    private Serde<DefaultK> getDefaultKeySerde() {
        return this.defaultKeySerde != null ? this.defaultKeySerde : this.getStreamsConfig().defaultKeySerde();
    }

    private Serde<DefaultV> getDefaultValueSerde() {
        return this.defaultValueSerde != null ? this.defaultValueSerde : this.getStreamsConfig().defaultValueSerde();
    }

    @Override
    public void afterEach(final ExtensionContext context) {
        this.testDriver.close();
        this.schemaRegistry.afterEach(context);
    }

    @Override
    public void beforeEach(final ExtensionContext context) {
        this.schemaRegistry.beforeEach(context);
        this.properties.setProperty(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, this.getSchemaRegistryUrl());
        final Topology topology = this.topologyFactory.apply(this.properties);
        this.testDriver = new TopologyTestDriver(topology, this.properties);

        this.inputTopics.clear();
        this.outputTopics.clear();

        for (final TopologyDescription.Subtopology subtopology : topology.describe().subtopologies()) {
            for (final TopologyDescription.Node node : subtopology.nodes()) {
                if (node instanceof TopologyDescription.Source) {
                    addExternalTopics(this.inputTopics, ((TopologyDescription.Source) node).topics());
                } else if (node instanceof TopologyDescription.Sink) {
                    addExternalTopics(this.outputTopics, ((TopologyDescription.Sink) node).topic());
                }
            }
        }
    }

    public TestInput<DefaultK, DefaultV> input() {
        if (this.inputTopics.size() != 1) {
            throw new IllegalStateException("Please use #input(String) to select a topic");
        }
        return this.input(this.inputTopics.iterator().next());
    }

    public TestInput<DefaultK, DefaultV> input(final String topic) {
        if (!this.inputTopics.contains(topic)) {
            throw new NoSuchElementException(String.format("Input topic '%s' not found", topic));
        }
        return new TestInput<>(this.testDriver, topic, this.getDefaultKeySerde(), this.getDefaultValueSerde());
    }

    public TestOutput<DefaultK, DefaultV> streamOutput() {
        if (this.outputTopics.size() != 1) {
            throw new IllegalStateException("Please use #output(String) to select a topic");
        }
        return this.streamOutput(this.outputTopics.iterator().next());
    }

    public TestOutput<DefaultK, DefaultV> streamOutput(final String topic) {
        if (!this.outputTopics.contains(topic)) {
            throw new NoSuchElementException(String.format("Output topic '%s' not found", topic));
        }
        return new StreamOutput<>(this.testDriver, topic, this.getDefaultKeySerde(), this.getDefaultValueSerde());
    }

    public TestOutput<DefaultK, DefaultV> tableOutput() {
        return this.streamOutput().asTable();
    }

    public TestOutput<DefaultK, DefaultV> tableOutput(final String topic) {
        return this.streamOutput(topic).asTable();
    }

    public SchemaRegistryClient getSchemaRegistry() {
        return this.schemaRegistry.getSchemaRegistryClient();
    }

    public String getSchemaRegistryUrl() {
        return this.schemaRegistry.getUrl();
    }
}
