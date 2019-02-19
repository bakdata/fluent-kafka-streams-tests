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

/**
 * Represents the main interaction with Kafka for testing purposes. Handles all inputs and outputs of the
 * {@link Topology} under test. This should be registered as an extension in your JUnit tests, to ensure that
 * certain setup and teardown methods are called.
 * Usage:
 * {@code
 *
 * @RegisterExtension
 * final TestTopology<Object, String> testTopology =
 *     new TestTopology<>(app::getTopology, app.getKafkaProperties());
 * }
 *
 * With {@code app} being any Kafka Streams application that you want to test.
 *
 */
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

    /**
     * Create a new {@link TestTopology} for your topology under test.
     *
     * @param topologyFactory Provides the topology under test. Ideally, this should always create a fresh topology to
     *                        ensure strict separation of each test run.
     * @param properties The properties of the Kafka Streams application under test.
     *                   Required entries: APPLICATION_ID_CONFIG, BOOTSTRAP_SERVERS_CONFIG,
     *                                     DEFAULT_KEY_SERDE_CLASS_CONFIG, DEFAULT_VALUE_SERDE_CLASS_CONFIG
     */
    public TestTopology(final Function<? super Properties, ? extends Topology> topologyFactory, final Map<?, ?> properties) {
        this.topologyFactory = topologyFactory;
        this.properties.putAll(properties);
        this.defaultKeySerde = null;
        this.defaultValueSerde = null;
    }

    /**
     * See above constructor for more details.
     * @param topologyFactory Provides the topology under test but does not require properties to build the topology.
     */
    public TestTopology(final Supplier<? extends Topology> topologyFactory, final Map<?, ?> properties) {
        this((Properties) -> topologyFactory.get(), properties);
    }

    /**
     * See above constructor for more details.
     * @param topology A fixed topology to be tested. This should only be used, if you are sure that the topology is not
     *                 affected by other test runs. Otherwise, side-effects could impact your tests.
     */
    public TestTopology(final Topology topology, final Map<?, ?> properties) {
        this((Properties) -> topology, properties);
    }

    /**
     * Get all properties that the application has set.
     */
    public StreamsConfig getStreamsConfig() {
        return new StreamsConfig(this.properties);
    }

    /**
     * Get the default serde of the key type in your application.
     */
    private Serde<DefaultK> getDefaultKeySerde() {
        return this.defaultKeySerde != null ? this.defaultKeySerde : this.getStreamsConfig().defaultKeySerde();
    }

    /**
     * Get the default serde of the value type in your application.
     */
    private Serde<DefaultV> getDefaultValueSerde() {
        return this.defaultValueSerde != null ? this.defaultValueSerde : this.getStreamsConfig().defaultValueSerde();
    }


    /**
     * Get the only input topic used by the topology under test.
     *
     * @throws IllegalStateException if more than one input topic is present.
     * @return {@link TestInput} of the input topic that you want to write to.
     */
    public TestInput<DefaultK, DefaultV> input() {
        if (this.inputTopics.size() != 1) {
            throw new IllegalStateException("#input() works with exactly 1 topic, if more are used," +
                    " please use #input(String) to select a topic");
        }
        return this.input(this.inputTopics.iterator().next());
    }

    /**
     * Get the input topic with the name `topic` used by the topology under test.
     *
     * @throws NoSuchElementException if there is no topic with that name.
     * @return {@link TestInput} of the input topic that you want to write to.
     */
    public TestInput<DefaultK, DefaultV> input(final String topic) {
        if (!this.inputTopics.contains(topic)) {
            throw new NoSuchElementException(String.format("Input topic '%s' not found", topic));
        }
        return new TestInput<>(this.testDriver, topic, this.getDefaultKeySerde(), this.getDefaultValueSerde());
    }

    /**
     * Get the only output topic used by the topology under test with
     * {@link org.apache.kafka.streams.kstream.KStream}-semantics.
     *
     * @throws IllegalStateException if more than one output topic is present.
     * @return {@link StreamOutput} of the output topic that you want to read from.
     */
    public TestOutput<DefaultK, DefaultV> streamOutput() {
        if (this.outputTopics.size() != 1) {
            throw new IllegalStateException("Please use #output(String) to select a topic");
        }
        return this.streamOutput(this.outputTopics.iterator().next());
    }

    /**
     * Get the output topic with the name `topic` used by the topology under test with
     * {@link org.apache.kafka.streams.kstream.KStream}-semantics.
     *
     * @throws NoSuchElementException if there is no topic with that name.
     * @return {@link StreamOutput} of the output topic that you want to read from.
     */
    public TestOutput<DefaultK, DefaultV> streamOutput(final String topic) {
        if (!this.outputTopics.contains(topic)) {
            throw new NoSuchElementException(String.format("Output topic '%s' not found", topic));
        }
        return new StreamOutput<>(this.testDriver, topic, this.getDefaultKeySerde(), this.getDefaultValueSerde());
    }

    /**
     * Get the only output topic used by the topology under test with
     * {@link org.apache.kafka.streams.kstream.KTable}-semantics.
     *
     * @throws IllegalStateException if more than one output topic is present.
     * @return {@link TableOutput} of the output topic that you want to read from.
     */
    public TestOutput<DefaultK, DefaultV> tableOutput() {
        return this.streamOutput().asTable();
    }

    /**
     * Get the output topic with the name `topic` used by the topology under test with
     * {@link org.apache.kafka.streams.kstream.KTable}-semantics.
     *
     * @throws NoSuchElementException if there is no topic with that name.
     * @return {@link TableOutput} of the output topic that you want to read from.
     */
    public TestOutput<DefaultK, DefaultV> tableOutput(final String topic) {
        return this.streamOutput(topic).asTable();
    }

    /**
     * TODO(aheise): anything to add?
     * Get the interface to the schema registry.
     */
    public SchemaRegistryClient getSchemaRegistry() {
        return this.schemaRegistry.getSchemaRegistryClient();
    }

    /**
     * TODO(aheise): anything to add?
     * Get the URL of the schema registry.
     */
    public String getSchemaRegistryUrl() {
        return this.schemaRegistry.getUrl();
    }

    // ==================
    // Non-public methods
    // ==================
    private static void addExternalTopics(final Collection<String> allTopics, final String topics) {
        if (topics.contains("KSTREAM-") || topics.contains("KTABLE-")) {
            // Internal node created by Kafka. Not relevant for testing.
            return;
        }

        // TODO: support wildcards
        if (topics.startsWith("[")) {
            // A list of topics in the form of [topic1,...,topicN]
            allTopics.addAll(Arrays.asList(topics.substring(1, topics.length() - 1).split(",")));
        } else {
            // Only one topic present without leading '['
            allTopics.add(topics);
        }
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
}
