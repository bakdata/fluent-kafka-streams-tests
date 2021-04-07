/*
 * MIT License
 *
 * Copyright (c) 2019 bakdata GmbH
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.bakdata.fluent_kafka_streams_tests;

import com.bakdata.schemaregistrymock.SchemaRegistryMock;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;
import lombok.Getter;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.TopologyDescription.GlobalStore;
import org.apache.kafka.streams.TopologyDescription.Sink;
import org.apache.kafka.streams.TopologyDescription.Source;
import org.apache.kafka.streams.TopologyTestDriver;

/**
 * <p>Represents the main interaction with Kafka for testing purposes. Handles all inputs and outputs of the
 * {@link Topology} under test. This should be registered as an extension in your JUnit tests, to ensure that certain
 * setup and teardown methods are called.</p> Usage:
 * <pre><code>
 * class WordCountTest {
 *     private final WordCount app = new WordCount();
 *
 *     {@literal
 *     private final TestTopology<Object, String> testTopology =
 *         new TestTopology<>(this.app::getTopology, this.app.getKafkaProperties());}
 *
 *     {@literal @BeforeEach}
 *     void setup() {
 *         this.testTopology.start();
 *     }
 *
 *     {@literal @AfterEach}
 *     void teardown() {
 *         this.testTopology.stop();
 *     }
 *
 *     {@literal @Test}
 *     void shouldAggregateSameWordStream() {
 *         this.testTopology.input()
 *             .add("cat")
 *             .add("dog")
 *             .add("cat");
 *
 *         this.testTopology.streamOutput().withSerde(Serdes.String(), Serdes.Long())
 *             .expectNextRecord().hasKey("cat").hasValue(1L)
 *             .expectNextRecord().hasKey("dog").hasValue(1L)
 *             .expectNextRecord().hasKey("cat").hasValue(2L)
 *             .expectNoMoreRecord();
 *     }
 * }
 * </code></pre>
 * <p>With {@code app} being any Kafka Streams application that you want to test.</p>
 *
 * <p>In case the topology uses a {@link org.apache.kafka.streams.processor.TopicNameExtractor} to select output topics
 * dynamically, you must manually register these topics. You can add them to the set of output topics returned by
 * #getOutputTopics().</p>
 */
@Getter
public class TestTopology<DefaultK, DefaultV> {
    private SchemaRegistryMock schemaRegistry = new SchemaRegistryMock();
    private final Function<? super Properties, ? extends Topology> topologyFactory;
    private final Properties properties = new Properties();
    private final Collection<String> inputTopics = new HashSet<>();
    private final Collection<String> outputTopics = new HashSet<>();

    private final Serde<DefaultK> defaultKeySerde;
    private final Serde<DefaultV> defaultValueSerde;
    private TopologyTestDriver testDriver;
    private Path stateDirectory;

    /**
     * Used by wither methods.
     */
    protected TestTopology(
            final Function<? super Properties, ? extends Topology> topologyFactory, final Map<?, ?> properties,
            final Serde<DefaultK> defaultKeySerde, final Serde<DefaultV> defaultValueSerde) {
        this.topologyFactory = topologyFactory;
        this.properties.putAll(properties);
        this.defaultKeySerde = defaultKeySerde;
        this.defaultValueSerde = defaultValueSerde;
    }

    /**
     * <p>Create a new {@link TestTopology} for your topology under test.</p>
     *
     * @param topologyFactory Provides the topology under test. Ideally, this should always create a fresh topology to
     * ensure strict separation of each test run.
     * @param properties The properties of the Kafka Streams application under test. Required entries:
     * APPLICATION_ID_CONFIG, BOOTSTRAP_SERVERS_CONFIG
     */
    public TestTopology(final Function<? super Properties, ? extends Topology> topologyFactory,
            final Map<?, ?> properties) {
        this.topologyFactory = topologyFactory;
        this.properties.putAll(properties);
        this.defaultKeySerde = null;
        this.defaultValueSerde = null;
    }

    /**
     * <p>Create a new {@link TestTopology} for your topology under test.</p>
     *
     * @param topologyFactory Provides the topology under test. Ideally, this should always create a fresh topology to
     * ensure strict separation of each test run.
     * @param properties The properties of the Kafka Streams application under test. Required entries:
     * APPLICATION_ID_CONFIG, BOOTSTRAP_SERVERS_CONFIG
     */
    public TestTopology(final Supplier<? extends Topology> topologyFactory, final Map<?, ?> properties) {
        this(props -> topologyFactory.get(), properties);
    }

    /**
     * <p>Create a new {@link TestTopology} for your topology under test.</p>
     *
     * @param topology A fixed topology to be tested. This should only be used, if you are sure that the topology is not
     * affected by other test runs. Otherwise, side-effects could impact your tests.
     * @param properties The properties of the Kafka Streams application under test. Required entries:
     * APPLICATION_ID_CONFIG, BOOTSTRAP_SERVERS_CONFIG
     */
    public TestTopology(final Topology topology, final Map<?, ?> properties) {
        this(props -> topology, properties);
    }

    /**
     * <p>Create a new {@link TestTopology} for your topology under test.</p>
     *
     * @param topology A fixed topology to be tested. This should only be used, if you are sure that the topology is not
     * affected by other test runs. Otherwise, side-effects could impact your tests.
     * @param properties The properties of the Kafka Streams application under test. Required entries:
     * APPLICATION_ID_CONFIG, BOOTSTRAP_SERVERS_CONFIG
     * @param schemaRegistry external mocked Schema Registry to use under the test.
     */
    public TestTopology(
            final Topology topology,
            final Map<?, ?> properties,
            final SchemaRegistryMock schemaRegistry) {
        this(props -> topology, properties);
        this.schemaRegistry = schemaRegistry;
    }

    private static void addExternalTopics(final Collection<String> allTopics, final String topic) {
        if (topic.contains("KSTREAM-") || topic.contains("KTABLE-") || topic.contains("-repartition")) {
            // Internal node created by Kafka. Not relevant for testing.
            return;
        }

        allTopics.add(topic);
    }

    /**
     * Get all properties that the application has set.
     */
    public StreamsConfig getStreamsConfig() {
        return new StreamsConfig(this.properties);
    }

    public <V> TestTopology<DefaultK, V> withDefaultValueSerde(final Serde<V> defaultValueSerde) {
        return this.withDefaultSerde(this.defaultKeySerde, defaultValueSerde);
    }

    public <K> TestTopology<K, DefaultV> withDefaultKeySerde(final Serde<K> defaultKeySerde) {
        return this.withDefaultSerde(defaultKeySerde, this.defaultValueSerde);
    }

    public <K, V> TestTopology<K, V> withDefaultSerde(final Serde<K> defaultKeySerde,
            final Serde<V> defaultValueSerde) {
        return this.with(this.topologyFactory, this.properties, defaultKeySerde, defaultValueSerde);
    }

    protected <K, V> TestTopology<K, V> with(final Function<? super Properties, ? extends Topology> topologyFactory,
            final Map<?, ?> properties, final Serde<K> defaultKeySerde, final Serde<V> defaultValueSerde) {
        return new TestTopology<>(topologyFactory, properties, defaultKeySerde, defaultValueSerde);
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
     * @return {@link TestInput} of the input topic that you want to write to.
     * @throws IllegalStateException if more than one input topic is present.
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
     * @return {@link TestInput} of the input topic that you want to write to.
     * @throws NoSuchElementException if there is no topic with that name.
     */
    public TestInput<DefaultK, DefaultV> input(final String topic) {
        if (!this.inputTopics.contains(topic)) {
            throw new NoSuchElementException(String.format("Input topic '%s' not found", topic));
        }
        return new TestInput<>(this.testDriver, topic, this.getDefaultKeySerde(), this.getDefaultValueSerde());
    }

    /**
     * <p>Get the only output topic used by the topology under test with
     * {@link org.apache.kafka.streams.kstream.KStream}-semantics.</p>
     *
     * <p>Note: The StreamOutput is a one-time iterable. Cache it if you need to iterate several times.</p>
     *
     * @return {@link StreamOutput} of the output topic that you want to read from.
     * @throws IllegalStateException if more than one output topic is present.
     */
    public TestOutput<DefaultK, DefaultV> streamOutput() {
        if (this.outputTopics.size() != 1) {
            throw new IllegalStateException("Please use #output(String) to select a topic");
        }
        return this.streamOutput(this.outputTopics.iterator().next());
    }

    /**
     * <p>Get the output topic with the name `topic` used by the topology under test with
     * {@link org.apache.kafka.streams.kstream.KStream}-semantics.</p>
     *
     * <p>Note: The StreamOutput is a one-time iterable. Cache it if you need to iterate several times.</p>
     *
     * @return {@link StreamOutput} of the output topic that you want to read from.
     * @throws NoSuchElementException if there is no topic with that name.
     */
    public TestOutput<DefaultK, DefaultV> streamOutput(final String topic) {
        if (!this.outputTopics.contains(topic)) {
            throw new NoSuchElementException(String.format("Output topic '%s' not found", topic));
        }
        return new StreamOutput<>(this.testDriver, topic, this.getDefaultKeySerde(), this.getDefaultValueSerde());
    }

    /**
     * <p>Get the only output topic used by the topology under test with
     * {@link org.apache.kafka.streams.kstream.KTable}-semantics.</p>
     *
     * @return {@link TableOutput} of the output topic that you want to read from.
     * @throws IllegalStateException if more than one output topic is present.
     */
    public TestOutput<DefaultK, DefaultV> tableOutput() {
        return this.streamOutput().asTable();
    }

    /**
     * <p>Get the output topic with the name `topic` used by the topology under test with
     * {@link org.apache.kafka.streams.kstream.KTable}-semantics.</p>
     *
     * @return {@link TableOutput} of the output topic that you want to read from.
     * @throws NoSuchElementException if there is no topic with that name.
     */
    public TestOutput<DefaultK, DefaultV> tableOutput(final String topic) {
        return this.streamOutput(topic).asTable();
    }

    /**
     * Get the client to the schema registry for setup or verifications.
     */
    public SchemaRegistryClient getSchemaRegistry() {
        return this.schemaRegistry.getSchemaRegistryClient();
    }

    /**
     * Get the URL of the schema registry in the format that is expected in Kafka Streams configurations.
     */
    public String getSchemaRegistryUrl() {
        return this.schemaRegistry.getUrl();
    }

    public void stop() {
        if (this.testDriver != null) {
            this.testDriver.close();
        }
        this.schemaRegistry.stop();
        try (final Stream<Path> stateFiles = Files.walk(this.stateDirectory)) {
            stateFiles.sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        } catch (final IOException e) {
            throw new UncheckedIOException("Cannot delete state directory", e);
        }
    }

    public void start() {
        if(!this.schemaRegistry.isRunning()) {
            this.schemaRegistry.start();
        }
        this.properties
                .setProperty(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, this.getSchemaRegistryUrl());
        try {
            this.stateDirectory = Files.createTempDirectory("fluent-kafka-streams");
        } catch (final IOException e) {
            throw new UncheckedIOException("Cannot create temporary state directory", e);
        }
        this.properties.setProperty(StreamsConfig.STATE_DIR_CONFIG, this.stateDirectory.toAbsolutePath().toString());
        final Topology topology = this.topologyFactory.apply(this.properties);
        this.testDriver = new TopologyTestDriver(topology, this.properties);

        this.inputTopics.clear();
        this.outputTopics.clear();

        for (final TopologyDescription.Subtopology subtopology : topology.describe().subtopologies()) {
            for (final TopologyDescription.Node node : subtopology.nodes()) {
                if (node instanceof TopologyDescription.Source) {
                    for (final String topic : ((Source) node).topicSet()) {
                        addExternalTopics(this.inputTopics, topic);
                    }
                } else if (node instanceof TopologyDescription.Sink) {
                    // might be null if a TopicNameExtractor is used
                    final String topic = ((Sink) node).topic();
                    if (topic != null) {
                        addExternalTopics(this.outputTopics, topic);
                    }
                }
            }
        }

        for (final GlobalStore store : topology.describe().globalStores()) {
            store.source().topicSet().forEach(name -> addExternalTopics(this.inputTopics, name));
        }
    }
}
