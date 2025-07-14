/*
 * MIT License
 *
 * Copyright (c) 2025 bakdata
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

import com.bakdata.kafka.Configurator;
import com.bakdata.kafka.util.TopologyInformation;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import lombok.Getter;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
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
 * @param <DefaultK> Default type of keys
 * @param <DefaultV> Default type of values
 */
@Getter
public class TestTopology<DefaultK, DefaultV> implements AutoCloseable {
    private final Function<? super Map<String, Object>, ? extends Topology> topologyFactory;
    private final Map<String, Object> properties = new HashMap<>();
    private final Collection<String> inputTopics = new HashSet<>();
    private final Collection<Pattern> inputPatterns = new HashSet<>();
    private final Collection<String> outputTopics = new HashSet<>();
    private final Map<String, Object> userProperties;

    private final Serde<DefaultK> defaultKeySerde;
    private final Serde<DefaultV> defaultValueSerde;
    private TopologyTestDriver testDriver;
    private Path stateDirectory;
    private TopologyDescription topologyDescription;
    private TopologyInformation topologyInformation;

    /**
     * Used by wither methods.
     */
    protected TestTopology(final Function<? super Map<String, Object>, ? extends Topology> topologyFactory,
            final Map<String, Object> properties,
            final Serde<DefaultK> defaultKeySerde,
            final Serde<DefaultV> defaultValueSerde) {
        this.topologyFactory = topologyFactory;
        this.userProperties = properties;
        this.defaultKeySerde = defaultKeySerde;
        this.defaultValueSerde = defaultValueSerde;
    }

    /**
     * <p>Create a new {@code TestTopology} for your topology under test.</p>
     *
     * @param topologyFactory Provides the topology under test. Ideally, this should always create a fresh topology to
     * ensure strict separation of each test run.
     * @param properties The properties of the Kafka Streams application under test. Required entries:
     * APPLICATION_ID_CONFIG, BOOTSTRAP_SERVERS_CONFIG
     */
    public TestTopology(final Function<? super Map<String, Object>, ? extends Topology> topologyFactory,
            final Map<String, Object> properties) {
        this(topologyFactory, properties, null, null);
    }

    /**
     * <p>Create a new {@code TestTopology} for your topology under test.</p>
     *
     * @param topologyFactory Provides the topology under test. Ideally, this should always create a fresh topology to
     * ensure strict separation of each test run.
     * @param properties The properties of the Kafka Streams application under test. Required entries:
     * APPLICATION_ID_CONFIG, BOOTSTRAP_SERVERS_CONFIG
     */
    public TestTopology(final Supplier<? extends Topology> topologyFactory, final Map<String, Object> properties) {
        this(props -> topologyFactory.get(), properties);
    }

    /**
     * <p>Create a new {@code TestTopology} for your topology under test.</p>
     *
     * @param topology A fixed topology to be tested. This should only be used, if you are sure that the topology is not
     * affected by other test runs. Otherwise, side effects could impact your tests.
     * @param properties The properties of the Kafka Streams application under test. Required entries:
     * APPLICATION_ID_CONFIG, BOOTSTRAP_SERVERS_CONFIG
     */
    public TestTopology(final Topology topology, final Map<String, Object> properties) {
        this(props -> topology, properties);
    }

    /**
     * Get all properties that the application has set.
     */
    public StreamsConfig getStreamsConfig() {
        return new StreamsConfig(this.properties);
    }

    /**
     * Overrides the default value serde
     *
     * @param defaultValueSerde Default value serde to use
     * @param <V> Default type of values
     * @return Copy of current {@code TestTopology} with provided value serde
     */
    public <V> TestTopology<DefaultK, V> withDefaultValueSerde(final Serde<V> defaultValueSerde) {
        return this.withDefaultSerde(this.defaultKeySerde, defaultValueSerde);
    }

    /**
     * Overrides the default key serde
     *
     * @param defaultKeySerde Default key serde to use
     * @param <K> Default type of key
     * @return Copy of current {@code TestTopology} with provided key serde
     */
    public <K> TestTopology<K, DefaultV> withDefaultKeySerde(final Serde<K> defaultKeySerde) {
        return this.withDefaultSerde(defaultKeySerde, this.defaultValueSerde);
    }

    /**
     * Overrides the default value serde
     *
     * @param defaultKeySerde Default key serde to use
     * @param defaultValueSerde Default value serde to use
     * @param <K> Default type of key
     * @param <V> Default type of values
     * @return Copy of current {@code TestTopology} with provided serdes
     */
    public <K, V> TestTopology<K, V> withDefaultSerde(final Serde<K> defaultKeySerde,
            final Serde<V> defaultValueSerde) {
        return this.with(this.topologyFactory, this.userProperties, defaultKeySerde, defaultValueSerde);
    }

    /**
     * Start the {@code TestTopology} and create all required resources.
     * <p>
     * This method creates the state directory and creates a {@link TopologyTestDriver}.
     */
    public void start() {
        this.properties.putAll(this.userProperties);
        try {
            this.stateDirectory = Files.createTempDirectory("fluent-kafka-streams");
        } catch (final IOException e) {
            throw new UncheckedIOException("Cannot create temporary state directory", e);
        }
        this.properties.put(StreamsConfig.STATE_DIR_CONFIG, this.stateDirectory.toAbsolutePath().toString());
        final Topology topology = this.topologyFactory.apply(this.properties);
        this.topologyDescription = topology.describe();
        this.testDriver = new TopologyTestDriver(topology, this.createProperties());

        this.inputTopics.clear();
        this.inputPatterns.clear();
        this.outputTopics.clear();

        this.topologyInformation = new TopologyInformation(this.topologyDescription,
                this.getStreamsConfig().getString(StreamsConfig.APPLICATION_ID_CONFIG));
        this.outputTopics.addAll(this.topologyInformation.getExternalSinkTopics());
        this.inputTopics.addAll(this.topologyInformation.getExternalSourceTopics());
        this.inputPatterns.addAll(this.topologyInformation.getExternalSourcePatterns());
    }

    @Override
    public void close() {
        this.stop();
    }

    /**
     * Get the default serde of the key type in your application.
     */
    private Serde<DefaultK> getDefaultKeySerde() {
        return this.defaultKeySerde != null ? this.defaultKeySerde
                : (Serde<DefaultK>) this.getStreamsConfig().defaultKeySerde();
    }

    /**
     * Get the default serde of the value type in your application.
     */
    private Serde<DefaultV> getDefaultValueSerde() {
        return this.defaultValueSerde != null ? this.defaultValueSerde
                : (Serde<DefaultV>) this.getStreamsConfig().defaultValueSerde();
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
     * @param topic name of topic to write to
     * @return {@link TestInput} of the input topic that you want to write to.
     * @throws NoSuchElementException if there is no topic with that name.
     */
    public TestInput<DefaultK, DefaultV> input(final String topic) {
        if (!this.inputTopics.contains(topic) && this.inputPatterns.stream()
                .noneMatch(p -> p.matcher(topic).matches())) {
            throw new NoSuchElementException(String.format("Input topic '%s' not found", topic));
        }
        return new TestInput<>(this.testDriver, topic, this.getDefaultKeySerde(), this.getDefaultValueSerde(),
                this.getDefaultKeySerde(), this.getDefaultValueSerde(),
                this.createConfigurator());
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
     * @param topic name of topic to read from
     * @return {@link StreamOutput} of the output topic that you want to read from.
     * @throws NoSuchElementException if there is no topic with that name.
     */
    public TestOutput<DefaultK, DefaultV> streamOutput(final String topic) {
        if (!this.outputTopics.contains(topic)) {
            throw new NoSuchElementException(String.format("Output topic '%s' not found", topic));
        }
        return new StreamOutput<>(this.testDriver, topic, this.getDefaultKeySerde(), this.getDefaultValueSerde(),
                this.getDefaultKeySerde(), this.getDefaultValueSerde(),
                this.createConfigurator());
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
     * @param topic name of topic to read from
     * @return {@link TableOutput} of the output topic that you want to read from.
     * @throws NoSuchElementException if there is no topic with that name.
     */
    public TestOutput<DefaultK, DefaultV> tableOutput(final String topic) {
        return this.streamOutput(topic).asTable();
    }

    /**
     * Stop the {@code TestTopology} and cleaning up all resources.
     * <p>
     * This method closes the {@link TopologyTestDriver} and removes the state directory.
     */
    public void stop() {
        if (this.testDriver != null) {
            this.testDriver.close();
        }
        try (final Stream<Path> stateFiles = Files.walk(this.stateDirectory)) {
            stateFiles.sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        } catch (final IOException e) {
            throw new UncheckedIOException("Cannot delete state directory", e);
        }
    }

    /**
     * Create {@code Configurator} to configure {@link Serde} using {@link #properties}.
     *
     * @return {@code Configurator}
     */
    public Configurator createConfigurator() {
        return new Configurator(this.properties);
    }

    protected <K, V> TestTopology<K, V> with(
            final Function<? super Map<String, Object>, ? extends Topology> topologyFactory,
            final Map<String, Object> userProperties, final Serde<K> defaultKeySerde,
            final Serde<V> defaultValueSerde) {
        return new TestTopology<>(topologyFactory, userProperties, defaultKeySerde, defaultValueSerde);
    }

    private Properties createProperties() {
        final Properties props = new Properties();
        props.putAll(this.properties);
        return props;
    }
}
