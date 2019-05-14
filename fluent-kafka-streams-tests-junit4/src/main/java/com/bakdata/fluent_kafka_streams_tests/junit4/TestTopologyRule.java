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

package com.bakdata.fluent_kafka_streams_tests.junit4;

import com.bakdata.fluent_kafka_streams_tests.TestTopology;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.Getter;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.Topology;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * <p>Represents the main interaction with Kafka for testing purposes. Handles all inputs and outputs of the
 * {@link Topology} under test. This should be registered as an extension in your JUnit tests, to ensure that certain
 * setup and teardown methods are called.</p> Usage:
 * <pre><code>
 * public class WordCountTest {
 *     private final WordCount app = new WordCount();
 *
 *     {@literal @Rule
 *     public final TestTopologyRule<Object, String> testTopology =
 *         new TestTopologyRule<>(this.app::getTopology, this.app.getKafkaProperties());}
 *
 *     {@literal @Test}
 *     public void shouldAggregateSameWordStream() {
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
 */
@Getter
public class TestTopologyRule<DefaultK, DefaultV> extends TestTopology<DefaultK, DefaultV>
        implements TestRule {
    public TestTopologyRule(
            final Function<? super Properties, ? extends Topology> topologyFactory, final Map<?, ?> properties) {
        super(topologyFactory, properties);
    }

    public TestTopologyRule(
            final Supplier<? extends Topology> topologyFactory, final Map<?, ?> properties) {
        super(topologyFactory, properties);
    }

    public TestTopologyRule(final Topology topology, final Map<?, ?> properties) {
        super(topology, properties);
    }

    protected TestTopologyRule(
            final Function<? super Properties, ? extends Topology> topologyFactory, final Map<?, ?> properties,
            final Serde<DefaultK> defaultKeySerde,
            final Serde<DefaultV> defaultValueSerde) {
        super(topologyFactory, properties, defaultKeySerde, defaultValueSerde);
    }

    @Override
    public Statement apply(final Statement base, final Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                TestTopologyRule.this.start();
                try {
                    base.evaluate();
                } finally {
                    TestTopologyRule.this.stop();
                }
            }
        };
    }

    @Override
    protected <K, V> TestTopology<K, V> with(final Function<? super Properties, ? extends Topology> topologyFactory,
            final Map<?, ?> properties, final Serde<K> defaultKeySerde, final Serde<V> defaultValueSerde) {
        return new TestTopologyRule<>(topologyFactory, properties, defaultKeySerde, defaultValueSerde);
    }

    @Override
    public <V> TestTopologyRule<DefaultK, V> withDefaultValueSerde(final Serde<V> defaultValueSerde) {
        return (TestTopologyRule<DefaultK, V>) super.withDefaultValueSerde(defaultValueSerde);
    }

    @Override
    public <K> TestTopologyRule<K, DefaultV> withDefaultKeySerde(final Serde<K> defaultKeySerde) {
        return (TestTopologyRule<K, DefaultV>) super.withDefaultKeySerde(defaultKeySerde);
    }
}
