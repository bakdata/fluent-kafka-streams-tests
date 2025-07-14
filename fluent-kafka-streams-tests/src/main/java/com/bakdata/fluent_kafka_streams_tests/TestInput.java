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

import com.bakdata.kafka.Preconfigured;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;

/**
 * Represents the input topic for the tested app via {@link TestTopology}.
 *
 * @param <K> the key type of the input topic
 * @param <V> the value type of the input topic
 */
public class TestInput<K, V> {
    private final TopologyTestDriver testDriver;
    private final TestInputTopic<K, V> testInputTopic;
    private final String topic;
    private final SerdeConfig<K, V> serdeConfig;

    private Long timestamp;

    /**
     * <p>Constructor for the test input topic.</p>
     *
     * @param testDriver Kafka's {@link TopologyTestDriver} used in this test.
     * @param topic Name of input topic.
     * @param serdeConfig configuration for serdes.
     */
    protected TestInput(final TopologyTestDriver testDriver, final String topic, final SerdeConfig<K, V> serdeConfig) {
        this.testDriver = testDriver;
        this.topic = topic;
        this.serdeConfig = serdeConfig;

        this.testInputTopic = this.testDriver.createInputTopic(this.topic,
                this.serdeConfig.getKeySerializer(),
                this.serdeConfig.getValueSerializer()
        );
    }

    /**
     * Set new serde for this input.
     *
     * @param keySerde The serializer/deserializer to be used for the keys in the input.
     * @param valueSerde The serializer/deserializer to be used for the values in the input.
     * @return Copy of current {@code TestInput} with provided serdes
     */
    public <KR, VR> TestInput<KR, VR> withSerde(final Serde<KR> keySerde, final Serde<VR> valueSerde) {
        return this.with(this.serdeConfig.withSerde(keySerde, valueSerde));
    }

    /**
     * Set new serde for this input. Serdes are configured using properties of the test topology.
     *
     * @param keySerde The serializer/deserializer to be used for the keys in the input.
     * @param valueSerde The serializer/deserializer to be used for the values in the input.
     * @return Copy of current {@code TestInput} with provided serdes
     */
    public <KR, VR> TestInput<KR, VR> configureWithSerde(final Preconfigured<? extends Serde<KR>> keySerde,
            final Preconfigured<? extends Serde<VR>> valueSerde) {
        return this.with(this.serdeConfig.configureWithSerde(keySerde, valueSerde));
    }

    /**
     * Set new serde for this input. Serdes are configured using properties of the test topology.
     *
     * @param keySerde The serializer/deserializer to be used for the keys in the input.
     * @param valueSerde The serializer/deserializer to be used for the values in the input.
     * @return Copy of current {@code TestInput} with provided serdes
     */
    public <KR, VR> TestInput<KR, VR> configureWithSerde(final Serde<KR> keySerde, final Serde<VR> valueSerde) {
        return this.with(this.serdeConfig.configureWithSerde(keySerde, valueSerde));
    }

    /**
     * Set new key serde for this input.
     *
     * @param keySerde The serializer/deserializer to be used for the keys in the input.
     * @return Copy of current {@code TestInput} with provided key serde
     */
    public <KR> TestInput<KR, V> withKeySerde(final Serde<KR> keySerde) {
        return this.with(this.serdeConfig.withKeySerde(keySerde));
    }

    /**
     * Set new key serde for this input. Serde is configured using properties of the test topology.
     *
     * @param keySerde The serializer/deserializer to be used for the keys in the input.
     * @return Copy of current {@code TestInput} with provided key serde
     */
    public <KR> TestInput<KR, V> configureWithKeySerde(final Preconfigured<? extends Serde<KR>> keySerde) {
        return this.with(this.serdeConfig.configureWithKeySerde(keySerde));
    }

    /**
     * Set new key serde for this input. Serde is configured using properties of the test topology.
     *
     * @param keySerde The serializer/deserializer to be used for the keys in the input.
     * @return Copy of current {@code TestInput} with provided key serde
     */
    public <KR> TestInput<KR, V> configureWithKeySerde(final Serde<KR> keySerde) {
        return this.with(this.serdeConfig.configureWithKeySerde(keySerde));
    }

    /**
     * Set new value serde for this input.
     *
     * @param valueSerde The serializer/deserializer to be used for the values in the input.
     * @return Copy of current {@code TestInput} with provided value serde
     */
    public <VR> TestInput<K, VR> withValueSerde(final Serde<VR> valueSerde) {
        return this.with(this.serdeConfig.withValueSerde(valueSerde));
    }

    /**
     * Set new value serde for this input. Serde is configured using properties of the test topology.
     *
     * @param valueSerde The serializer/deserializer to be used for the values in the input.
     * @return Copy of current {@code TestInput} with provided value serde
     */
    public <VR> TestInput<K, VR> configureWithValueSerde(final Preconfigured<? extends Serde<VR>> valueSerde) {
        return this.with(this.serdeConfig.configureWithValueSerde(valueSerde));
    }

    /**
     * Set new value serde for this input. Serde is configured using properties of the test topology.
     *
     * @param valueSerde The serializer/deserializer to be used for the values in the input.
     * @return Copy of current {@code TestInput} with provided value serde
     */
    public <VR> TestInput<K, VR> configureWithValueSerde(final Serde<VR> valueSerde) {
        return this.with(this.serdeConfig.configureWithValueSerde(valueSerde));
    }

    /**
     * <p>Type-casts the key and value to the given types.</p>
     *
     * A type-cast is useful if you have general-purpose serde, such as Json or Avro, which is used for different types
     * in input and output. Thus, instead of unnecessarily overriding the serde, this method just casts the input.
     *
     * @param keyType the new key type.
     * @param valueType the new value type.
     * @return Copy of current {@code TestInput} with provided types
     */
    public <KR, VR> TestInput<KR, VR> withTypes(final Class<KR> keyType, final Class<VR> valueType) {
        return this.with(this.serdeConfig.withTypes(keyType, valueType));
    }

    /**
     * <p>Type-casts the key to the given type.</p>
     *
     * A type-cast is useful if you have general-purpose serde, such as Json or Avro, which is used for different types
     * in input and output. Thus, instead of unnecessarily overriding the serde, this method just casts the input.
     *
     * @param keyType the new key type.
     * @return Copy of current {@code TestInput} with provided key type
     */
    public <KR> TestInput<KR, V> withKeyType(final Class<KR> keyType) {
        return this.with(this.serdeConfig.withKeyType(keyType));
    }

    /**
     * <p>Type-casts the value to the given type.</p>
     *
     * A type-cast is useful if you have general-purpose serde, such as Json or Avro, which is used for different types
     * in input and output. Thus, instead of unnecessarily overriding the serde, this method just casts the input.
     *
     * @param valueType the new value type.
     * @return Copy of current {@code TestInput} with provided value type
     */
    public <VR> TestInput<K, VR> withValueType(final Class<VR> valueType) {
        return this.with(this.serdeConfig.withValueType(valueType));
    }

    private <KR, VR> TestInput<KR, VR> with(final SerdeConfig<KR, VR> newSerdeConfig) {
        return new TestInput<>(this.testDriver, this.topic, newSerdeConfig);
    }

    /**
     * Set the event time of the following record. Use like this: {@code myInput.at(60000).add(myValue)}.
     *
     * @param timestamp Event time in milliseconds.
     * @return This input, so it can be chained.
     */
    public TestInput<K, V> at(final long timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    /**
     * <p>Set the event time of the following record.</p>
     * <p>Use if you want to specify the time instead of simply using milliseconds.</p>
     * <p>Use like this: {@code myInput.at(1, TimeUnit.MINUTES).add(myValue)}.</p>
     *
     * @param timestamp Number of time units wanted, i.e., {@literal 1 --> 1 minute/second/hour, 10 --> 10
     * minutes/seconds/hours.}
     * @param unit Time unit for this timestamp (e.g., {@code TimeUnit.{MINUTES|SECONDS|HOURS}}).
     * @return This input, so it can be chained.
     */
    public TestInput<K, V> at(final long timestamp, final TimeUnit unit) {
        return this.at(unit.toMillis(timestamp));
    }

    /**
     * Add a value to the input topic. The key will default to null. If a timestamp was specified with {@link #at(long,
     * TimeUnit)} or {@link #at(long)}, that timestamp will be used here. Otherwise, the timestamp will default to 0.
     *
     * @param value Value to be inserted into topic.
     * @return This input, so it can be chained.
     */
    public TestInput<K, V> add(final V value) {
        return this.addInternal(null, value, this.timestamp, null);
    }

    /**
     * Add a key and value to the input topic. If a timestamp was specified with {@link #at(long, TimeUnit)} or {@link
     * #at(long)}, that timestamp will be used here. Otherwise, the timestamp will default to 0.
     *
     * @param key Key to be inserted into the topic.
     * @param value Value to be inserted into topic.
     * @return This input, so it can be chained.
     */
    public TestInput<K, V> add(final K key, final V value) {
        return this.addInternal(key, value, this.timestamp, null);
    }

    /**
     * Add a key and value to the input topic with a given timestamp. The recommended way is to use {@link #at(long,
     * TimeUnit)} or {@link #at(long)}, as they are easier to read and more expressive.
     *
     * @param key Key to be inserted into the topic.
     * @param value Value to be inserted into topic.
     * @param timestamp Event time at which the event should be inserted.
     * @return This input, so it can be chained.
     */
    public TestInput<K, V> add(final K key, final V value, final long timestamp) {
        return this.addInternal(key, value, timestamp, null);
    }

    /**
     * Add a key and value to the input topic with given headers. If a timestamp was specified with {@link #at(long,
     * TimeUnit)} or {@link #at(long)}, that timestamp will be used here. Otherwise, the timestamp will default to 0.
     *
     * @param key Key to be inserted into the topic.
     * @param value Value to be inserted into topic.
     * @param headers Record headers.
     * @return This input, so it can be chained.
     */
    public TestInput<K, V> add(final K key, final V value, final Headers headers) {
        return this.addInternal(key, value, this.timestamp, headers);
    }

    /**
     * Add a key and value to the input topic with a given timestamp and headers. The recommended way is to use {@link
     * #at(long, TimeUnit)} or {@link #at(long)}, as they are easier to read and more expressive.
     *
     * @param key Key to be inserted into the topic.
     * @param value Value to be inserted into topic.
     * @param timestamp Event time at which the event should be inserted.
     * @param headers Record headers.
     * @return This input, so it can be chained.
     */
    public TestInput<K, V> add(final K key, final V value, final long timestamp, final Headers headers) {
        return this.addInternal(key, value, timestamp, headers);
    }

    // ==================
    // Non-public methods
    // ==================
    private TestInput<K, V> addInternal(final K key, final V value, final Long timestamp, final Headers headers) {
        this.testInputTopic.pipeInput(new TestRecord<>(key, value, headers, timestamp == null ? 0 : timestamp));
        return this;
    }
}

