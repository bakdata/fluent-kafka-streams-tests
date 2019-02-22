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

import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;

/**
 * Represents the input topic for the tested app via {@link TestTopology}.
 *
 * @param <K> the key type of the input topic
 * @param <V> the value type of the input topic
 */
public class TestInput<K, V> {
    private final TopologyTestDriver testDriver;
    private final String topic;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;
    private final ConsumerRecordFactory<K, V> consumerFactory;
    private Long timestamp;

    /**
     * <p>Constructor for the test input topic.</p>
     *
     * @param testDriver Kafka's {@link TopologyTestDriver} used in this test.
     * @param topic Name of input topic.
     * @param keySerde Serde for key type in topic.
     * @param valueSerde Serde for value type in topic.
     */
    protected TestInput(final TopologyTestDriver testDriver, final String topic, final Serde<K> keySerde,
            final Serde<V> valueSerde) {
        this.testDriver = testDriver;
        this.topic = topic;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;

        this.consumerFactory = new ConsumerRecordFactory<>(topic,
                keySerde == null ? new UnspecifiedSerializer<K>() : keySerde.serializer(),
                valueSerde == null ? new UnspecifiedSerializer<V>() : valueSerde.serializer()) {
            @Override
            public ConsumerRecord<byte[], byte[]> create(final String topicName, final K key, final V value,
                    final Headers headers, final long timestampMs) {
                final ConsumerRecord<byte[], byte[]> record = super.create(topicName, key, value, headers, timestampMs);
                testDriver.pipeInput(record);
                return record;
            }
        };
    }

    /**
     * Set new serde for this input.
     *
     * @param keySerde   The serializer/deserializer to be used for the keys in the input.
     * @param valueSerde The serializer/deserializer to be used for the values in the input.
     */
    public <KR, VR> TestInput<KR, VR> withSerde(final Serde<KR> keySerde, final Serde<VR> valueSerde) {
        return new TestInput<>(this.testDriver, this.topic, keySerde, valueSerde);
    }

    /**
     * Set new key serde for this input.
     */
    public <KR> TestInput<KR, V> withKeySerde(final Serde<KR> keySerde) {
        return this.withSerde(keySerde, this.valueSerde);
    }

    /**
     * Set new value serde for this input.
     */
    public <VR> TestInput<K, VR> withValueSerde(final Serde<VR> valueSerde) {
        return this.withSerde(this.keySerde, valueSerde);
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
     * Add a value to the input topic. The key will default to null. If a timestamp was specified with
     * {@link #at(long, TimeUnit)} or {@link #at(long)}, that timestamp will be used here. Otherwise, the timestamp will
     * default to 0.
     *
     * @param value Value to be inserted into topic.
     * @return This input, so it can be chained.
     */
    public TestInput<K, V> add(final V value) {
        return this.addInternal(null, value, this.timestamp);
    }

    /**
     * Add a key and value to the input topic. If a timestamp was specified with {@link #at(long, TimeUnit)} or
     * {@link #at(long)}, that timestamp will be used here. Otherwise, the timestamp will default to 0.
     *
     * @param key Key to be inserted into the topic.
     * @param value Value to be inserted into topic.
     * @return This input, so it can be chained.
     */
    public TestInput<K, V> add(final K key, final V value) {
        return this.addInternal(key, value, this.timestamp);
    }

    /**
     * Add a key and value to the input topic with a given timestamp. The recommended way is to use
     * {@link #at(long, TimeUnit)} or {@link #at(long)}, as they are easier to read and more expressive.
     *
     * @param key Key to be inserted into the topic.
     * @param value Value to be inserted into topic.
     * @param timestamp Event time at which the event should be inserted.
     * @return This input, so it can be chained.
     */
    public TestInput<K, V> add(final K key, final V value, final long timestamp) {
        return this.addInternal(key, value, timestamp);
    }

    // ==================
    // Non-public methods
    // ==================
    private TestInput<K, V> addInternal(final K key, final V value, final Long timestamp) {
        this.consumerFactory.create(key, value, timestamp == null ? 0 : timestamp);
        return this;
    }

    private static class UnspecifiedSerializer<V> implements Serializer<V> {
        @Override
        public void configure(final Map<String, ?> configs, final boolean isKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte[] serialize(final String topic, final V data) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
            throw new UnsupportedOperationException();
        }
    }
}

