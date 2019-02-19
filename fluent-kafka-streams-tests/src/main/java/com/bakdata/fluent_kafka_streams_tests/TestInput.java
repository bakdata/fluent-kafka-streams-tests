package com.bakdata.fluent_kafka_streams_tests;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static java.util.Optional.ofNullable;

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
    private Long timestamp = null;

    /**
     * Constructor for the test input topic.
     * You should not need to call this. The recommended way is to use {@link TestTopology#input(String)} or
     * {@link TestTopology#input()}.
     *
     * @param testDriver Kafka's {@link TopologyTestDriver} used in this test.
     * @param topic Name of input topic.
     * @param keySerde Serde for key type in topic.
     * @param valueSerde Serde for value type in topic.
     */
    public TestInput(final TopologyTestDriver testDriver, final String topic, final Serde<K> keySerde, final Serde<V> valueSerde) {
        this.testDriver = testDriver;
        this.topic = topic;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;

        this.consumerFactory = new ConsumerRecordFactory<>(topic,
                keySerde == null ? new UnspecifiedSerializer<K>() : keySerde.serializer(),
                valueSerde == null ? new UnspecifiedSerializer<V>() : valueSerde.serializer()) {
            @Override
            public ConsumerRecord<byte[], byte[]> create(final String topicName,
                                                         final K key,
                                                         final V value,
                                                         final Headers headers,
                                                         final long timestampMs) {
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
     * TODO: do we even need this anymore?
     */
    public TestInput<K, V> withDefaultSerde(final Supplier<? extends Serde<K>> keySerdeSupplier, final Supplier<? extends Serde<V>> valueSerdeSupplier) {
        return this.withSerde(ofNullable(this.keySerde).orElseGet(keySerdeSupplier),
                ofNullable(this.valueSerde).orElseGet(valueSerdeSupplier));
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
     * Set the event time of the following record.
     * Use if you want to specify the time instead of simply using milliseconds.
     * Use like this: {@code myInput.at(1, TimeUnit.MINUTES).add(myValue)}.
     *
     * @param timestamp Number of time units wanted, i.e., 1 --> 1 minute/second/hour, 10 --> 10 minutes/seconds/hours.
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

