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

public class TestInput<K, V> {
    private final TopologyTestDriver testDriver;
    private final String topic;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;
    private final ConsumerRecordFactory<K, V> consumerFactory;
    private Long timestamp = null;

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

    public <KR, VR> TestInput<KR, VR> withSerde(final Serde<KR> keySerde, final Serde<VR> valueSerde) {
        return new TestInput<>(this.testDriver, this.topic, keySerde, valueSerde);
    }

    public <KR> TestInput<KR, V> withKeySerde(final Serde<KR> keySerde) {
        return this.withSerde(keySerde, this.valueSerde);
    }

    public <VR> TestInput<K, VR> withValueSerde(final Serde<VR> valueSerde) {
        return this.withSerde(this.keySerde, valueSerde);
    }

    public TestInput<K, V> withDefaultSerde(final Supplier<? extends Serde<K>> keySerdeSupplier, final Supplier<? extends Serde<V>> valueSerdeSupplier) {
        return this.withSerde(ofNullable(this.keySerde).orElseGet(keySerdeSupplier),
                ofNullable(this.valueSerde).orElseGet(valueSerdeSupplier));
    }

    public TestInput<K, V> at(final long timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    public TestInput<K, V> at(final long timestamp, final TimeUnit unit) {
        return this.at(unit.toMillis(timestamp));
    }

    public TestInput<K, V> add(final V value) {
        return this.addInternal(null, value, this.timestamp);
    }

    public TestInput<K, V> add(final K key, final V value) {
        return this.addInternal(key, value, this.timestamp);
    }

    public TestInput<K, V> add(final K key, final V value, final long timestamp) {
        return this.addInternal(key, value, timestamp);
    }

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

