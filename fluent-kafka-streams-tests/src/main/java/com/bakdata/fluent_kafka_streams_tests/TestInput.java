package com.bakdata.fluent_kafka_streams_tests;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;

import java.util.Map;
import java.util.function.Supplier;

import static java.util.Optional.ofNullable;

public class TestInput<K, V> {
    private final TopologyTestDriver testDriver;
    private final String topic;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;

    private final ConsumerRecordFactory<K, V> consumerFactory;

    public TestInput(TopologyTestDriver testDriver, String topic, Serde<K> keySerde, Serde<V> valueSerde) {
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

    public <KR, VR> TestInput<KR, VR> withSerde(Serde<KR> keySerde, Serde<VR> valueSerde) {
        return new TestInput<>(testDriver, topic, keySerde, valueSerde);
    }

    public <KR> TestInput<KR, V> withKeySerde(Serde<KR> keySerde) {
        return withSerde(keySerde, valueSerde);
    }

    public <VR> TestInput<K, VR> withValueSerde(Serde<VR> valueSerde) {
        return withSerde(keySerde, valueSerde);
    }

    public TestInput<K, V> withDefaultSerde(Supplier<Serde<K>> keySerdeSupplier, Supplier<Serde<V>> valueSerdeSupplier) {
        return withSerde(ofNullable(keySerde).orElseGet(keySerdeSupplier),
                ofNullable(valueSerde).orElseGet(valueSerdeSupplier));
    }

    public TestInput<K, V> add(final V value) {
        this.consumerFactory.create(value);
        return this;
    }

    public TestInput<K, V> add(final K key, final V value) {
        this.consumerFactory.create(key, value);
        return this;
    }

    public TestInput<K, V> add(final K key, final V value, final long timestamp) {
        this.consumerFactory.create(key, value, timestamp);
        return this;
    }

    private static class UnspecifiedSerializer<V> implements Serializer<V> {
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

