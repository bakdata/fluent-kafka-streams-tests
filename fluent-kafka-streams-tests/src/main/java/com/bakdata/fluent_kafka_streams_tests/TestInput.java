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

public class TestInput<K, V> extends ConsumerRecordFactory<K, V> {
    private final TopologyTestDriver testDriver;
    private final String topic;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;

    public TestInput(TopologyTestDriver testDriver, String topic, Serde<K> keySerde, Serde<V> valueSerde) {
        super(topic, keySerde == null ? new DummySerializer<>() : keySerde.serializer(),
                valueSerde == null ? new DummySerializer<>() : valueSerde.serializer());
        this.testDriver = testDriver;
        this.topic = topic;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    public <KR, VR> TestInput<KR, VR> withSerde(Serde<KR> keySerde, Serde<VR> valueSerde) {
        return new TestInput<>(testDriver, topic, keySerde, valueSerde);
    }

    public TestInput<K, V> withDefaultSerde(Supplier<Serde<K>> keySerdeSupplier, Supplier<Serde<V>> valueSerdeSupplier) {
        return withSerde(ofNullable(keySerde).orElseGet(keySerdeSupplier),
                ofNullable(valueSerde).orElseGet(valueSerdeSupplier));
    }

    public TestInput<K, V> and() {
        return this;
    }

    public ConsumerRecord<byte[], byte[]> create(final String topicName,
                                                 final K key,
                                                 final V value,
                                                 final Headers headers,
                                                 final long timestampMs) {
        final ConsumerRecord<byte[], byte[]> record = super.create(topicName, key, value, headers, timestampMs);
        testDriver.pipeInput(record);
        return record;
    }

    private static class DummySerializer<V> implements Serializer<V> {
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

