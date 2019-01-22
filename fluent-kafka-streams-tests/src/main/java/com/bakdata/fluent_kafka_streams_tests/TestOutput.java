package com.bakdata.fluent_kafka_streams_tests;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.TopologyTestDriver;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;

import static java.util.Optional.ofNullable;

public interface TestOutput<K, V> {
    <KR, VR> TestOutput<KR, VR> withSerde(Serde<KR> keySerde, Serde<VR> valueSerde);
    TestOutput<K, V> withDefaultSerde(Supplier<Serde<K>> keySerdeSupplier, Supplier<Serde<V>> valueSerdeSupplier);

    ProducerRecord<K, V> readOneRecord();
    Expectation<K, V> expectNextRecord();
    Expectation<K, V> expectNoMoreRecord();

    // Table semantics (each key only once)
    TestOutput<K, V> asTable();

    // Stream semantics (each key possibly multiple times)
    TestOutput<K, V> asStream();
}

@RequiredArgsConstructor
abstract class BaseOutput<K, V> implements TestOutput<K, V> {
    protected final TopologyTestDriver testDriver;
    protected final String topic;
    protected final Serde<K> keySerde;
    protected final Serde<V> valueSerde;

    @Override
    public <KR, VR> TestOutput<KR, VR> withSerde(Serde<KR> keySerde, Serde<VR> valueSerde) {
        return create(testDriver, topic, keySerde, valueSerde);
    }

    @Override
    public TestOutput<K, V> withDefaultSerde(Supplier<Serde<K>> keySerdeSupplier, Supplier<Serde<V>> valueSerdeSupplier) {
        return withSerde(ofNullable(keySerde).orElseGet(keySerdeSupplier),
                ofNullable(valueSerde).orElseGet(valueSerdeSupplier));
    }

    @Override
    public Expectation<K, V> expectNextRecord() {
        return new Expectation<>(readOneRecord(), this);
    }

    @Override
    public Expectation<K, V> expectNoMoreRecord() {
        return expectNextRecord().toBeEmpty();
    }

    @Override
    public TestOutput<K, V> asTable() {
        return new TableOutput<>(testDriver, topic, keySerde, valueSerde);
    }

    @Override
    public TestOutput<K, V> asStream() {
        return new StreamOutput<>(testDriver, topic, keySerde, valueSerde);
    }

    // Private
    protected abstract <VR, KR> TestOutput<KR,VR> create(TopologyTestDriver testDriver, String topic, Serde<KR> keySerde, Serde<VR> valueSerde);
}

class StreamOutput<K, V> extends BaseOutput<K, V> {
    public StreamOutput(TopologyTestDriver testDriver, String topic, Serde<K> keySerde, Serde<V> valueSerde) {
        super(testDriver, topic, keySerde, valueSerde);
    }

    @Override
    public ProducerRecord<K, V> readOneRecord() {
        return testDriver.readOutput(topic, keySerde.deserializer(), valueSerde.deserializer());
    }

    @Override
    protected <VR, KR> TestOutput<KR, VR> create(TopologyTestDriver testDriver, String topic, Serde<KR> keySerde,
                                                          Serde<VR> valueSerde) {
        return new StreamOutput<>(testDriver, topic, keySerde, valueSerde);
    }
}

class TableOutput<K, V> extends BaseOutput<K, V> {
    private final Map<K, ProducerRecord<K, V>> table = new LinkedHashMap<>();
    private Iterator<ProducerRecord<K, V>> tableIterator = null;

    public TableOutput(TopologyTestDriver testDriver, String topic, Serde<K> keySerde, Serde<V> valueSerde) {
        super(testDriver, topic, keySerde, valueSerde);
    }

    @Override
    public ProducerRecord<K, V> readOneRecord() {
        if (tableIterator == null) {
            ProducerRecord<K, V> record = testDriver.readOutput(topic, keySerde.deserializer(), valueSerde.deserializer());
            while (record != null) {
                table.put(record.key(), record);
                record = testDriver.readOutput(topic, keySerde.deserializer(), valueSerde.deserializer());
            }

            tableIterator = table.values().stream().iterator();
        }

        // Emulate testDriver, which returns null on last read
        return tableIterator.hasNext() ? tableIterator.next() : null;
    }

    @Override
    protected <VR, KR> TestOutput<KR, VR> create(TopologyTestDriver testDriver, String topic, Serde<KR> keySerde,
                                                          Serde<VR> valueSerde) {
        return new TableOutput<>(testDriver, topic, keySerde, valueSerde);
    }
}

