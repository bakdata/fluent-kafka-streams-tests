package com.bakdata.fluent_kafka_streams_tests;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.TopologyTestDriver;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Supplier;

import static java.util.Optional.ofNullable;

public interface TestOutput<K, V> extends Iterable<ProducerRecord<K, V>> {
    <KR, VR> TestOutput<KR, VR> withSerde(Serde<KR> keySerde, Serde<VR> valueSerde);
    <KR> TestOutput<KR, V> withKeySerde(Serde<KR> keySerde);
    <VR> TestOutput<K, VR> withValueSerde(Serde<VR> valueSerde);
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

    public <KR> TestOutput<KR, V> withKeySerde(Serde<KR> keySerde) {
        return withSerde(keySerde, valueSerde);
    }

    public <VR> TestOutput<K, VR> withValueSerde(Serde<VR> valueSerde) {
        return withSerde(keySerde, valueSerde);
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

    protected ProducerRecord<K, V> readFromTestDriver() {
        return testDriver.readOutput(topic, keySerde.deserializer(), valueSerde.deserializer());
    }

    protected abstract <VR, KR> TestOutput<KR,VR> create(TopologyTestDriver testDriver, String topic, Serde<KR> keySerde, Serde<VR> valueSerde);
}

class StreamOutput<K, V> extends BaseOutput<K, V> {
    public StreamOutput(TopologyTestDriver testDriver, String topic, Serde<K> keySerde, Serde<V> valueSerde) {
        super(testDriver, topic, keySerde, valueSerde);
    }

    @Override
    public ProducerRecord<K, V> readOneRecord() {
        return readFromTestDriver();
    }

    @Override
    protected <VR, KR> TestOutput<KR, VR> create(TopologyTestDriver testDriver, String topic, Serde<KR> keySerde,
                                                          Serde<VR> valueSerde) {
        return new StreamOutput<>(testDriver, topic, keySerde, valueSerde);
    }

    @Override
    public @NonNull Iterator<ProducerRecord<K, V>> iterator() {
        return new Iterator<>() {
            private ProducerRecord<K, V> current = readFromTestDriver();

            @Override
            public boolean hasNext() {
                return current != null;
            }

            @Override
            public ProducerRecord<K, V> next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                ProducerRecord<K, V> toReturn = current;
                current = readFromTestDriver();
                return toReturn;
            }
        };
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
            tableIterator = iterator();
        }

        // Emulate testDriver, which returns null on last read
        return tableIterator.hasNext() ? tableIterator.next() : null;
    }

    @Override
    public @NonNull Iterator<ProducerRecord<K, V>> iterator() {
        ProducerRecord<K, V> record = readFromTestDriver();
        while (record != null) {
            table.put(record.key(), record);
            record = readFromTestDriver();
        }
        return table.values().stream().iterator();
    }

    @Override
    protected <VR, KR> TestOutput<KR, VR> create(TopologyTestDriver testDriver, String topic, Serde<KR> keySerde,
                                                          Serde<VR> valueSerde) {
        return new TableOutput<>(testDriver, topic, keySerde, valueSerde);
    }
}

