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

    TestOutput<K, V> withDefaultSerde(Supplier<? extends Serde<K>> keySerdeSupplier, Supplier<? extends Serde<V>> valueSerdeSupplier);

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
    public <KR, VR> TestOutput<KR, VR> withSerde(final Serde<KR> keySerde, final Serde<VR> valueSerde) {
        return this.create(this.testDriver, this.topic, keySerde, valueSerde);
    }

    public <KR> TestOutput<KR, V> withKeySerde(final Serde<KR> keySerde) {
        return this.withSerde(keySerde, this.valueSerde);
    }

    public <VR> TestOutput<K, VR> withValueSerde(final Serde<VR> valueSerde) {
        return this.withSerde(this.keySerde, valueSerde);
    }

    @Override
    public TestOutput<K, V> withDefaultSerde(final Supplier<? extends Serde<K>> keySerdeSupplier, final Supplier<? extends Serde<V>> valueSerdeSupplier) {
        return this.withSerde(ofNullable(this.keySerde).orElseGet(keySerdeSupplier),
                ofNullable(this.valueSerde).orElseGet(valueSerdeSupplier));
    }

    @Override
    public Expectation<K, V> expectNextRecord() {
        return new Expectation<>(this.readOneRecord(), this);
    }

    @Override
    public Expectation<K, V> expectNoMoreRecord() {
        return this.expectNextRecord().toBeEmpty();
    }

    @Override
    public TestOutput<K, V> asTable() {
        return new TableOutput<>(this.testDriver, this.topic, this.keySerde, this.valueSerde);
    }

    @Override
    public TestOutput<K, V> asStream() {
        return new StreamOutput<>(this.testDriver, this.topic, this.keySerde, this.valueSerde);
    }

    protected ProducerRecord<K, V> readFromTestDriver() {
        return this.testDriver.readOutput(this.topic, this.keySerde.deserializer(), this.valueSerde.deserializer());
    }

    protected abstract <VR, KR> TestOutput<KR,VR> create(TopologyTestDriver testDriver, String topic, Serde<KR> keySerde, Serde<VR> valueSerde);
}

class StreamOutput<K, V> extends BaseOutput<K, V> {
    StreamOutput(final TopologyTestDriver testDriver, final String topic, final Serde<K> keySerde, final Serde<V> valueSerde) {
        super(testDriver, topic, keySerde, valueSerde);
    }

    @Override
    public ProducerRecord<K, V> readOneRecord() {
        return this.readFromTestDriver();
    }

    @Override
    protected <VR, KR> TestOutput<KR, VR> create(final TopologyTestDriver testDriver, final String topic, final Serde<KR> keySerde,
                                                 final Serde<VR> valueSerde) {
        return new StreamOutput<>(testDriver, topic, keySerde, valueSerde);
    }

    @Override
    public @NonNull Iterator<ProducerRecord<K, V>> iterator() {
        return new Iterator<>() {
            private ProducerRecord<K, V> current = StreamOutput.this.readFromTestDriver();

            @Override
            public boolean hasNext() {
                return this.current != null;
            }

            @Override
            public ProducerRecord<K, V> next() {
                if (!this.hasNext()) {
                    throw new NoSuchElementException();
                }
                final ProducerRecord<K, V> toReturn = this.current;
                this.current = StreamOutput.this.readFromTestDriver();
                return toReturn;
            }
        };
    }
}

class TableOutput<K, V> extends BaseOutput<K, V> {
    private final Map<K, ProducerRecord<K, V>> table = new LinkedHashMap<>();
    private Iterator<ProducerRecord<K, V>> tableIterator = null;

    TableOutput(final TopologyTestDriver testDriver, final String topic, final Serde<K> keySerde, final Serde<V> valueSerde) {
        super(testDriver, topic, keySerde, valueSerde);
    }

    @Override
    public ProducerRecord<K, V> readOneRecord() {
        if (this.tableIterator == null) {
            this.tableIterator = this.iterator();
        }

        // Emulate testDriver, which returns null on last read
        return this.tableIterator.hasNext() ? this.tableIterator.next() : null;
    }

    @Override
    public @NonNull Iterator<ProducerRecord<K, V>> iterator() {
        ProducerRecord<K, V> record = this.readFromTestDriver();
        while (record != null) {
            this.table.put(record.key(), record);
            record = this.readFromTestDriver();
        }
        return this.table.values().stream().iterator();
    }

    @Override
    protected <VR, KR> TestOutput<KR, VR> create(final TopologyTestDriver testDriver, final String topic, final Serde<KR> keySerde,
                                                 final Serde<VR> valueSerde) {
        return new TableOutput<>(testDriver, topic, keySerde, valueSerde);
    }
}

