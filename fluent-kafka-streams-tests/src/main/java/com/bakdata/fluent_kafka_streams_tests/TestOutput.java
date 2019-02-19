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


/**
 * Represents the output stream of the tested app via the {@link TestTopology}.
 * This can be used via the {@link StreamOutput} or the {@link TableOutput}, dependent on the desired semantics.
 * For more details see each implementation.
 *
 * @param <K> the key type of the output stream
 * @param <V> the value type of the output stream
 */
public interface TestOutput<K, V> extends Iterable<ProducerRecord<K, V>> {
    /**
     * Set new serde for this output.
     *
     * @param keySerde   The serializer/deserializer to be used for the keys in the output.
     * @param valueSerde The serializer/deserializer to be used for the values in the output.
     */
    <KR, VR> TestOutput<KR, VR> withSerde(Serde<KR> keySerde, Serde<VR> valueSerde);

    /**
     * Set new key serde for this output.
     */
    <KR> TestOutput<KR, V> withKeySerde(Serde<KR> keySerde);

    /**
     * Set new value serde for this output.
     */
    <VR> TestOutput<K, VR> withValueSerde(Serde<VR> valueSerde);

    /**
     * TODO: do we even need this anymore?
     */
    TestOutput<K, V> withDefaultSerde(Supplier<? extends Serde<K>> keySerdeSupplier,
                                      Supplier<? extends Serde<V>> valueSerdeSupplier);

    /**
     * TODO
     *
     * @param keyType
     * @param valueType
     */
    default <KR, VR> TestOutput<KR, VR> withTypes(Class<KR> keyType, Class<VR> valueType) {
        return (TestOutput<KR, VR>) this;
    }

    /**
     * TODO
     *
     * @param keyType
     */
    default <KR> TestOutput<KR, V> withKeyType(Class<KR> keyType) {
        return (TestOutput<KR, V>) this;
    }

    /**
     * TODO
     *
     * @param valueType
     */
    default <VR> TestOutput<K, VR> withValueType(Class<VR> valueType) {
        return (TestOutput<K, VR>) this;
    }

    /**
     * Reads the next value from the output stream.
     * Usually, you should not need to call this. The recommended way should be to use either
     * - the {@link #expectNextRecord()} and {@link #expectNoMoreRecord()} methods OR
     * - the iterable interface (via {@link #iterator()}.
     *
     * @return The next value in the output stream depending on the output type (stream or table semantics).
     * {@code null} if no more values are present.
     */
    ProducerRecord<K, V> readOneRecord();

    /**
     * Reads the next record as creates an {@link Expectation} for it.
     *
     * @return An {@link Expectation} containing the next record from the output.
     */
    Expectation<K, V> expectNextRecord();

    /**
     * Reads the next record from the output and expects it to be the end of output.
     *
     * @return An {@link Expectation} containing the next record from the output.
     */
    Expectation<K, V> expectNoMoreRecord();

    /**
     * Interpret the output with {@link org.apache.kafka.streams.kstream.KTable} semantics (each key only once).
     * Note: once the first value of the stream has been read or the iterator has be called, you cannot switch between
     * the output types any more.
     */
    TestOutput<K, V> asTable();

    /**
     * Interpret the output with {@link org.apache.kafka.streams.kstream.KStream} semantics (each key multiple times).
     * This is the default, there should usually be no need to call this method.
     * Note: once the first value of the stream has been read or the iterator has be called, you cannot switch between
     * the output types any more.
     */
    TestOutput<K, V> asStream();
}

/**
 * Represents the {@link TestOutput} with {@link org.apache.kafka.streams.kstream.KStream} semantics.
 */
class StreamOutput<K, V> extends BaseOutput<K, V> {
    /**
     * Reads the next value from the output stream.
     * Usually, you should not need to call this. The recommended way should be to use either
     * - the {@link #expectNextRecord()} and {@link #expectNoMoreRecord()} methods OR
     * - the iterable interface (via {@link #iterator()}.
     *
     * @return The next value in the output stream. {@code null} if no more values are present.
     */
    @Override
    public ProducerRecord<K, V> readOneRecord() {
        return this.readFromTestDriver();
    }

    /**
     * Creates an iterator of {@link ProducerRecord} for the stream output. Can only be read once.
     */
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

    // ==================
    // Non-public methods
    // ==================
    StreamOutput(final TopologyTestDriver testDriver, final String topic, final Serde<K> keySerde,
                 final Serde<V> valueSerde) {
        super(testDriver, topic, keySerde, valueSerde);
    }

    @Override
    protected <VR, KR> TestOutput<KR, VR> create(final TopologyTestDriver testDriver, final String topic,
                                                 final Serde<KR> keySerde,
                                                 final Serde<VR> valueSerde) {
        return new StreamOutput<>(testDriver, topic, keySerde, valueSerde);
    }
}

class TableOutput<K, V> extends BaseOutput<K, V> {
    /**
     * Reads the next value from the output stream.
     * Usually, you should not need to call this. The recommended way should be to use either
     * - the {@link #expectNextRecord()} and {@link #expectNoMoreRecord()} methods OR
     * - the iterable interface (via {@link #iterator()}.
     *
     * @return The next value in the output stream. {@code null} if no more values are present.
     */
    @Override
    public ProducerRecord<K, V> readOneRecord() {
        if (this.tableIterator == null) {
            this.tableIterator = this.iterator();
        }

        // Emulate testDriver, which returns null on last read
        return this.tableIterator.hasNext() ? this.tableIterator.next() : null;
    }

    /**
     * Creates an iterator of {@link ProducerRecord} for the table output. Can only be read once.
     */
    @Override
    public @NonNull Iterator<ProducerRecord<K, V>> iterator() {
        ProducerRecord<K, V> record = this.readFromTestDriver();
        while (record != null) {
            this.table.put(record.key(), record);
            record = this.readFromTestDriver();
        }
        return this.table.values().stream().iterator();
    }

    // ==================
    // Non-public methods
    // ==================
    private final Map<K, ProducerRecord<K, V>> table = new LinkedHashMap<>();
    private Iterator<ProducerRecord<K, V>> tableIterator = null;

    TableOutput(final TopologyTestDriver testDriver, final String topic, final Serde<K> keySerde,
                final Serde<V> valueSerde) {
        super(testDriver, topic, keySerde, valueSerde);
    }

    @Override
    protected <VR, KR> TestOutput<KR, VR> create(final TopologyTestDriver testDriver, final String topic,
                                                 final Serde<KR> keySerde,
                                                 final Serde<VR> valueSerde) {
        return new TableOutput<>(testDriver, topic, keySerde, valueSerde);
    }
}

@RequiredArgsConstructor
abstract class BaseOutput<K, V> implements TestOutput<K, V> {
    protected final TopologyTestDriver testDriver;
    protected final String topic;
    protected final Serde<K> keySerde;
    protected final Serde<V> valueSerde;

    /**
     * Set new serde for this output.
     *
     * @param keySerde   The serializer/deserializer to be used for the keys in the output.
     * @param valueSerde The serializer/deserializer to be used for the values in the output.
     */
    @Override
    public <KR, VR> TestOutput<KR, VR> withSerde(final Serde<KR> keySerde, final Serde<VR> valueSerde) {
        return this.create(this.testDriver, this.topic, keySerde, valueSerde);
    }

    /**
     * Set new key serde for this output.
     */
    public <KR> TestOutput<KR, V> withKeySerde(final Serde<KR> keySerde) {
        return this.withSerde(keySerde, this.valueSerde);
    }

    /**
     * Set new value serde for this output.
     */
    public <VR> TestOutput<K, VR> withValueSerde(final Serde<VR> valueSerde) {
        return this.withSerde(this.keySerde, valueSerde);
    }

    @Override
    public TestOutput<K, V> withDefaultSerde(final Supplier<? extends Serde<K>> keySerdeSupplier, final Supplier<?
            extends Serde<V>> valueSerdeSupplier) {
        return this.withSerde(ofNullable(this.keySerde).orElseGet(keySerdeSupplier),
                ofNullable(this.valueSerde).orElseGet(valueSerdeSupplier));
    }

    /**
     * Reads the next record as creates an {@link Expectation} for it.
     *
     * @return An {@link Expectation} containing the next record from the output.
     */
    @Override
    public Expectation<K, V> expectNextRecord() {
        return new Expectation<>(this.readOneRecord(), this);
    }

    /**
     * Reads the next record from the output and expects it to be the end of output.
     *
     * @return An {@link Expectation} containing the next record from the output.
     */
    @Override
    public Expectation<K, V> expectNoMoreRecord() {
        return this.expectNextRecord().toBeEmpty();
    }

    /**
     * Interpret the output with {@link org.apache.kafka.streams.kstream.KTable} semantics (each key only once).
     * Note: once the first value of the stream has been read or the iterator has be called, you cannot switch between
     * the output types any more.
     */
    @Override
    public TestOutput<K, V> asTable() {
        return new TableOutput<>(this.testDriver, this.topic, this.keySerde, this.valueSerde);
    }

    /**
     * Interpret the output with {@link org.apache.kafka.streams.kstream.KStream} semantics (each key multiple times).
     * This is the default, there should usually be no need to call this method.
     * Note: once the first value of the stream has been read or the iterator has be called, you cannot switch between
     * the output types any more.
     */
    @Override
    public TestOutput<K, V> asStream() {
        return new StreamOutput<>(this.testDriver, this.topic, this.keySerde, this.valueSerde);
    }

    // ==================
    // Non-public methods
    // ==================
    protected ProducerRecord<K, V> readFromTestDriver() {
        return this.testDriver.readOutput(this.topic, this.keySerde.deserializer(), this.valueSerde.deserializer());
    }

    protected abstract <VR, KR> TestOutput<KR, VR> create(TopologyTestDriver testDriver, String topic,
                                                          Serde<KR> keySerde, Serde<VR> valueSerde);
}

