package com.bakdata.fluent_kafka_streams_tests;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.TopologyTestDriver;


/**
 * <p>Represents the output stream of the tested app via the {@link TestTopology}.</p>
 * <p>This can be used via the {@link StreamOutput} or the {@link TableOutput}, dependent on the desired semantics.</p>
 * <p>For more details see each implementation.</p>
 *
 * <p>Note: The StreamOutput is a one-time iterable. Cache it if you need to iterate several times.</p>
 *
 * @param <K> the key type of the output stream
 * @param <V> the value type of the output stream
 */
public interface TestOutput<K, V> extends Iterable<ProducerRecord<K, V>> {
    /**
     * Set new serde for this output.
     *
     * @param keySerde   The serializer/deserializer to be used for the keys in the output
     * @param valueSerde The serializer/deserializer to be used for the values in the output
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
     * <p>Type-casts the key and value to the given types.</p>
     *
     * A type-cast is useful if you have general-purpose serde, such as Json or Avro, which is used for different
     * types in input and output. Thus, instead of unnecessarily overriding the serde, this method just casts the
     * output.
     *
     * @param keyType the new key type
     * @param valueType the new value type
     */
    default <KR, VR> TestOutput<KR, VR> withTypes(Class<KR> keyType, Class<VR> valueType) {
        return (TestOutput<KR, VR>) this;
    }

    /**
     * <p>Type-casts the key to the given type.</p>
     *
     * A type-cast is useful if you have general-purpose serde, such as Json or Avro, which is used for different
     * types in input and output. Thus, instead of unnecessarily overriding the serde, this method just casts the
     * output.
     *
     * @param keyType the new key type
     */
    default <KR> TestOutput<KR, V> withKeyType(Class<KR> keyType) {
        return (TestOutput<KR, V>) this;
    }

    /**
     * <p>Type-casts the value to the given type.</p>
     *
     * A type-cast is useful if you have general-purpose serde, such as Json or Avro, which is used for different
     * types in input and output. Thus, instead of unnecessarily overriding the serde, this method just casts the
     * output.
     *
     * @param valueType the new value type
     */
    default <VR> TestOutput<K, VR> withValueType(Class<VR> valueType) {
        return (TestOutput<K, VR>) this;
    }

    /**
     * <p>Reads the next value from the output stream.</p>
     * Usually, you should not need to call this. The recommended way should be to use either
     * <ul>
     * <li>the {@link #expectNextRecord()} and {@link #expectNoMoreRecord()} methods OR</li>
     * <li>the iterable interface (via {@link #iterator()}.</li>
     * </ul>
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
     * <p>Interpret the output with {@link org.apache.kafka.streams.kstream.KTable} semantics (each key only once).</p>
     * <p>Note: once the first value of the stream has been read or the iterator has be called, you cannot switch
     * between
     * the output types any more.</p>
     */
    TestOutput<K, V> asTable();

    /**
     * <p>Interpret the output with {@link org.apache.kafka.streams.kstream.KStream} semantics (each key multiple
     * times).</p>
     * <p>This is the default, there should usually be no need to call this method.</p>
     * <p>Note: once the first value of the stream has been read or the iterator has be called, you cannot switch
     * between
     * the output types any more.</p>
     */
    TestOutput<K, V> asStream();
}

/**
 * <p>Represents the {@link TestOutput} with {@link org.apache.kafka.streams.kstream.KStream} semantics.</p>
 *
 * <p>Note: The StreamOutput is a one-time iterable. Cache it if you need to iterate several times.</p>
 */
class StreamOutput<K, V> extends BaseOutput<K, V> {
    // ==================
    // Non-public methods
    // ==================
    StreamOutput(final TopologyTestDriver testDriver, final String topic, final Serde<K> keySerde,
            final Serde<V> valueSerde) {
        super(testDriver, topic, keySerde, valueSerde);
    }

    /**
     * Reads the next value from the output stream.<br/>
     * Usually, you should not need to call this. The recommended way should be to use either
     * <ul>
     * <li>the {@link #expectNextRecord()} and {@link #expectNoMoreRecord()} methods OR</li>
     * <li>the iterable interface (via {@link #iterator()}.</li>
     * </ul>
     *
     * @return The next value in the output stream. {@code null} if no more values are present.<br/>
     */
    @Override
    public ProducerRecord<K, V> readOneRecord() {
        return this.readFromTestDriver();
    }

    /**
     * Creates an iterator of {@link ProducerRecord} for the stream output. Can only be read once.<br/>
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

    @Override
    protected <VR, KR> TestOutput<KR, VR> create(final TopologyTestDriver testDriver, final String topic,
            final Serde<KR> keySerde, final Serde<VR> valueSerde) {
        return new StreamOutput<>(testDriver, topic, keySerde, valueSerde);
    }
}

class TableOutput<K, V> extends BaseOutput<K, V> {
    // ==================
    // Non-public methods
    // ==================
    private final Map<K, ProducerRecord<K, V>> table = new LinkedHashMap<>();
    private Iterator<ProducerRecord<K, V>> tableIterator = null;

    TableOutput(final TopologyTestDriver testDriver, final String topic, final Serde<K> keySerde,
            final Serde<V> valueSerde) {
        super(testDriver, topic, keySerde, valueSerde);
    }

    /**
     * <p>Reads the next value from the output stream.</p>
     * Usually, you should not need to call this. The recommended way should be to use either
     * <ul>
     * <li>the {@link #expectNextRecord()} and {@link #expectNoMoreRecord()} methods OR</li>
     * <li>the iterable interface (via {@link #iterator()}.</li>
     * </ul>
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

    @Override
    protected <VR, KR> TestOutput<KR, VR> create(final TopologyTestDriver testDriver, final String topic,
            final Serde<KR> keySerde, final Serde<VR> valueSerde) {
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
     * Set new serde for this output.<br/>
     *
     * @param keySerde   The serializer/deserializer to be used for the keys in the output.
     * @param valueSerde The serializer/deserializer to be used for the values in the output.
     */
    @Override
    public <KR, VR> TestOutput<KR, VR> withSerde(final Serde<KR> keySerde, final Serde<VR> valueSerde) {
        return this.create(this.testDriver, this.topic, keySerde, valueSerde);
    }

    /**
     * Set new key serde for this output.<br/>
     */
    public <KR> TestOutput<KR, V> withKeySerde(final Serde<KR> keySerde) {
        return this.withSerde(keySerde, this.valueSerde);
    }

    /**
     * Set new value serde for this output.<br/>
     */
    public <VR> TestOutput<K, VR> withValueSerde(final Serde<VR> valueSerde) {
        return this.withSerde(this.keySerde, valueSerde);
    }

    /**
     * Reads the next record as creates an {@link Expectation} for it.<br/>
     *
     * @return An {@link Expectation} containing the next record from the output.<br/>
     */
    @Override
    public Expectation<K, V> expectNextRecord() {
        return new Expectation<>(this.readOneRecord(), this);
    }

    /**
     * Reads the next record from the output and expects it to be the end of output.<br/>
     *
     * @return An {@link Expectation} containing the next record from the output.<br/>
     */
    @Override
    public Expectation<K, V> expectNoMoreRecord() {
        return this.expectNextRecord().toBeEmpty();
    }

    /**
     * Interpret the output with {@link org.apache.kafka.streams.kstream.KTable} semantics (each key only once).<br/>
     * Note: once the first value of the stream has been read or the iterator has be called, you cannot switch between
     * the output types any more.<br/>
     */
    @Override
    public TestOutput<K, V> asTable() {
        return new TableOutput<>(this.testDriver, this.topic, this.keySerde, this.valueSerde);
    }

    /**
     * Interpret the output with {@link org.apache.kafka.streams.kstream.KStream} semantics (each key multiple times)
     * .<br/>
     * This is the default, there should usually be no need to call this method.<br/>
     * Note: once the first value of the stream has been read or the iterator has be called, you cannot switch between
     * the output types any more.<br/>
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

