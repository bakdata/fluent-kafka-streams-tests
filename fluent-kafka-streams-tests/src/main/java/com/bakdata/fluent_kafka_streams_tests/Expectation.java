package com.bakdata.fluent_kafka_streams_tests;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Assertions;

/**
 * Represents a single output {@link ProducerRecord} from {@link TestOutput} to be tested.<br/>
 *
 * @param <K> the key type of the record under test
 * @param <V> the value type of the record under test
 */
@RequiredArgsConstructor
public class Expectation<K, V> {
    private final ProducerRecord<K, V> record;
    private final TestOutput<K, V> output;

    /**
     * Asserts whether a record exists.<br/>
     */
    public Expectation<K, V> isPresent() {
        Assertions.assertNotNull(this.record, "No more records found");
        return this.and();
    }

    /**
     * Checks for the equality of the {@link ProducerRecord#key()} and {@code expectedKey}.<br/>
     */
    public Expectation<K, V> hasKey(final K expectedKey) {
        this.isPresent();
        Assertions.assertEquals(expectedKey, this.record.key(), "Record key does not match");
        return this.and();
    }

    /**
     * Checks for the equality of the {@link ProducerRecord#value()} and {@code expectedValue}.<br/>
     */
    public Expectation<K, V> hasValue(final V expectedValue) {
        this.isPresent();
        Assertions.assertEquals(expectedValue, this.record.value(), "Record value does not match");
        return this.and();
    }

    /**
     * Concatenates calls to this Expectation. It is not necessary to call this method, but it can be seen as a more
     * readable alternative to simple chaining.<br/>
     */
    public Expectation<K, V> and() {
        return this;
    }

    /**
     * Reads the next record as creates an {@link Expectation} for it.<br/>
     * This is logically equivalent to {@link TestOutput#expectNextRecord()}.<br/>
     * This methods main purpose is to allow chaining:
     *
     * <pre>{@code
     * myOutput.expectNextRecord()
     *         .expectNextRecord()
     *         .expectNoMoreRecord();
     * }</pre>
     *
     * @return An {@link Expectation} containing the next record from the output.<br/>
     */
    public Expectation<K, V> expectNextRecord() {
        return this.output.expectNextRecord();
    }

    /**
     * Reads the next record from the output and expects it to be the end of output.<br/>
     * This is logically equivalent to {@link TestOutput#expectNoMoreRecord()}.<br/>
     * This methods main purpose is to allow chaining:
     *
     * <pre>{@code
     * myOutput.expectNextRecord()
     *         .expectNextRecord()
     *         .expectNoMoreRecord();
     * }</pre>
     *
     * @return An {@link Expectation} containing the next record from the output.<br/>
     */
    public Expectation<K, V> expectNoMoreRecord() {
        return this.output.expectNoMoreRecord();
    }

    /**
     * Asserts that there is no recors present, i.e., the end of the output has been reached.<br/>
     */
    public Expectation<K, V> toBeEmpty() {
        Assertions.assertNull(this.record);
        return this.and();
    }
}