/*
 * MIT License
 *
 * Copyright (c) 2023 bakdata
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.bakdata.fluent_kafka_streams_tests;

import java.util.Objects;
import java.util.function.Consumer;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Represents a single output {@link ProducerRecord} from {@link TestOutput} to be tested.
 *
 * @param <K> the key type of the record under test
 * @param <V> the value type of the record under test
 */
@RequiredArgsConstructor
public class Expectation<K, V> {
    private final ProducerRecord<K, V> lastRecord;
    private final TestOutput<K, V> output;

    /**
     * Asserts whether a record exists.
     */
    public Expectation<K, V> isPresent() {
        if (this.lastRecord == null) {
            throw new AssertionError("No more records found");
        }
        return this.and();
    }

    /**
     * Checks for the equality of the {@link ProducerRecord#key()} and {@code expectedKey}.
     *
     * @param expectedKey key to expect
     * @return the current {@code Expectation} chain
     */
    public Expectation<K, V> hasKey(final K expectedKey) {
        this.isPresent();
        if (!Objects.equals(this.lastRecord.key(), expectedKey)) {
            throw new AssertionError("Record key does not match");
        }
        return this.and();
    }

    /**
     * Forwards {@link ProducerRecord#key()} to the provided condition in order make assertions using another
     * framework.
     *
     * @param requirements consumer that accepts the current record's key
     * @return the current {@code Expectation} chain
     */
    public Expectation<K, V> hasKeySatisfying(final Consumer<? super K> requirements) {
        this.isPresent();
        requirements.accept(this.lastRecord.key());
        return this.and();
    }

    /**
     * Checks for the equality of the {@link ProducerRecord#value()} and {@code expectedValue}.
     * @param expectedValue value to expect
     * @return the current {@code Expectation} chain
     */
    public Expectation<K, V> hasValue(final V expectedValue) {
        this.isPresent();
        if (!Objects.equals(this.lastRecord.value(), expectedValue)) {
            throw new AssertionError("Record value does not match");
        }
        return this.and();
    }

    /**
     * Forwards {@link ProducerRecord#value()} to the provided condition in order make assertions using another
     * framework.
     *
     * @param requirements consumer that accepts the current record's value
     * @return the current {@code Expectation} chain
     */
    public Expectation<K, V> hasValueSatisfying(final Consumer<? super V> requirements) {
        this.isPresent();
        requirements.accept(this.lastRecord.value());
        return this.and();
    }

    /**
     * Concatenates calls to this Expectation. It is not necessary to call this method, but it can be seen as a more
     * readable alternative to simple chaining.
     * @return this
     */
    public Expectation<K, V> and() {
        return this;
    }

    /**
     * <p>Reads the next record as creates an {@code Expectation} for it.</p>
     * <p>This is logically equivalent to {@link TestOutput#expectNextRecord()}.</p>
     * <p>This methods main purpose is to allow chaining:</p>
     * <pre>{@code
     * myOutput.expectNextRecord()
     *         .expectNextRecord()
     *         .expectNoMoreRecord();
     * }</pre>
     *
     * @return An {@code Expectation} containing the next record from the output.
     */
    public Expectation<K, V> expectNextRecord() {
        return this.output.expectNextRecord();
    }

    /**
     * <p>Reads the next record from the output and expects it to be the end of output.</p>
     * <p>This is logically equivalent to {@link TestOutput#expectNoMoreRecord()}.</p>
     * <p>This methods main purpose is to allow chaining:</p>
     * <pre>{@code
     * myOutput.expectNextRecord()
     *         .expectNextRecord()
     *         .expectNoMoreRecord();
     * }</pre>
     *
     * @return An {@code Expectation} containing the next record from the output.
     */
    public Expectation<K, V> expectNoMoreRecord() {
        return this.output.expectNoMoreRecord();
    }

    /**
     * <p>Asserts that there is no records present, i.e., the end of the output has been reached.</p>
     * <p>This method should be used when there are no records at all expected.</p>
     * @return the current {@code Expectation} chain
     */
    public Expectation<K, V> toBeEmpty() {
        if (this.lastRecord != null) {
            throw new AssertionError("More records found");
        }
        return this.and();
    }
}
