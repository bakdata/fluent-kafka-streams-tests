/*
 * MIT License
 *
 * Copyright (c) 2024 bakdata GmbH
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

import java.util.List;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;


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
     * @param keySerde The serializer/deserializer to be used for the keys in the output
     * @param valueSerde The serializer/deserializer to be used for the values in the output
     * @return Copy of current {@code TestOutput} with provided serdes
     */
    <KR, VR> TestOutput<KR, VR> withSerde(Serde<KR> keySerde, Serde<VR> valueSerde);

    /**
     * Set new key serde for this output.
     *
     * @param keySerde The serializer/deserializer to be used for the keys in the output
     * @return Copy of current {@code TestOutput} with provided key serde
     */
    <KR> TestOutput<KR, V> withKeySerde(Serde<KR> keySerde);

    /**
     * Set new value serde for this output.
     * @param valueSerde The serializer/deserializer to be used for the values in the output
     * @return Copy of current {@code TestOutput} with provided value serde
     */
    <VR> TestOutput<K, VR> withValueSerde(Serde<VR> valueSerde);

    /**
     * <p>Type-casts the key and value to the given types.</p>
     *
     * A type-cast is useful if you have general-purpose serde, such as Json or Avro, which is used for different types
     * in input and output. Thus, instead of unnecessarily overriding the serde, this method just casts the output.
     *
     * @param keyType the new key type.
     * @param valueType the new value type.
     * @return Copy of current {@code TestOutput} with provided types
     */
    default <KR, VR> TestOutput<KR, VR> withTypes(final Class<KR> keyType, final Class<VR> valueType) {
        return (TestOutput<KR, VR>) this;
    }

    /**
     * <p>Type-casts the key to the given type.</p>
     *
     * A type-cast is useful if you have general-purpose serde, such as Json or Avro, which is used for different types
     * in input and output. Thus, instead of unnecessarily overriding the serde, this method just casts the output.
     *
     * @param keyType the new key type.
     * @return Copy of current {@code TestOutput} with provided key type
     */
    default <KR> TestOutput<KR, V> withKeyType(final Class<KR> keyType) {
        return (TestOutput<KR, V>) this;
    }

    /**
     * <p>Type-casts the value to the given type.</p>
     *
     * A type-cast is useful if you have general-purpose serde, such as Json or Avro, which is used for different types
     * in input and output. Thus, instead of unnecessarily overriding the serde, this method just casts the output.
     *
     * @param valueType the new value type.
     * @return Copy of current {@code TestOutput} with provided value type
     */
    default <VR> TestOutput<K, VR> withValueType(final Class<VR> valueType) {
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
     * @return The next value in the output stream depending on the output type (stream or table semantics). {@code
     * null} if no more values are present.
     */
    ProducerRecord<K, V> readOneRecord();

    /**
     * Reads the next record and creates an {@link Expectation} for it.
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
     * between the output types any more.</p>
     * @return Current output with {@link org.apache.kafka.streams.kstream.KTable} semantics
     */
    TestOutput<K, V> asTable();

    /**
     * <p>Interpret the output with {@link org.apache.kafka.streams.kstream.KStream} semantics (each key multiple
     * times).</p>
     * <p>This is the default, there should usually be no need to call this method.</p>
     * <p>Note: once the first value of the stream has been read or the iterator has be called, you cannot switch
     * between the output types any more.</p>
     *
     * @return Current output with {@link org.apache.kafka.streams.kstream.KStream} semantics
     */
    TestOutput<K, V> asStream();

    /**
     * Convert the output to a {@link java.util.List}.
     *
     * @return A {@link java.util.List} representing the output
     */
    List<ProducerRecord<K, V>> toList();
}

