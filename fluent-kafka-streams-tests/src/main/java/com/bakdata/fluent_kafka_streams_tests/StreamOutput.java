/*
 * MIT License
 *
 * Copyright (c) 2025 bakdata
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

import com.bakdata.kafka.Configurator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import lombok.NonNull;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.TopologyTestDriver;

/**
 * <p>Represents the {@link TestOutput} with {@link org.apache.kafka.streams.kstream.KStream} semantics.</p>
 *
 * <p>Note: The StreamOutput is a one-time iterable. Cache it if you need to iterate several times.</p>
 */
class StreamOutput<K, V> extends BaseOutput<K, V> {
    StreamOutput(final TopologyTestDriver testDriver, final String topic, final Serde<K> keySerde,
            final Serde<V> valueSerde, final Serde<?> defaultKeySerde, final Serde<?> defaultValueSerde,
            final Configurator configurator) {
        super(testDriver, topic, keySerde, valueSerde, defaultKeySerde, defaultValueSerde, configurator);
    }

    /**
     * Reads the next value from the output stream.<br/> Usually, you should not need to call this. The recommended way
     * should be to use either
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
        return new Iterator<ProducerRecord<K, V>>() {
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
    @Override
    protected <VR, KR> TestOutput<KR, VR> create(final TopologyTestDriver testDriver, final String topic,
            final Serde<KR> keySerde, final Serde<VR> valueSerde, final Serde<?> defaultKeySerde,
            final Serde<?> defaultValueSerde, final Configurator configurator) {
        return new StreamOutput<>(testDriver, topic, keySerde, valueSerde, defaultKeySerde, defaultValueSerde,
                configurator);
    }
}
