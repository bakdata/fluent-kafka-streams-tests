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

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.NonNull;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.TopologyTestDriver;

class TableOutput<K, V> extends BaseOutput<K, V> {
    private final Map<K, ProducerRecord<K, V>> table = new LinkedHashMap<>();
    private Iterator<ProducerRecord<K, V>> tableIterator;

    TableOutput(final TopologyTestDriver testDriver, final String topic, final SerdeConfig<K, V> serdeConfig) {
        super(testDriver, topic, serdeConfig);
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
        ProducerRecord<K, V> producerRecord = this.readFromTestDriver();
        while (producerRecord != null) {
            this.table.put(producerRecord.key(), producerRecord);
            producerRecord = this.readFromTestDriver();
        }
        return this.table.values().stream().iterator();
    }

    // ==================
    // Non-public methods
    // ==================
    @Override
    protected <VR, KR> TestOutput<KR, VR> create(final TopologyTestDriver testDriver, final String topic,
            final SerdeConfig<KR, VR> serdeConfig) {
        return new TableOutput<>(testDriver, topic, serdeConfig);
    }
}
