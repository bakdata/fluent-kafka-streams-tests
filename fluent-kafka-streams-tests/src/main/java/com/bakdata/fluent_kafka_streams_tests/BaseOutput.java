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

import com.bakdata.kafka.Preconfigured;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;

abstract class BaseOutput<K, V> implements TestOutput<K, V> {
    private final TopologyTestDriver testDriver;
    private final TestOutputTopic<K, V> testOutputTopic;
    private final String topic;
    private final SerdeConfig<K, V> serdeConfig;

    protected BaseOutput(final TopologyTestDriver testDriver, final String topic, final SerdeConfig<K, V> serdeConfig) {
        this.testDriver = testDriver;
        this.topic = topic;
        this.serdeConfig = serdeConfig;

        this.testOutputTopic = this.testDriver
                .createOutputTopic(this.topic, this.serdeConfig.getKeyDeserializer(),
                        this.serdeConfig.getValueDeserializer());
    }

    /**
     * Set new serde for this output.<br/>
     *
     * @param keySerde The serializer/deserializer to be used for the keys in the output.
     * @param valueSerde The serializer/deserializer to be used for the values in the output.
     */
    @Override
    public <KR, VR> TestOutput<KR, VR> withSerde(final Serde<KR> keySerde, final Serde<VR> valueSerde) {
        return this.with(this.serdeConfig.withSerde(keySerde, valueSerde));
    }

    @Override
    public <KR, VR> TestOutput<KR, VR> configureWithSerde(final Preconfigured<? extends Serde<KR>> keySerde,
            final Preconfigured<? extends Serde<VR>> valueSerde) {
        return this.with(this.serdeConfig.configureWithSerde(keySerde, valueSerde));
    }

    @Override
    public <KR, VR> TestOutput<KR, VR> configureWithSerde(final Serde<KR> keySerde, final Serde<VR> valueSerde) {
        return this.with(this.serdeConfig.configureWithSerde(keySerde, valueSerde));
    }

    /**
     * Set new key serde for this output.<br/>
     */
    @Override
    public <KR> TestOutput<KR, V> withKeySerde(final Serde<KR> keySerde) {
        return this.with(this.serdeConfig.withKeySerde(keySerde));
    }

    @Override
    public <KR> TestOutput<KR, V> configureWithKeySerde(final Preconfigured<? extends Serde<KR>> keySerde) {
        return this.with(this.serdeConfig.configureWithKeySerde(keySerde));
    }

    @Override
    public <KR> TestOutput<KR, V> configureWithKeySerde(final Serde<KR> keySerde) {
        return this.with(this.serdeConfig.configureWithKeySerde(keySerde));
    }

    /**
     * Set new value serde for this output.<br/>
     */
    @Override
    public <VR> TestOutput<K, VR> withValueSerde(final Serde<VR> valueSerde) {
        return this.with(this.serdeConfig.withValueSerde(valueSerde));
    }

    @Override
    public <VR> TestOutput<K, VR> configureWithValueSerde(final Preconfigured<? extends Serde<VR>> valueSerde) {
        return this.with(this.serdeConfig.configureWithValueSerde(valueSerde));
    }

    @Override
    public <VR> TestOutput<K, VR> configureWithValueSerde(final Serde<VR> valueSerde) {
        return this.with(this.serdeConfig.configureWithValueSerde(valueSerde));
    }

    @Override
    public <KR, VR> TestOutput<KR, VR> withTypes(final Class<KR> keyType, final Class<VR> valueType) {
        return this.with(this.serdeConfig.withTypes(keyType, valueType));
    }

    @Override
    public <KR> TestOutput<KR, V> withKeyType(final Class<KR> keyType) {
        return this.with(this.serdeConfig.withKeyType(keyType));
    }

    @Override
    public <VR> TestOutput<K, VR> withValueType(final Class<VR> valueType) {
        return this.with(this.serdeConfig.withValueType(valueType));
    }

    /**
     * Reads the next record and creates an {@link Expectation} for it.<br/>
     *
     * Note that calling this method by itself without chaining at least one of the {@code has*()} methods will not
     * check for the existence of a next record!<br/>
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
     * the output types anymore.<br/>
     */
    @Override
    public TestOutput<K, V> asTable() {
        return new TableOutput<>(this.testDriver, this.topic, this.serdeConfig);
    }

    /**
     * Interpret the output with {@link org.apache.kafka.streams.kstream.KStream} semantics (each key multiple times)
     * .<br/> This is the default, there should usually be no need to call this method.<br/> Note: once the first value
     * of the stream has been read or the iterator has be called, you cannot switch between the output types
     * anymore.<br/>
     */
    @Override
    public TestOutput<K, V> asStream() {
        return new StreamOutput<>(this.testDriver, this.topic, this.serdeConfig);
    }

    /**
     * Convert the output to a {@link java.util.List}. In case the current instance of this class is a
     * {@link StreamOutput}, the output will be converted to List with {@link org.apache.kafka.streams.kstream.KStream}
     * semantics (each key multiple times). In case the current instance of this class is a {@link TableOutput}, the
     * output will be converted to List with {@link org.apache.kafka.streams.kstream.KTable} semantics (each key only
     * once).
     *
     * @return A {@link java.util.List} representing the output
     */
    @Override
    public List<ProducerRecord<K, V>> toList() {
        final List<ProducerRecord<K, V>> list = new ArrayList<>();
        this.iterator().forEachRemaining(list::add);
        return list;
    }

    // ==================
    // Non-public methods
    // ==================
    protected ProducerRecord<K, V> readFromTestDriver() {
        // the Expectation implementation requires null if the topic is empty but outputTopic.readRecord() throws a
        // NoSuchElementException. Thus, we have to check beforehand.
        if (this.testOutputTopic.isEmpty()) {
            return null;
        }
        final TestRecord<K, V> testRecord = this.testOutputTopic.readRecord();
        // partition is always 0, see TopologyTestDriver.PARTITION_ID
        return new ProducerRecord<>(this.topic, 0, testRecord.timestamp(), testRecord.key(), testRecord.value(),
                testRecord.getHeaders());
    }

    protected abstract <VR, KR> TestOutput<KR, VR> create(TopologyTestDriver testDriver, String topic,
            SerdeConfig<KR, VR> serdeConfig);

    private <KR, VR> TestOutput<KR, VR> with(final SerdeConfig<KR, VR> newSerdeConfig) {
        return this.create(this.testDriver, this.topic, newSerdeConfig);
    }
}
