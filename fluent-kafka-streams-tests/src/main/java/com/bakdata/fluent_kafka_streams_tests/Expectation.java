package com.bakdata.fluent_kafka_streams_tests;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Assertions;

@RequiredArgsConstructor
public class Expectation<K, V> {
    private final ProducerRecord<K, V> record;
    private final TestOutput<K, V> output;

    public Expectation<K, V> isPresent() {
        Assertions.assertNotNull(this.record, "No more records found");
        return this.and();
    }

    public Expectation<K, V> hasKey(final K key) {
        this.isPresent();
        Assertions.assertEquals(key, this.record.key(), "Record key does not match");
        return this.and();
    }

    public Expectation<K, V> hasValue(final V value) {
        this.isPresent();
        Assertions.assertEquals(value, this.record.value(), "Record value does not match");
        return this.and();
    }

    public Expectation<K, V> and() {
        return this;
    }

    public Expectation<K, V> expectNextRecord() {
        return this.output.expectNextRecord();
    }

    public Expectation<K, V> expectNoMoreRecord() {
        return this.output.expectNoMoreRecord();
    }

    public Expectation<K, V> toBeEmpty() {
        Assertions.assertNull(this.record);
        return this.and();
    }
}