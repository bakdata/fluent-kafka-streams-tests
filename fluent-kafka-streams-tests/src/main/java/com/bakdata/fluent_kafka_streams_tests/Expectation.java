package com.bakdata.fluent_kafka_streams_tests;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Assertions;

@RequiredArgsConstructor
public class Expectation<K, V> {
    private final ProducerRecord<K, V> record;
    private final TestOutput<K, V> output;

    public Expectation<K, V>  isPresent() {
        Assertions.assertNotNull(record, "No more records found");
        return and();
    }

    public Expectation<K, V>  hasKey(K key) {
        isPresent();
        Assertions.assertEquals(key, record.key(), "Record key does not match");
        return and();
    }

    public Expectation<K, V>  hasValue(V value) {
        isPresent();
        Assertions.assertEquals(value, record.value(), "Record value does not match");
        return and();
    }

    public Expectation<K, V> and() {
        return this;
    }

    public Expectation<K, V> expectNextRecord() {
        return output.expectNextRecord();
    }

    public Expectation<K, V> expectNoMoreRecord() {
        return output.expectNoMoreRecord();
    }

    public Expectation<K, V> toBeEmpty() {
        Assertions.assertNull(record);
        return and();
    }
}