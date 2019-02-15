package com.bakdata.fluent_kafka_streams_tests.serde;

import lombok.experimental.Delegate;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class JsonSerde<T> implements Serde<T> {
    @Delegate
    private final Serde<T> inner;

    public JsonSerde(Class<T> clazz) {
        inner = Serdes.serdeFrom(new JsonSerializer<T>(), new JsonDeserializer<T>(clazz));
    }

    public JsonSerde() {
        this((Class<T>) Object.class);
    }
}
