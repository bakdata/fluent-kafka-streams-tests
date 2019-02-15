package com.bakdata.fluent_kafka_streams_tests.serde;

import lombok.experimental.Delegate;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class JsonSerde<T> implements Serde<T> {
    @Delegate
    private final Serde<T> inner;

    public JsonSerde(final Class<T> clazz) {
        this.inner = Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(clazz));
    }

    public JsonSerde() {
        this((Class<T>) Object.class);
    }
}
