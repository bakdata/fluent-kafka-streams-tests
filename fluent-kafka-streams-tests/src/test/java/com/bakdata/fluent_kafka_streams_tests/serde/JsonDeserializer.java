package com.bakdata.fluent_kafka_streams_tests.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

public class JsonDeserializer<T> implements Deserializer<T> {
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Class<? extends T> clazz;

    public JsonDeserializer(final Class<? extends T> clazz) {
        this.clazz = clazz;
    }

    @Override
    public void configure(final Map<String, ?> props, final boolean isKey) {
        // nothing to configure
    }

    @Override
    public T deserialize(final String topic, final byte[] bytes) {
        if (bytes == null)
            return null;

        try {
            return this.objectMapper.readValue(bytes, this.clazz);
        } catch (final IOException e) {
            throw new SerializationException(e);
        }
    }

    @Override
    public void close() {
        // nothing to close
    }
}