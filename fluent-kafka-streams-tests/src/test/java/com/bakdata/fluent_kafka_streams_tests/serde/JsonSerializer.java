package com.bakdata.fluent_kafka_streams_tests.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.NoArgsConstructor;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.util.Map;

@NoArgsConstructor
public class JsonSerializer<T> implements Serializer<T> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(final Map<String, ?> props, final boolean isKey) {
        // nothing to configure
    }

    @Override
    public byte[] serialize(final String topic, final T data) {
        if (data == null)
            return null;

        try {
            return this.objectMapper.writeValueAsBytes(data);
        } catch (final IOException e) {
            throw new SerializationException("Error serializing JSON message", e);
        }
    }

    @Override
    public void close() {
        // nothing to close
    }

}