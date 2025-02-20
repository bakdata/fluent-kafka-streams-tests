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

package com.bakdata.kafka;

import static com.bakdata.kafka.Preconfigured.create;

import java.util.Map;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Configure {@link Serde} and {@link Serializer} using base properties
 */
@RequiredArgsConstructor
public class Configurator {

    private final @NonNull Map<String, Object> kafkaProperties;

    /**
     * Configure a {@code Serde} for values using {@link #kafkaProperties}
     * @param serde serde to configure
     * @return configured {@code Serde}
     * @param <T> type to be (de-)serialized
     */
    public <T> Serde<T> configureForValues(final Serde<T> serde) {
        return this.configureForValues(create(serde));
    }

    /**
     * Configure a {@code Serde} for values using {@link #kafkaProperties} and config overrides
     * @param serde serde to configure
     * @param configOverrides configuration overrides
     * @return configured {@code Serde}
     * @param <T> type to be (de-)serialized
     */
    public <T> Serde<T> configureForValues(final Serde<T> serde, final Map<String, Object> configOverrides) {
        return this.configureForValues(create(serde, configOverrides));
    }

    /**
     * Configure a {@code Serde} for keys using {@link #kafkaProperties}
     * @param serde serde to configure
     * @return configured {@code Serde}
     * @param <T> type to be (de-)serialized
     */
    public <T> Serde<T> configureForKeys(final Serde<T> serde) {
        return this.configureForKeys(create(serde));
    }

    /**
     * Configure a {@code Serde} for keys using {@link #kafkaProperties} and config overrides
     * @param serde serde to configure
     * @param configOverrides configuration overrides
     * @return configured {@code Serde}
     * @param <T> type to be (de-)serialized
     */
    public <T> Serde<T> configureForKeys(final Serde<T> serde, final Map<String, Object> configOverrides) {
        return this.configureForKeys(create(serde, configOverrides));
    }

    /**
     * Configure a {@code Serializer} for values using {@link #kafkaProperties}
     * @param serializer serializer to configure
     * @return configured {@code Serializer}
     * @param <T> type to be (de-)serialized
     */
    public <T> Serializer<T> configureForValues(final Serializer<T> serializer) {
        return this.configureForValues(create(serializer));
    }

    /**
     * Configure a {@code Serializer} for values using {@link #kafkaProperties} and config overrides
     * @param serializer serializer to configure
     * @param configOverrides configuration overrides
     * @return configured {@code Serializer}
     * @param <T> type to be (de-)serialized
     */
    public <T> Serializer<T> configureForValues(final Serializer<T> serializer,
            final Map<String, Object> configOverrides) {
        return this.configureForValues(create(serializer, configOverrides));
    }

    /**
     * Configure a {@code Serializer} for keys using {@link #kafkaProperties}
     * @param serializer serializer to configure
     * @return configured {@code Serializer}
     * @param <T> type to be (de-)serialized
     */
    public <T> Serializer<T> configureForKeys(final Serializer<T> serializer) {
        return this.configureForKeys(create(serializer));
    }

    /**
     * Configure a {@code Serializer} for keys using {@link #kafkaProperties} and config overrides
     * @param serializer serializer to configure
     * @param configOverrides configuration overrides
     * @return configured {@code Serializer}
     * @param <T> type to be (de-)serialized
     */
    public <T> Serializer<T> configureForKeys(final Serializer<T> serializer,
            final Map<String, Object> configOverrides) {
        return this.configureForKeys(create(serializer, configOverrides));
    }

    /**
     * Configure a {@code Preconfigured} for values object using {@link #kafkaProperties}
     * @param preconfigured pre-configured {@link Serde} or {@link Serializer}
     * @return configured instance
     * @param <T> type of configured instance
     */
    public <T> T configureForValues(final Preconfigured<T> preconfigured) {
        return preconfigured.configureForValues(this.kafkaProperties);
    }

    /**
     * Configure a {@code Preconfigured} for keys object using {@link #kafkaProperties}
     * @param preconfigured pre-configured {@link Serde} or {@link Serializer}
     * @return configured instance
     * @param <T> type of configured instance
     */
    public <T> T configureForKeys(final Preconfigured<T> preconfigured) {
        return preconfigured.configureForKeys(this.kafkaProperties);
    }

}
