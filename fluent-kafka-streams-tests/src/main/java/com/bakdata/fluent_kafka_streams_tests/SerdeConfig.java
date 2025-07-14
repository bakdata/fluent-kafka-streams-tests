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
import com.bakdata.kafka.Preconfigured;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Serde;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
final class SerdeConfig<K, V> {
    @Getter
    private final Serde<K> keySerde;
    @Getter
    private final Serde<V> valueSerde;
    private final Serde<?> defaultKeySerde;
    private final Serde<?> defaultValueSerde;
    private final Configurator configurator;

    static <K, V> SerdeConfig<K, V> create(@NonNull final Serde<K> keySerde, @NonNull final Serde<V> valueSerde,
            final Configurator configurator) {
        return new SerdeConfig<>(keySerde, valueSerde, keySerde, valueSerde, configurator);
    }

    <KR, VR> SerdeConfig<KR, VR> withSerde(final Serde<KR> keySerde, final Serde<VR> valueSerde) {
        final Serde<KR> newKeySerde = keySerde == null ? this.getDefaultKeySerde() : keySerde;
        final Serde<VR> newValueSerde =
                valueSerde == null ? this.getDefaultValueSerde() : valueSerde;
        return new SerdeConfig<>(newKeySerde, newValueSerde, this.defaultKeySerde, this.defaultValueSerde,
                this.configurator);
    }

    <KR, VR> SerdeConfig<KR, VR> configureWithSerde(final Preconfigured<? extends Serde<KR>> keySerde,
            final Preconfigured<? extends Serde<VR>> valueSerde) {
        return this.withSerde(this.configureForKeys(keySerde), this.configureForValues(valueSerde));
    }

    <KR, VR> SerdeConfig<KR, VR> configureWithSerde(final Serde<KR> keySerde, final Serde<VR> valueSerde) {
        return this.configureWithSerde(Preconfigured.create(keySerde), Preconfigured.create(valueSerde));
    }

    <KR> SerdeConfig<KR, V> withKeySerde(final Serde<KR> keySerde) {
        return this.withSerde(keySerde, this.valueSerde);
    }

    <KR> SerdeConfig<KR, V> configureWithKeySerde(final Preconfigured<? extends Serde<KR>> keySerde) {
        return this.withSerde(this.configureForKeys(keySerde), this.valueSerde);
    }

    <KR> SerdeConfig<KR, V> configureWithKeySerde(final Serde<KR> keySerde) {
        return this.configureWithKeySerde(Preconfigured.create(keySerde));
    }

    <VR> SerdeConfig<K, VR> withValueSerde(final Serde<VR> valueSerde) {
        return this.withSerde(this.keySerde, valueSerde);
    }

    <VR> SerdeConfig<K, VR> configureWithValueSerde(final Preconfigured<? extends Serde<VR>> valueSerde) {
        return this.withSerde(this.keySerde, this.configureForValues(valueSerde));
    }

    <VR> SerdeConfig<K, VR> configureWithValueSerde(final Serde<VR> valueSerde) {
        return this.configureWithValueSerde(Preconfigured.create(valueSerde));
    }

    <KR, VR> SerdeConfig<KR, VR> withTypes(final Class<KR> keyType, final Class<VR> valueType) {
        return (SerdeConfig<KR, VR>) this;
    }

    <KR> SerdeConfig<KR, V> withKeyType(final Class<KR> keyType) {
        return (SerdeConfig<KR, V>) this;
    }

    <VR> SerdeConfig<K, VR> withValueType(final Class<VR> valueType) {
        return (SerdeConfig<K, VR>) this;
    }

    private <KR> Serde<KR> configureForKeys(final Preconfigured<? extends Serde<KR>> keySerde) {
        return this.configurator.configureForKeys(keySerde);
    }

    private <VR> Serde<VR> configureForValues(final Preconfigured<? extends Serde<VR>> valueSerde) {
        return this.configurator.configureForValues(valueSerde);
    }

    private <KR> Serde<KR> getDefaultKeySerde() {
        return (Serde<KR>) this.defaultKeySerde;
    }

    private <VR> Serde<VR> getDefaultValueSerde() {
        return (Serde<VR>) this.defaultValueSerde;
    }
}
