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
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Serde;

@RequiredArgsConstructor
class SerdeConfig {
    private final Serde<?> defaultKeySerde;
    private final Serde<?> defaultValueSerde;
    private final Configurator configurator;

    public <KR> Serde<KR> configureForKeys(final Preconfigured<? extends Serde<KR>> keySerde) {
        return this.configurator.configureForKeys(keySerde);
    }

    public <VR> Serde<VR> configureForValues(final Preconfigured<? extends Serde<VR>> valueSerde) {
        return this.configurator.configureForValues(valueSerde);
    }

    <KR> Serde<KR> getDefaultKeySerde() {
        return (Serde<KR>) this.defaultKeySerde;
    }

    <VR> Serde<VR> getDefaultValueSerde() {
        return (Serde<VR>) this.defaultValueSerde;
    }
}
