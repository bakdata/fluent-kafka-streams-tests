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

import static com.bakdata.fluent_kafka_streams_tests.test_applications.MirrorAvroNonDefaultSerde.getKeySerde;
import static com.bakdata.fluent_kafka_streams_tests.test_applications.MirrorAvroNonDefaultSerde.getValueSerde;

import com.bakdata.fluent_kafka_streams_tests.test_applications.MirrorAvroNonDefaultSerde;
import com.bakdata.fluent_kafka_streams_tests.test_types.City;
import com.bakdata.fluent_kafka_streams_tests.test_types.Person;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


class MirrorAvroNonDefaultSerdeTest {

    private final TestTopology<String, String> testTopology =
            new TestTopology<>(MirrorAvroNonDefaultSerde::getTopology, MirrorAvroNonDefaultSerde.getKafkaProperties());

    @BeforeEach
    void start() {
        this.testTopology.start();
    }

    @AfterEach
    void stop() {
        this.testTopology.stop();
    }

    @Test
    void shouldConfigurePreconfiguredSerdes() {
        this.testTopology.input()
                .configureWithSerde(getKeySerde(), getValueSerde())
                .add(new City("City1", 2), new Person("Huey", "City1"));

        this.testTopology.streamOutput()
                .configureWithSerde(getKeySerde(), getValueSerde())
                .expectNextRecord().hasKey(new City("City1", 2)).hasValue(new Person("Huey", "City1"))
                .expectNoMoreRecord();
    }

    @Test
    void shouldConfigureSerdes() {
        this.testTopology.input()
                .configureWithSerde(new SpecificAvroSerde<>(), new SpecificAvroSerde<>())
                .add(new City("City1", 2), new Person("Huey", "City1"));

        this.testTopology.streamOutput()
                .configureWithSerde(new SpecificAvroSerde<>(), new SpecificAvroSerde<>())
                .expectNextRecord().hasKey(new City("City1", 2)).hasValue(new Person("Huey", "City1"))
                .expectNoMoreRecord();
    }

    @Test
    void shouldConfigurePreconfiguredKeyAndValueSerdes() {
        this.testTopology.input()
                .configureWithKeySerde(getKeySerde())
                .configureWithValueSerde(getValueSerde())
                .add(new City("City1", 2), new Person("Huey", "City1"));

        this.testTopology.streamOutput()
                .configureWithKeySerde(getKeySerde())
                .configureWithValueSerde(getValueSerde())
                .expectNextRecord().hasKey(new City("City1", 2)).hasValue(new Person("Huey", "City1"))
                .expectNoMoreRecord();
    }

    @Test
    void shouldConfigureKeyAndValueSerdes() {
        this.testTopology.input()
                .configureWithKeySerde(new SpecificAvroSerde<>())
                .configureWithValueSerde(new SpecificAvroSerde<>())
                .add(new City("City1", 2), new Person("Huey", "City1"));

        this.testTopology.streamOutput()
                .configureWithKeySerde(new SpecificAvroSerde<>())
                .configureWithValueSerde(new SpecificAvroSerde<>())
                .expectNextRecord().hasKey(new City("City1", 2)).hasValue(new Person("Huey", "City1"))
                .expectNoMoreRecord();
    }
}
