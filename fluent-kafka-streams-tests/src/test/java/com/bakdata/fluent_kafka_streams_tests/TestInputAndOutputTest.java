/*
 * The MIT License
 *
 * Copyright (c) 2019 bakdata GmbH
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

import com.bakdata.fluent_kafka_streams_tests.test_applications.MirrorAvro;
import com.bakdata.fluent_kafka_streams_tests.test_types.City;
import com.bakdata.fluent_kafka_streams_tests.test_types.Person;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


class TestInputAndOutputTest {

    private final TestTopology<Person, City> testTopology =
            new TestTopology<>(MirrorAvro::getTopology, MirrorAvro.getKafkaProperties());

    @BeforeEach
    void start() {
        this.testTopology.start();
    }

    @AfterEach
    void stop() {
        this.testTopology.stop();
    }

    @Test
    void shouldUseTypes() {
        this.testTopology.input()
                .withTypes(City.class, Person.class)
                .add(new City("City1", 2), new Person("Huey", "City1"));

        this.testTopology.streamOutput()
                .withTypes(City.class, Person.class)
                .expectNextRecord().hasKey(new City("City1", 2)).hasValue(new Person("Huey", "City1"))
                .expectNoMoreRecord();
    }

    @Test
    void shouldUseValueTypes() {
        this.testTopology.input()
                .withValueType(Person.class)
                .add(new Person("Huey", "City1"), new Person("Huey", "City1"));

        this.testTopology.streamOutput()
                .withValueType(Person.class)
                .expectNextRecord().hasKey(new Person("Huey", "City1")).hasValue(new Person("Huey", "City1"))
                .expectNoMoreRecord();
    }

    @Test
    void shouldUseKeyTypes() {
        this.testTopology.input()
                .withKeyType(City.class)
                .add(new City("City1", 2), new City("City1", 2));

        this.testTopology.streamOutput()
                .withKeyType(City.class)
                .expectNextRecord().hasKey(new City("City1", 2)).hasValue(new City("City1", 2))
                .expectNoMoreRecord();
    }
}
