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

import static com.bakdata.fluent_kafka_streams_tests.test_types.proto.CityOuterClass.City;
import static com.bakdata.fluent_kafka_streams_tests.test_types.proto.PersonOuterClass.Person;

import com.bakdata.fluent_kafka_streams_tests.test_applications.CountInhabitantsWithProto;
import java.util.Map;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CountInhabitantsWithProtoTest {

    private final CountInhabitantsWithProto app = new CountInhabitantsWithProto();

    private final TestTopology<Object, Object> testTopology =
            new TestTopology<>(this.app::getTopology, this::properties);

    static Person newPerson(final String name, final String city) {
        return Person.newBuilder().setName(name).setCity(city).build();
    }

    static City newCity(final String name, final int inhabitants) {
        return City.newBuilder().setName(name).setInhabitants(inhabitants).build();
    }

    @BeforeEach
    void start() {
        this.testTopology.start();
    }

    @AfterEach
    void stop() {
        this.testTopology.stop();
    }

    @Test
    void shouldAggregateInhabitants() {
        this.testTopology.input()
                .withValueSerde(this.app.newPersonSerde())
                .add("test", newPerson("Huey", "City1"))
                .add("test", newPerson("Dewey", "City2"))
                .add("test", newPerson("Louie", "City1"));

        this.testTopology.tableOutput().withValueSerde(this.app.newCitySerde())
                .expectNextRecord().hasKey("City1").hasValue(newCity("City1", 2))
                .expectNextRecord().hasKey("City2").hasValue(newCity("City2", 1))
                .expectNoMoreRecord();
    }

    @Test
    void shouldWorkForEmptyInput() {
        this.testTopology.tableOutput().withSerde(Serdes.String(), Serdes.Long())
                .expectNoMoreRecord();
    }

    @Test
    void shouldGetSchemaRegistryClient() {
        Assertions.assertNotNull(this.testTopology.getSchemaRegistry());
    }

    private Map<String, Object> properties(final String schemaRegistryUrl) {
        this.app.setSchemaRegistryUrl(schemaRegistryUrl);
        return this.app.getKafkaProperties();
    }
}
