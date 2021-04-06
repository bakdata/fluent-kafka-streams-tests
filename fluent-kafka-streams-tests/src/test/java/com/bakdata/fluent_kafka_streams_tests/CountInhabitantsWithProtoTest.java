package com.bakdata.fluent_kafka_streams_tests;

import static com.bakdata.fluent_kafka_streams_tests.test_types.proto.CityOuterClass.City;
import static com.bakdata.fluent_kafka_streams_tests.test_types.proto.PersonOuterClass.Person;

import com.bakdata.fluent_kafka_streams_tests.test_applications.CountInhabitantsWithProto;
import com.bakdata.schemaregistrymock.SchemaRegistryMock;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import java.util.List;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class CountInhabitantsWithProtoTest {

    private final CountInhabitantsWithProto app = new CountInhabitantsWithProto();

    private final List<SchemaProvider> listSchemaProviders = List.of(new ProtobufSchemaProvider());
    private final TestTopology<Object, Object> testTopology =
            new TestTopology<>(this.app::getTopology, this.app.getKafkaProperties(), this.listSchemaProviders);

    static Person newPerson(final String name, final String city) {
        return Person.newBuilder().setName(name).setCity(city).build();
    }

    static City newCity(final String name, final int inhabitants) {
        return City.newBuilder().setName(name).setInhabitants(inhabitants).build();
    }

    @BeforeEach
    void start() { this.testTopology.start(); }

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
}
