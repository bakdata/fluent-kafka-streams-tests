package com.bakdata.fluent_kafka_streams_tests;

import com.bakdata.fluent_kafka_streams_tests.test_applications.NameJoinGlobalKTable;
import com.bakdata.fluent_kafka_streams_tests.test_types.Person;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class NameJoinTest {
    private final NameJoinGlobalKTable app = new NameJoinGlobalKTable();

    private final TestTopology<String, Person> testTopology =
            new TestTopology<>(this.app::getTopology, this.app.getKafkaProperties());

    @BeforeEach
    void start() {
        this.testTopology.start();
    }

    @AfterEach
    void stop() {
        this.testTopology.stop();
    }

    @Test
    void testTopology() {
        this.testTopology.input(NameJoinGlobalKTable.NAME_INPUT).withSerde(Serdes.Long(), Serdes.String())
                .add(1L, "Robinson")
                .add(2L, "Walker");

        this.testTopology.input(NameJoinGlobalKTable.INPUT_TOPIC).withSerde(Serdes.Long(), Serdes.Long())
                .add(1L, 1L)
                .add(2L, 2L);

        this.testTopology.streamOutput(NameJoinGlobalKTable.OUTPUT_TOPIC).withSerde(Serdes.Long(), Serdes.String())
                .expectNextRecord().hasKey(1L).hasValue("Robinson")
                .expectNextRecord().hasKey(2L).hasValue("Walker")
                .expectNoMoreRecord();
    }

}