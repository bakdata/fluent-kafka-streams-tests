package com.bakdata.fluent_kafka_streams_tests;

import com.bakdata.fluent_kafka_streams_tests.test_applications.WordCount;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class WordCountWithStaticTopologyTest {
    private final WordCount app = new WordCount();

    final TestTopology<Object, String> testTopology =
            new TestTopology<>(this.app::getTopology, this.app.getKafkaProperties());

    @BeforeEach
    void start() {
        testTopology.start();
    }

    @AfterEach
    void stop() {
        testTopology.stop();
    }

    @Test
    void shouldAggregateSameWordStream() {
        this.testTopology.input().add("bla")
            .add("blub")
            .add("bla");

        this.testTopology.streamOutput().withSerde(Serdes.String(), Serdes.Long())
            .expectNextRecord().hasKey("bla").hasValue(1L)
            .expectNextRecord().hasKey("blub").hasValue(1L)
            .expectNextRecord().hasKey("bla").hasValue(2L)
            .expectNoMoreRecord();
    }
}
