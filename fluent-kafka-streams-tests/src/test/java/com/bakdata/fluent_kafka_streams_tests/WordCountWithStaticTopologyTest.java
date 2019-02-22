package com.bakdata.fluent_kafka_streams_tests;

import com.bakdata.fluent_kafka_streams_tests.test_applications.WordCount;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class WordCountWithStaticTopologyTest {
    private final WordCount app = new WordCount();

    @RegisterExtension
    final TestTopology<Object, String> testTopology = new TestTopology<>(this.app.getTopology(),
        this.app.getKafkaProperties());

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
