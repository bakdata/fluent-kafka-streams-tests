package com.bakdata.flluent_kafka_streams_tests;

import com.bakdata.fluent_kafka_streams_tests.TestTopology;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import testutils.WordCount;

class WordCountTest {
    private WordCount app = new WordCount();

    @RegisterExtension
    TestTopology<Object, String> testTopology = new TestTopology<>(app::getTopology, app.getKafkaProperties());

    @Test
    void shouldAggregateSameWordStream() {
        testTopology.input(app.getInputTopic()).add("bla");
        testTopology.input(app.getInputTopic()).add("bla");
        testTopology.input(app.getInputTopic()).add("blub");

        // Check Stream semantics
        testTopology.streamOutput(app.getOutputTopic()).withSerde(Serdes.String(), Serdes.Long())
                .expectNextRecord().hasKey("bla").hasValue(1L)
                .expectNextRecord().hasKey("bla").hasValue(2L)
                .expectNextRecord().hasKey("blub").hasValue(1L)
                .expectNoMoreRecord();
    }


    @Test
    void shouldAggregateSameWordTable() {
        testTopology.input(app.getInputTopic()).add("bla");
        testTopology.input(app.getInputTopic()).add("bla");
        testTopology.input(app.getInputTopic()).add("blub");

        // Check Table semantics
        testTopology.tableOutput(app.getOutputTopic()).withSerde(Serdes.String(), Serdes.Long())
                .expectNextRecord().hasKey("bla").hasValue(2L)
                .expectNextRecord().hasKey("blub").hasValue(1L)
                .expectNoMoreRecord();
    }

    @Test
    void shouldNotAggregateDifferentWordsStream() {
        testTopology.input(app.getInputTopic()).add("bla");
        testTopology.input(app.getInputTopic()).add("foo");
        testTopology.input(app.getInputTopic()).add("blub");

        // Check Stream semantics
        testTopology.streamOutput(app.getOutputTopic()).withSerde(Serdes.String(), Serdes.Long())
                .expectNextRecord().hasKey("bla").hasValue(1L)
                .expectNextRecord().hasKey("foo").hasValue(1L)
                .expectNextRecord().hasKey("blub").hasValue(1L)
                .expectNoMoreRecord();
    }

    @Test
    void shouldNotAggregateDifferentWordsTable() {
        testTopology.input(app.getInputTopic()).add("bla");
        testTopology.input(app.getInputTopic()).add("foo");
        testTopology.input(app.getInputTopic()).add("blub");

        // Check Table semantics
        testTopology.tableOutput(app.getOutputTopic()).withSerde(Serdes.String(), Serdes.Long())
                .expectNextRecord().hasKey("bla").hasValue(1L)
                .expectNextRecord().hasKey("foo").hasValue(1L)
                .expectNextRecord().hasKey("blub").hasValue(1L)
                .expectNoMoreRecord();
    }

    @Test
    void shouldAggregateSameWordOrderTable() {
        testTopology.input(app.getInputTopic()).add("blub");
        testTopology.input(app.getInputTopic()).add("bla");
        testTopology.input(app.getInputTopic()).add("blub");
        testTopology.input(app.getInputTopic()).add("blub");
        testTopology.input(app.getInputTopic()).add("bla");
        testTopology.input(app.getInputTopic()).add("blub");
        testTopology.input(app.getInputTopic()).add("bla");
        testTopology.input(app.getInputTopic()).add("bla");
        testTopology.input(app.getInputTopic()).add("blub");
        testTopology.input(app.getInputTopic()).add("bla");
        testTopology.input(app.getInputTopic()).add("bla");
        testTopology.input(app.getInputTopic()).add("bla");

        // Check Table semantics
        testTopology.tableOutput(app.getOutputTopic()).withSerde(Serdes.String(), Serdes.Long())
                .expectNextRecord().hasKey("blub").hasValue(5L)
                .expectNextRecord().hasKey("bla").hasValue(7L)
                .expectNoMoreRecord();
    }

    @Test
    void shouldReturnSingleInputAndOutputStream() {
        testTopology.input().add("bla");

        // Check Stream semantics
        testTopology.streamOutput().withSerde(Serdes.String(), Serdes.Long())
                .expectNextRecord().hasKey("bla").hasValue(1L)
                .expectNoMoreRecord();
    }

    @Test
    void shouldReturnSingleInputAndOutputTable() {
        testTopology.input().add("bla");

        // Check Table semantics
        testTopology.tableOutput().withSerde(Serdes.String(), Serdes.Long())
                .expectNextRecord().hasKey("bla").hasValue(1L)
                .expectNoMoreRecord();
    }
}
