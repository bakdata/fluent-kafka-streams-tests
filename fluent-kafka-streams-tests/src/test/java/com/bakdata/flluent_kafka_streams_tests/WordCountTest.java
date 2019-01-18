package com.bakdata.flluent_kafka_streams_tests;

import com.bakdata.fluent_kafka_streams_tests.TestTopology;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import testutils.WordCount;

public class WordCountTest {
    private WordCount app = new WordCount();

    @RegisterExtension
    TestTopology<Object, String> testTopology = TestTopology.<Object, String>builder()
            .properties(app.getKafkaProperties())
            .topologyFactory(app::getTopology)
            .build();

    @Test
    public void shouldAggregateSameWordStream() {
        testTopology.input(app.getInputTopic()).create("bla");
        testTopology.input(app.getInputTopic()).create("bla");
        testTopology.input(app.getInputTopic()).create("blub");

        // Check Stream semantics
        testTopology.streamOutput(app.getOutputTopic()).withSerde(Serdes.String(), Serdes.Long())
                .expectNextRecord().hasKey("bla").hasValue(1L)
                .expectNextRecord().hasKey("bla").hasValue(2L)
                .expectNextRecord().hasKey("blub").hasValue(1L)
                .expectNoMoreRecord();
    }


    @Test
    public void shouldAggregateSameWordTable() {
        testTopology.input(app.getInputTopic()).create("bla");
        testTopology.input(app.getInputTopic()).create("bla");
        testTopology.input(app.getInputTopic()).create("blub");

        // Check Table semantics
        testTopology.tableOutput(app.getOutputTopic()).withSerde(Serdes.String(), Serdes.Long())
                .expectNextRecord().hasKey("bla").hasValue(2L)
                .expectNextRecord().hasKey("blub").hasValue(1L)
                .expectNoMoreRecord();
    }

    @Test
    public void shouldNotAggregateDifferentWordsStream() {
        testTopology.input(app.getInputTopic()).create("bla");
        testTopology.input(app.getInputTopic()).create("foo");
        testTopology.input(app.getInputTopic()).create("blub");

        // Check Stream semantics
        testTopology.streamOutput(app.getOutputTopic()).withSerde(Serdes.String(), Serdes.Long())
                .expectNextRecord().hasKey("bla").hasValue(1L)
                .expectNextRecord().hasKey("foo").hasValue(1L)
                .expectNextRecord().hasKey("blub").hasValue(1L)
                .expectNoMoreRecord();
    }

    @Test
    public void shouldNotAggregateDifferentWordsTable() {
        testTopology.input(app.getInputTopic()).create("bla");
        testTopology.input(app.getInputTopic()).create("foo");
        testTopology.input(app.getInputTopic()).create("blub");

        // Check Table semantics
        testTopology.tableOutput(app.getOutputTopic()).withSerde(Serdes.String(), Serdes.Long())
                .expectNextRecord().hasKey("bla").hasValue(1L)
                .expectNextRecord().hasKey("foo").hasValue(1L)
                .expectNextRecord().hasKey("blub").hasValue(1L)
                .expectNoMoreRecord();
    }

    @Test
    public void shouldAggregateSameWordOrderTable() {
        testTopology.input(app.getInputTopic()).create("blub");
        testTopology.input(app.getInputTopic()).create("bla");
        testTopology.input(app.getInputTopic()).create("blub");
        testTopology.input(app.getInputTopic()).create("blub");
        testTopology.input(app.getInputTopic()).create("bla");
        testTopology.input(app.getInputTopic()).create("blub");
        testTopology.input(app.getInputTopic()).create("bla");
        testTopology.input(app.getInputTopic()).create("bla");
        testTopology.input(app.getInputTopic()).create("blub");
        testTopology.input(app.getInputTopic()).create("bla");
        testTopology.input(app.getInputTopic()).create("bla");
        testTopology.input(app.getInputTopic()).create("bla");

        // Check Table semantics
        testTopology.tableOutput(app.getOutputTopic()).withSerde(Serdes.String(), Serdes.Long())
                .expectNextRecord().hasKey("blub").hasValue(5L)
                .expectNextRecord().hasKey("bla").hasValue(7L)
                .expectNoMoreRecord();
    }
}
