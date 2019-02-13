package com.bakdata.fluent_kafka_streams_tests;

import com.bakdata.fluent_kafka_streams_tests.testutils.WordCount;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

class WordCountTest {
    private final WordCount app = new WordCount();

    @RegisterExtension
    TestTopology<Object, String> testTopology = new TestTopology<>(app::getTopology, app.getKafkaProperties());

    @Test
    void shouldAggregateSameWordStream() {
        testTopology.input(app.getInputTopic()).add("bla");
        testTopology.input(app.getInputTopic()).add("blub");
        testTopology.input(app.getInputTopic()).add("bla");

        // Check Stream semantics
        testTopology.streamOutput(app.getOutputTopic()).withSerde(Serdes.String(), Serdes.Long())
                .expectNextRecord().hasKey("bla").hasValue(1L)
                .expectNextRecord().hasKey("blub").hasValue(1L)
                .expectNextRecord().hasKey("bla").hasValue(2L)
                .expectNoMoreRecord();
    }


    @Test
    void shouldAggregateSameWordTable() {
        testTopology.input(app.getInputTopic()).add("bla");
        testTopology.input(app.getInputTopic()).add("blub");
        testTopology.input(app.getInputTopic()).add("bla");

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

    @Test
    void shouldReturnCorrectIteratorStream() {
        testTopology.input().add("bla");
        testTopology.input().add("blub");
        testTopology.input().add("foo");
        List<String> expected = List.of("bla", "blub", "foo");

        // Check Stream semantics
        List<String> actual = StreamSupport
                .stream(testTopology.streamOutput().withSerde(Serdes.String(), Serdes.Long()).spliterator(), false)
                .map(ProducerRecord::key)
                .collect(Collectors.toList());
        assertEquals(expected, actual);
    }

    @Test
    void shouldReturnCorrectIteratorTable() {
        testTopology.input().add("bla");
        testTopology.input().add("blub");
        testTopology.input().add("foo");
        List<String> expected = List.of("bla", "blub", "foo");

        assertThat(testTopology.tableOutput().withSerde(Serdes.String(), Serdes.Long()).iterator())
                .extracting(ProducerRecord::key)
                .containsAll(expected);
    }
}
