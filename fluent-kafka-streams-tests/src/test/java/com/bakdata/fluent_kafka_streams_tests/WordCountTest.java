package com.bakdata.fluent_kafka_streams_tests;

import com.bakdata.fluent_kafka_streams_tests.testutils.WordCount;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class WordCountTest {
    private final WordCount app = new WordCount();

    @RegisterExtension
    TestTopology<Object, String> testTopology = new TestTopology<>(app::getTopology, app.getKafkaProperties());

    @Test
    void shouldAggregateSameWordStream() {
        testTopology.input().add("bla")
                .add("blub")
                .add("bla");

        testTopology.streamOutput().withSerde(Serdes.String(), Serdes.Long())
                .expectNextRecord().hasKey("bla").hasValue(1L)
                .expectNextRecord().hasKey("blub").hasValue(1L)
                .expectNextRecord().hasKey("bla").hasValue(2L)
                .expectNoMoreRecord();
    }

    @Test
    void shouldAggregateSameWordTable() {
        testTopology.input().add("bla")
                .add("blub")
                .add("bla");
        testTopology.tableOutput().withSerde(Serdes.String(), Serdes.Long())
                .expectNextRecord().hasKey("bla").hasValue(2L)
                .expectNextRecord().hasKey("blub").hasValue(1L)
                .expectNoMoreRecord();
    }

    @Test
    void shouldNotAggregateDifferentWordsStream() {
        testTopology.input().add("bla")
                .add("foo")
                .add("blub");

        testTopology.streamOutput().withSerde(Serdes.String(), Serdes.Long())
                .expectNextRecord().hasKey("bla").hasValue(1L)
                .expectNextRecord().hasKey("foo").hasValue(1L)
                .expectNextRecord().hasKey("blub").hasValue(1L)
                .expectNoMoreRecord();
    }

    @Test
    void shouldNotAggregateDifferentWordsTable() {
        testTopology.input().add("bla")
                .add("foo")
                .add("blub");

        testTopology.tableOutput().withSerde(Serdes.String(), Serdes.Long())
                .expectNextRecord().hasKey("bla").hasValue(1L)
                .expectNextRecord().hasKey("foo").hasValue(1L)
                .expectNextRecord().hasKey("blub").hasValue(1L)
                .expectNoMoreRecord();
    }

    @Test
    void shouldReturnNoInputAndOutputStream() {
        testTopology.streamOutput().withSerde(Serdes.String(), Serdes.Long())
                .expectNoMoreRecord();
    }

    @Test
    void shouldReturnNoInputAndOutputTable() {
        testTopology.tableOutput().withSerde(Serdes.String(), Serdes.Long())
                .expectNoMoreRecord();
    }

    @Test
    void shouldAggregateSameWordOrderTable() {
        testTopology.input().add("blub") // 1 blub
                .add("bla") // 1 bla
                .add("blub") // 2 blub
                .add("blub") // 3 blub
                .add("bla") // 2 bla
                .add("blub") // 4 blub
                .add("bla") // 3 bla
                .add("bla") // 4 bla
                .add("blub") // 5 blub
                .add("bla") // 5 bla
                .add("bla") // 6 bla
                .add("bla"); // 7 bla

        testTopology.tableOutput(app.getOutputTopic()).withSerde(Serdes.String(), Serdes.Long())
                .expectNextRecord().hasKey("blub").hasValue(5L)
                .expectNextRecord().hasKey("bla").hasValue(7L)
                .expectNoMoreRecord();
    }

    @Test
    void shouldReturnSingleInputAndOutputStream() {
        testTopology.input().add("bla");

        testTopology.streamOutput().withSerde(Serdes.String(), Serdes.Long())
                .expectNextRecord().hasKey("bla").hasValue(1L)
                .expectNoMoreRecord();
    }

    @Test
    void shouldReturnSingleInputAndOutputTable() {
        testTopology.input().add("bla");

        testTopology.tableOutput().withSerde(Serdes.String(), Serdes.Long())
                .expectNextRecord().hasKey("bla").hasValue(1L)
                .expectNoMoreRecord();
    }

    @Test
    void shouldReturnCorrectIteratorStream() {
        testTopology.input().add("bla")
                .add("blub")
                .add("foo");
        List<String> expected = List.of("bla", "blub", "foo");

        assertThat(testTopology.tableOutput().withSerde(Serdes.String(), Serdes.Long()).iterator())
                .extracting(ProducerRecord::key)
                .containsAll(expected);
    }

    @Test
    void shouldReturnCorrectIteratorTable() {
        testTopology.input().add("bla")
                .add("blub")
                .add("foo");
        List<String> expected = List.of("bla", "blub", "foo");

        assertThat(testTopology.tableOutput().withSerde(Serdes.String(), Serdes.Long()).iterator())
                .extracting(ProducerRecord::key)
                .containsAll(expected);
    }
}
