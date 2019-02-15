package com.bakdata.fluent_kafka_streams_tests;

import com.bakdata.fluent_kafka_streams_tests.test_applications.WordCount;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class WordCountTest {
    private final WordCount app = new WordCount();

    @RegisterExtension
    final TestTopology<Object, String> testTopology = new TestTopology<>(this.app::getTopology, this.app.getKafkaProperties());

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

    @Test
    void shouldAggregateSameWordTable() {
        this.testTopology.input().add("bla")
                .add("blub")
                .add("bla");
        this.testTopology.tableOutput().withSerde(Serdes.String(), Serdes.Long())
                .expectNextRecord().hasKey("bla").hasValue(2L)
                .expectNextRecord().hasKey("blub").hasValue(1L)
                .expectNoMoreRecord();
    }

    @Test
    void shouldNotAggregateDifferentWordsStream() {
        this.testTopology.input().add("bla")
                .add("foo")
                .add("blub");

        this.testTopology.streamOutput().withSerde(Serdes.String(), Serdes.Long())
                .expectNextRecord().hasKey("bla").hasValue(1L)
                .expectNextRecord().hasKey("foo").hasValue(1L)
                .expectNextRecord().hasKey("blub").hasValue(1L)
                .expectNoMoreRecord();
    }

    @Test
    void shouldNotAggregateDifferentWordsTable() {
        this.testTopology.input().add("bla")
                .add("foo")
                .add("blub");

        this.testTopology.tableOutput().withSerde(Serdes.String(), Serdes.Long())
                .expectNextRecord().hasKey("bla").hasValue(1L)
                .expectNextRecord().hasKey("foo").hasValue(1L)
                .expectNextRecord().hasKey("blub").hasValue(1L)
                .expectNoMoreRecord();
    }

    @Test
    void shouldReturnNoInputAndOutputStream() {
        this.testTopology.streamOutput().withSerde(Serdes.String(), Serdes.Long())
                .expectNoMoreRecord();
    }

    @Test
    void shouldReturnNoInputAndOutputTable() {
        this.testTopology.tableOutput().withSerde(Serdes.String(), Serdes.Long())
                .expectNoMoreRecord();
    }

    @Test
    void shouldAggregateSameWordOrderTable() {
        this.testTopology.input().add("blub") // 1 blub
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

        this.testTopology.tableOutput(this.app.getOutputTopic()).withSerde(Serdes.String(), Serdes.Long())
                .expectNextRecord().hasKey("blub").hasValue(5L)
                .expectNextRecord().hasKey("bla").hasValue(7L)
                .expectNoMoreRecord();
    }

    @Test
    void shouldReturnSingleInputAndOutputStream() {
        this.testTopology.input().add("bla");

        this.testTopology.streamOutput().withSerde(Serdes.String(), Serdes.Long())
                .expectNextRecord().hasKey("bla").hasValue(1L)
                .expectNoMoreRecord();
    }

    @Test
    void shouldReturnSingleInputAndOutputTable() {
        this.testTopology.input().add("bla");

        this.testTopology.tableOutput().withSerde(Serdes.String(), Serdes.Long())
                .expectNextRecord().hasKey("bla").hasValue(1L)
                .expectNoMoreRecord();
    }

    @Test
    void shouldReturnCorrectIteratorStream() {
        this.testTopology.input().add("bla")
                .add("blub")
                .add("foo");
        final List<String> expected = List.of("bla", "blub", "foo");

        assertThat(this.testTopology.tableOutput().withSerde(Serdes.String(), Serdes.Long()).iterator())
                .extracting(ProducerRecord::key)
                .containsAll(expected);
    }

    @Test
    void shouldReturnCorrectIteratorTable() {
        this.testTopology.input().add("bla")
                .add("blub")
                .add("foo");
        final List<String> expected = List.of("bla", "blub", "foo");

        assertThat(this.testTopology.tableOutput().withSerde(Serdes.String(), Serdes.Long()).iterator())
                .extracting(ProducerRecord::key)
                .containsAll(expected);
    }
}
