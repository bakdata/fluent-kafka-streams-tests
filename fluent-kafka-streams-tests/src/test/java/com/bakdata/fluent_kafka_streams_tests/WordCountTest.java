package com.bakdata.fluent_kafka_streams_tests;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.bakdata.fluent_kafka_streams_tests.test_applications.WordCount;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class WordCountTest {
    private final WordCount app = new WordCount();

    private final TestTopology<Object, String> testTopology = new TestTopology<>(this.app::getTopology,
        this.app.getKafkaProperties());

    @BeforeEach
    void start() {
        this.testTopology.start();
    }

    @AfterEach
    void stop() {
        this.testTopology.stop();
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
            .add("bla")
            .add("foo");
        final List<String> expected = List.of("bla", "blub", "bla", "foo");

        assertThat(this.testTopology.streamOutput().withSerde(Serdes.String(), Serdes.Long()))
            .extracting(ProducerRecord::key)
            .containsAll(expected);
    }

    @Test
    void shouldReturnCorrectIteratorExplicitStream() {
        this.testTopology.input().add("bla")
            .add("blub")
            .add("bla")
            .add("foo");
        final List<String> expected = List.of("bla", "blub", "bla", "foo");

        assertThat(this.testTopology.streamOutput().withSerde(Serdes.String(), Serdes.Long()).iterator())
            .extracting(ProducerRecord::key)
            .containsAll(expected);
    }

    @Test
    void shouldReturnCorrectIteratorTable() {
        this.testTopology.input().add("bla")
            .add("blub")
            .add("bla")
            .add("foo");
        final List<String> expected = List.of("bla", "blub", "foo");

        assertThat(this.testTopology.tableOutput().withSerde(Serdes.String(), Serdes.Long()))
            .extracting(ProducerRecord::key)
            .containsAll(expected);
    }

    @Test
    void shouldFailCorrectIteratorStreamNoHasNextCheck() {
        final Iterator<ProducerRecord<String, Long>> output = this.testTopology.streamOutput()
            .withSerde(Serdes.String(), Serdes.Long()).iterator();
        assertThrows(NoSuchElementException.class, output::next);
    }

    @Test
    void shouldFailCorrectIteratorTableNoHasNextCheck() {
        final Iterator<ProducerRecord<String, Long>> output = this.testTopology.tableOutput()
            .withSerde(Serdes.String(), Serdes.Long()).iterator();
        assertThrows(NoSuchElementException.class, output::next);
    }

    @Test
    void shouldReturnCorrectIteratorExplicitTable() {
        this.testTopology.input().add("bla")
            .add("blub")
            .add("bla")
            .add("foo");
        final List<String> expected = List.of("bla", "blub", "foo");

        assertThat(this.testTopology.tableOutput().withSerde(Serdes.String(), Serdes.Long()).iterator())
            .extracting(ProducerRecord::key)
            .containsAll(expected);
    }

    @Test
    void shouldWorkOnTableToStream() {
        this.testTopology.input()
            .add("bla")
            .add("blub")
            .add("bla");

        // Unnecessary conversion between table and stream to check that nothing breaks
        this.testTopology.streamOutput().asTable().asStream()
            .withSerde(Serdes.String(), Serdes.Long())
            .expectNextRecord().hasKey("bla").hasValue(1L)
            .expectNextRecord().hasKey("blub").hasValue(1L)
            .expectNextRecord().hasKey("bla").hasValue(2L)
            .expectNoMoreRecord();
    }

    @Test
    void singleWordShouldBePresent() {
        this.testTopology.input().add("bla");
        this.testTopology.tableOutput().expectNextRecord().isPresent();
    }

    @Test
    void shouldBeDoneAfterSingleWord() {
        this.testTopology.input().add("bla");
        this.testTopology.tableOutput().expectNextRecord().isPresent().expectNextRecord().toBeEmpty();
    }

    @Test
    void shouldDoNothingOnEmptyInput() {
        this.testTopology.streamOutput().expectNoMoreRecord().and().expectNoMoreRecord().toBeEmpty();
    }
}
