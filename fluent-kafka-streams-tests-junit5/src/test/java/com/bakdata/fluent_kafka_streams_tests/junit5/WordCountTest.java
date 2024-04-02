/*
 * MIT License
 *
 * Copyright (c) 2024 bakdata
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.bakdata.fluent_kafka_streams_tests.junit5;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.bakdata.fluent_kafka_streams_tests.junit5.test_applications.WordCount;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class WordCountTest {
    private final WordCount app = new WordCount();

    @RegisterExtension
    final TestTopologyExtension<Object, String> testTopology = new TestTopologyExtension<>(this.app::getTopology,
            WordCount.getKafkaProperties());

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
                .toIterable()
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
                .toIterable()
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
