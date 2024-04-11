/*
 * MIT License
 *
 * Copyright (c) 2019 bakdata GmbH
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

package com.bakdata.fluent_kafka_streams_tests.junit4;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.bakdata.fluent_kafka_streams_tests.junit4.test_applications.WordCount;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.Rule;
import org.junit.Test;

public class WordCountTest {
    private final WordCount app = new WordCount();

    @Rule
    public final TestTopologyRule<Object, String> testTopology =
            new TestTopologyRule<>(this.app::getTopology, this.app.getKafkaProperties());

    @Test
    public void shouldAggregateSameWordStream() {
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
    public void shouldAggregateSameWordTable() {
        this.testTopology.input().add("bla")
                .add("blub")
                .add("bla");
        this.testTopology.tableOutput().withSerde(Serdes.String(), Serdes.Long())
                .expectNextRecord().hasKey("bla").hasValue(2L)
                .expectNextRecord().hasKey("blub").hasValue(1L)
                .expectNoMoreRecord();
    }

    @Test
    public void shouldNotAggregateDifferentWordsStream() {
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
    public void shouldNotAggregateDifferentWordsTable() {
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
    public void shouldReturnNoInputAndOutputStream() {
        this.testTopology.streamOutput().withSerde(Serdes.String(), Serdes.Long())
                .expectNoMoreRecord();
    }

    @Test
    public void shouldReturnNoInputAndOutputTable() {
        this.testTopology.tableOutput().withSerde(Serdes.String(), Serdes.Long())
                .expectNoMoreRecord();
    }

    @Test
    public void shouldAggregateSameWordOrderTable() {
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
    public void shouldReturnSingleInputAndOutputStream() {
        this.testTopology.input().add("bla");

        this.testTopology.streamOutput().withSerde(Serdes.String(), Serdes.Long())
                .expectNextRecord().hasKey("bla").hasValue(1L)
                .expectNoMoreRecord();
    }

    @Test
    public void shouldReturnSingleInputAndOutputTable() {
        this.testTopology.input().add("bla");

        this.testTopology.tableOutput().withSerde(Serdes.String(), Serdes.Long())
                .expectNextRecord().hasKey("bla").hasValue(1L)
                .expectNoMoreRecord();
    }

    @Test
    public void shouldReturnCorrectIteratorStream() {
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
    public void shouldReturnCorrectIteratorExplicitStream() {
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
    public void shouldReturnCorrectIteratorTable() {
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
    public void shouldFailCorrectIteratorStreamNoHasNextCheck() {
        final Iterator<ProducerRecord<String, Long>> output = this.testTopology.streamOutput()
                .withSerde(Serdes.String(), Serdes.Long()).iterator();
        assertThatThrownBy(output::next).isInstanceOf(NoSuchElementException.class);
    }

    @Test
    public void shouldFailCorrectIteratorTableNoHasNextCheck() {
        final Iterator<ProducerRecord<String, Long>> output = this.testTopology.tableOutput()
                .withSerde(Serdes.String(), Serdes.Long()).iterator();
        assertThatThrownBy(output::next).isInstanceOf(NoSuchElementException.class);
    }

    @Test
    public void shouldReturnCorrectIteratorExplicitTable() {
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
    public void shouldWorkOnTableToStream() {
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
    public void singleWordShouldBePresent() {
        this.testTopology.input().add("bla");
        this.testTopology.tableOutput().expectNextRecord().isPresent();
    }

    @Test
    public void shouldBeDoneAfterSingleWord() {
        this.testTopology.input().add("bla");
        this.testTopology.tableOutput().expectNextRecord().isPresent().expectNextRecord().toBeEmpty();
    }

    @Test
    public void shouldDoNothingOnEmptyInput() {
        this.testTopology.streamOutput().expectNoMoreRecord().and().expectNoMoreRecord().toBeEmpty();
    }

    @Test
    public void shouldConvertStreamOutputToList() {
        this.testTopology.input()
                .add("bla")
                .add("blub")
                .add("bla");

        final List<ProducerRecord<String, Long>> outputs = this.testTopology.streamOutput()
                .withSerde(Serdes.String(), Serdes.Long())
                .toList();

        assertThat(outputs)
                .extracting(ProducerRecord::key)
                .containsExactly("bla", "blub", "bla");
        assertThat(outputs)
                .extracting(ProducerRecord::value)
                .containsExactly(1L, 1L, 2L);
    }

    @Test
    public void shouldConvertTableOutputToList() {
        this.testTopology.input()
                .add("bla")
                .add("blub")
                .add("bla");

        final List<ProducerRecord<String, Long>> outputs = this.testTopology.tableOutput()
                .withSerde(Serdes.String(), Serdes.Long())
                .toList();

        assertThat(outputs)
                .extracting(ProducerRecord::key)
                .containsExactly("bla", "blub");
        assertThat(outputs)
                .extracting(ProducerRecord::value)
                .containsExactly(2L, 1L);
    }

    @Test
    public void shouldConvertEmptyStreamOutputToEmptyList() {
        final List<ProducerRecord<String, Long>> outputs = this.testTopology.streamOutput()
                .withSerde(Serdes.String(), Serdes.Long())
                .toList();

        assertThat(outputs)
                .isInstanceOf(List.class)
                .isEmpty();
    }

    @Test
    public void shouldConvertEmptyTableOutputToEmptyList() {
        final List<ProducerRecord<String, Long>> outputs = this.testTopology.tableOutput()
                .withSerde(Serdes.String(), Serdes.Long())
                .toList();

        assertThat(outputs)
                .isInstanceOf(List.class)
                .isEmpty();
    }
}
