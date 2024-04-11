/*
 * MIT License
 *
 * Copyright (c) 2023 bakdata GmbH
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

package com.bakdata.fluent_kafka_streams_tests;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.bakdata.fluent_kafka_streams_tests.test_applications.WordCount;
import java.util.Arrays;
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
        final List<String> expected = Arrays.asList("bla", "blub", "bla", "foo");

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
        final List<String> expected = Arrays.asList("bla", "blub", "bla", "foo");

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
        final List<String> expected = Arrays.asList("bla", "blub", "foo");

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
    void shouldThrowAssertionErrorIfNotPresent() {
        final Expectation<Object, String> expectation = this.testTopology.streamOutput().expectNextRecord();
        assertThatExceptionOfType(AssertionError.class)
                .isThrownBy(expectation::isPresent)
                .withMessage("No more records found");
    }

    @Test
    void shouldThrowAssertionErrorIfKeyNotMatching() {
        this.testTopology.input().add("bla", "blub");

        final Expectation<Object, String> expectation = this.testTopology.streamOutput().expectNextRecord();
        assertThatExceptionOfType(AssertionError.class)
                .isThrownBy(() -> expectation.hasKey("nope"))
                .withMessage("Record key does not match");
    }

    @Test
    void shouldThrowAssertionErrorIfValueNotMatching() {
        this.testTopology.input().add("bla", "blub");

        final Expectation<Object, String> expectation = this.testTopology.streamOutput().expectNextRecord();
        assertThatExceptionOfType(AssertionError.class)
                .isThrownBy(() -> expectation.hasValue("nope"))
                .withMessage("Record value does not match");
    }

    @Test
    void shouldMakeKeyAssertions() {
        this.testTopology.input().add("bla");

        this.testTopology.streamOutput()
                .withValueSerde(Serdes.Long())
                .expectNextRecord()
                .hasKeySatisfying(key -> assertThat(key).isEqualTo("bla"))
                .expectNoMoreRecord();
    }

    @Test
    void shouldFailKeyAssertion() {
        this.testTopology.input().add("bla");

        final Expectation<Object, Long> expectation = this.testTopology.streamOutput()
                .withValueSerde(Serdes.Long())
                .expectNextRecord();
        assertThatExceptionOfType(AssertionError.class)
                .isThrownBy(() -> expectation.hasKeySatisfying(key -> assertThat(key).isEqualTo("blub")));
    }

    @Test
    void shouldNotMakeKeyAssertions() {
        this.testTopology.input();

        final Expectation<Object, Long> expectation = this.testTopology.streamOutput()
                .withValueSerde(Serdes.Long())
                .expectNextRecord();
        assertThatExceptionOfType(AssertionError.class)
                .isThrownBy(() -> expectation.hasKeySatisfying(key -> {}))
                .withMessage("No more records found");
    }

    @Test
    void shouldMakeValueAssertions() {
        this.testTopology.input().add("bla");

        this.testTopology.streamOutput()
                .withValueSerde(Serdes.Long())
                .expectNextRecord()
                .hasValueSatisfying(value -> assertThat(value).isEqualTo(1L))
                .expectNoMoreRecord();
    }

    @Test
    void shouldFailValueAssertion() {
        this.testTopology.input().add("bla");

        final Expectation<Object, Long> expectation = this.testTopology.streamOutput()
                .withValueSerde(Serdes.Long())
                .expectNextRecord();
        assertThatExceptionOfType(AssertionError.class)
                .isThrownBy(() -> expectation.hasValueSatisfying(key -> assertThat(key).isEqualTo(2L)));
    }

    @Test
    void shouldNotMakeValueAssertions() {
        this.testTopology.input();

        final Expectation<Object, Long> expectation = this.testTopology.streamOutput()
                .withValueSerde(Serdes.Long())
                .expectNextRecord();
        assertThatExceptionOfType(AssertionError.class)
                .isThrownBy(() -> expectation.hasValueSatisfying(key -> {}))
                .withMessage("No more records found");
    }

    @Test
    void shouldThrowAssertionErrorIfNotEmpty() {
        this.testTopology.input().add("bla").add("blub");

        final Expectation<Object, String> expectation = this.testTopology.streamOutput().expectNextRecord();
        assertThatExceptionOfType(AssertionError.class)
                .isThrownBy(expectation::toBeEmpty)
                .withMessage("More records found");
    }

    @Test
    void shouldReturnCorrectIteratorExplicitTable() {
        this.testTopology.input().add("bla")
                .add("blub")
                .add("bla")
                .add("foo");
        final List<String> expected = Arrays.asList("bla", "blub", "foo");

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
    void shouldFailForUnmachtedKey() {
        this.testTopology.input().add("bla")
                .add("blub")
                .add("bla");

        assertThatThrownBy(() ->
                this.testTopology.streamOutput().withSerde(Serdes.String(), Serdes.Long())
                        .expectNextRecord().hasKey("blub"))
                .hasMessage("Record key does not match");
    }

    @Test
    void shouldFailForUnmatchedValue() {
        this.testTopology.input().add("bla")
                .add("blub")
                .add("bla");

        assertThatThrownBy(() ->
                this.testTopology.streamOutput().withSerde(Serdes.String(), Serdes.Long())
                        .expectNextRecord().hasKey("bla").hasValue(2L))
                .hasMessage("Record value does not match");
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

    @Test
    void shouldConvertStreamOutputToList() {
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
    void shouldConvertTableOutputToList() {
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
    void shouldConvertEmptyStreamOutputToEmptyList() {
        final List<ProducerRecord<String, Long>> outputs = this.testTopology.streamOutput()
                .withSerde(Serdes.String(), Serdes.Long())
                .toList();

        assertThat(outputs)
                .isInstanceOf(List.class)
                .isEmpty();
    }

    @Test
    void shouldConvertEmptyTableOutputToEmptyList() {
        final List<ProducerRecord<String, Long>> outputs = this.testTopology.tableOutput()
                .withSerde(Serdes.String(), Serdes.Long())
                .toList();

        assertThat(outputs)
                .isInstanceOf(List.class)
                .isEmpty();
    }
}
