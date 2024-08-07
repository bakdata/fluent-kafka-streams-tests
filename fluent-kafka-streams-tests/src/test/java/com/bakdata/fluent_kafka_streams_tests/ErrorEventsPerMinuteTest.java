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

package com.bakdata.fluent_kafka_streams_tests;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.bakdata.fluent_kafka_streams_tests.serde.JsonSerde;
import com.bakdata.fluent_kafka_streams_tests.test_applications.ErrorEventsPerMinute;
import com.bakdata.fluent_kafka_streams_tests.test_types.ClickEvent;
import com.bakdata.fluent_kafka_streams_tests.test_types.ErrorOutput;
import com.bakdata.fluent_kafka_streams_tests.test_types.StatusCode;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ErrorEventsPerMinuteTest {
    private static final int USER = 1;
    private static final int USER1 = 1;
    private static final int USER2 = 2;
    private static final int USER3 = 3;
    private final ErrorEventsPerMinute app = new ErrorEventsPerMinute();

    private final TestTopology<Integer, ClickEvent> testTopology =
            new TestTopology<>(this.app::getTopology, ErrorEventsPerMinute.getKafkaProperties());

    @BeforeEach
    void start() {
        this.testTopology.start();
    }

    @AfterEach
    void stop() {
        this.testTopology.stop();
    }

    @Test
    void shouldCountSingleUserSingleErrorCorrectlyStream() {
        this.testTopology.input(this.app.getStatusInputTopic()).withValueSerde(new JsonSerde<>(StatusCode.class))
                .add(500, new StatusCode(500, "Internal Server Error"));

        final long time = TimeUnit.MINUTES.toMillis(1);
        this.testTopology.input(this.app.getClickInputTopic())
                .at(time).add(USER, new ClickEvent(USER, 500));

        // Test Stream semantics
        this.testTopology.streamOutput(this.app.getErrorOutputTopic())
                .withValueSerde(new JsonSerde<>(ErrorOutput.class))
                .expectNextRecord().hasKey(500).hasValue(new ErrorOutput(500, 1L, time, "Internal Server Error"))
                .expectNoMoreRecord();
    }

    @Test
    void shouldCountSingleUserSingleErrorCorrectlyTable() {
        this.testTopology.input(this.app.getStatusInputTopic()).withValueSerde(new JsonSerde<>(StatusCode.class))
                .add(500, new StatusCode(500, "Internal Server Error"));

        final long time = TimeUnit.MINUTES.toMillis(1);
        this.testTopology.input(this.app.getClickInputTopic())
                .at(time).add(USER, new ClickEvent(USER, 500));

        // Test Stream semantics
        this.testTopology.tableOutput(this.app.getErrorOutputTopic()).withValueSerde(new JsonSerde<>(ErrorOutput.class))
                .expectNextRecord().hasKey(500).hasValue(new ErrorOutput(500, 1L, time, "Internal Server Error"))
                .expectNoMoreRecord();
    }

    @Test
    void shouldCountSingleUserMultipleCodesCorrectlyStream() {
        this.testTopology.input(this.app.getStatusInputTopic()).withValueSerde(new JsonSerde<>(StatusCode.class))
                .add(500, new StatusCode(500, "Internal Server Error"))
                .add(403, new StatusCode(403, "Access Forbidden"));

        final long time = TimeUnit.MINUTES.toMillis(1);
        this.testTopology.input(this.app.getClickInputTopic())
                .at(time).add(USER, new ClickEvent(USER, 500))
                .at(time).add(USER, new ClickEvent(USER, 403))
                .at(time).add(USER, new ClickEvent(USER, 403))
                .at(time).add(USER, new ClickEvent(USER, 403))
                .at(time).add(USER, new ClickEvent(USER, 500));

        // Test Stream semantics
        this.testTopology.streamOutput(this.app.getErrorOutputTopic())
                .withValueSerde(new JsonSerde<>(ErrorOutput.class))
                .expectNextRecord().hasKey(500).hasValue(new ErrorOutput(500, 1L, time, "Internal Server Error"))
                .expectNextRecord().hasKey(403).hasValue(new ErrorOutput(403, 1L, time, "Access Forbidden"))
                .expectNextRecord().hasKey(403).hasValue(new ErrorOutput(403, 2L, time, "Access Forbidden"))
                .expectNextRecord().hasKey(403).hasValue(new ErrorOutput(403, 3L, time, "Access Forbidden"))
                .expectNextRecord().hasKey(500).hasValue(new ErrorOutput(500, 2L, time, "Internal Server Error"))
                .expectNoMoreRecord();
    }

    @Test
    void shouldCountSingleUserMultipleCodesCorrectlyTable() {
        final TestInput<Integer, StatusCode> statusInput = this.testTopology.input(this.app.getStatusInputTopic())
                .withValueSerde(new JsonSerde<>(StatusCode.class));
        statusInput.add(500, new StatusCode(500, "Internal Server Error"));
        statusInput.add(403, new StatusCode(403, "Access Forbidden"));

        final long time = TimeUnit.MINUTES.toMillis(1);
        this.testTopology.input(this.app.getClickInputTopic())
                .at(time).add(USER, new ClickEvent(USER, 500))
                .at(time).add(USER, new ClickEvent(USER, 403))
                .at(time).add(USER, new ClickEvent(USER, 403))
                .at(time).add(USER, new ClickEvent(USER, 403))
                .at(time).add(USER, new ClickEvent(USER, 500));

        // Test Stream semantics
        this.testTopology.tableOutput(this.app.getErrorOutputTopic()).withValueSerde(new JsonSerde<>(ErrorOutput.class))
                .expectNextRecord().hasKey(500).hasValue(new ErrorOutput(500, 2L, time, "Internal Server Error"))
                .expectNextRecord().hasKey(403).hasValue(new ErrorOutput(403, 3L, time, "Access Forbidden"))
                .expectNoMoreRecord();
    }

    @Test
    void shouldCountMultiUserMultipleCodesMultipleWindowCorrectly() {
        final TestInput<Integer, StatusCode> statusInput = this.testTopology.input(this.app.getStatusInputTopic())
                .withValueSerde(new JsonSerde<>(StatusCode.class));
        statusInput.add(500, new StatusCode(500, "Internal Server Error"));
        statusInput.add(403, new StatusCode(403, "Access Forbidden"));
        statusInput.add(502, new StatusCode(502, "Bad Gateway"));

        final long time1 = TimeUnit.MINUTES.toMillis(1);
        // First window
        this.testTopology.input(this.app.getClickInputTopic())
                .at(time1).add(USER1, new ClickEvent(USER1, 200))
                .at(time1).add(USER2, new ClickEvent(USER2, 502))
                .at(time1).add(USER1, new ClickEvent(USER1, 500))
                .at(time1 + 10).add(USER1, new ClickEvent(USER1, 502))
                .at(time1 + 10).add(USER1, new ClickEvent(USER1, 200))
                .at(time1 + 10).add(USER1, new ClickEvent(USER1, 502))
                .at(time1 + 10 + 20).add(USER3, new ClickEvent(USER3, 403))
                .at(time1 + 10 + 20).add(USER2, new ClickEvent(USER2, 403));

        // Second window
        final long time2 = time1 + TimeUnit.MINUTES.toMillis(1);
        this.testTopology.input(this.app.getClickInputTopic())
                .at(time2).add(USER3, new ClickEvent(USER3, 403))
                .at(time2).add(USER3, new ClickEvent(USER3, 403))
                .at(time2).add(USER3, new ClickEvent(USER3, 200))
                .at(time2).add(USER2, new ClickEvent(USER2, 403))
                .at(time2).add(USER3, new ClickEvent(USER3, 502))
                .at(time2).add(USER2, new ClickEvent(USER2, 403))
                .at(time2).add(USER2, new ClickEvent(USER2, 500));

        // Third window
        final long time3 = time2 + TimeUnit.MINUTES.toMillis(1);
        this.testTopology.input(this.app.getClickInputTopic())
                .at(time3).add(USER3, new ClickEvent(USER3, 200))
                .at(time3).add(USER1, new ClickEvent(USER1, 403))
                .at(time3).add(USER2, new ClickEvent(USER2, 502))
                .at(time3).add(USER1, new ClickEvent(USER1, 403))
                .at(time3).add(USER1, new ClickEvent(USER1, 500))
                .at(time3).add(USER1, new ClickEvent(USER1, 500));

        // Test Stream semantics
        final TestOutput<Integer, ErrorOutput> errorOutut =
                this.testTopology.streamOutput(this.app.getErrorOutputTopic())
                        .withValueSerde(new JsonSerde<>(ErrorOutput.class));
        errorOutut
                // First window
                .expectNextRecord().hasKey(502).hasValue(new ErrorOutput(502, 1L, time1, "Bad Gateway"))
                .expectNextRecord().hasKey(500).hasValue(new ErrorOutput(500, 1L, time1, "Internal Server Error"))
                .expectNextRecord().hasKey(502).hasValue(new ErrorOutput(502, 2L, time1, "Bad Gateway"))
                .expectNextRecord().hasKey(502).hasValue(new ErrorOutput(502, 3L, time1, "Bad Gateway"))
                .expectNextRecord().hasKey(403).hasValue(new ErrorOutput(403, 1L, time1, "Access Forbidden"))
                .expectNextRecord().hasKey(403).hasValue(new ErrorOutput(403, 2L, time1, "Access Forbidden"))

                // Second window
                .expectNextRecord().hasKey(403).hasValue(new ErrorOutput(403, 1L, time2, "Access Forbidden"))
                .expectNextRecord().hasKey(403).hasValue(new ErrorOutput(403, 2L, time2, "Access Forbidden"))
                .expectNextRecord().hasKey(403).hasValue(new ErrorOutput(403, 3L, time2, "Access Forbidden"))
                .expectNextRecord().hasKey(502).hasValue(new ErrorOutput(502, 1L, time2, "Bad Gateway"))
                .expectNextRecord().hasKey(403).hasValue(new ErrorOutput(403, 4L, time2, "Access Forbidden"))
                .expectNextRecord().hasKey(500).hasValue(new ErrorOutput(500, 1L, time2, "Internal Server Error"))

                // Third window
                .expectNextRecord().hasKey(403).hasValue(new ErrorOutput(403, 1L, time3, "Access Forbidden"))
                .expectNextRecord().hasKey(502).hasValue(new ErrorOutput(502, 1L, time3, "Bad Gateway"))
                .expectNextRecord().hasKey(403).hasValue(new ErrorOutput(403, 2L, time3, "Access Forbidden"))
                .expectNextRecord().hasKey(500).hasValue(new ErrorOutput(500, 1L, time3, "Internal Server Error"))
                .expectNextRecord().hasKey(500).hasValue(new ErrorOutput(500, 2L, time3, "Internal Server Error"))
                .expectNoMoreRecord();
    }

    @Test
    void shouldAlertOnSixErrors() {
        final TestInput<Integer, StatusCode> statusInput = this.testTopology.input(this.app.getStatusInputTopic())
                .withValueSerde(new JsonSerde<>(StatusCode.class));
        statusInput.add(500, new StatusCode(500, "Internal Server Error"));

        final long time = TimeUnit.MINUTES.toMillis(1);
        this.testTopology.input(this.app.getClickInputTopic()).at(time).add(USER, new ClickEvent(USER1, 500))
                .at(time).add(USER, new ClickEvent(USER1, 500))
                .at(time).add(USER, new ClickEvent(USER1, 200))
                .at(time).add(USER, new ClickEvent(USER1, 500))
                .at(time).add(USER, new ClickEvent(USER1, 500))
                .at(time).add(USER, new ClickEvent(USER1, 500))
                .at(time).add(USER, new ClickEvent(USER1, 500));

        // Test Stream semantics
        this.testTopology.tableOutput(this.app.getErrorOutputTopic()).withValueSerde(new JsonSerde<>(ErrorOutput.class))
                .expectNextRecord().hasKey(500).hasValue(new ErrorOutput(500, 6L, time, "Internal Server Error"))
                .expectNoMoreRecord();

        this.testTopology.streamOutput(this.app.getAlertTopic()).withValueSerde(new JsonSerde<>(ErrorOutput.class))
                .expectNextRecord().hasKey(500).hasValue(new ErrorOutput(500, 6L, time, "Internal Server Error"));
    }

    @Test
    void shouldFailOnTooManyInputsForUnnamedCall() {
        assertThatExceptionOfType(IllegalStateException.class)
                .isThrownBy(this.testTopology::input)
                .withMessageStartingWith("#input() works with exactly 1 topic");
    }

    @Test
    void shouldFailOnTooManyOutputsForUnnamedCallStream() {
        assertThatExceptionOfType(IllegalStateException.class)
                .isThrownBy(this.testTopology::streamOutput)
                .withMessage("Please use #output(String) to select a topic");
    }

    @Test
    void shouldFailOnTooManyOutputsForUnnamedCallTable() {
        assertThatExceptionOfType(IllegalStateException.class)
                .isThrownBy(this.testTopology::streamOutput)
                .withMessage("Please use #output(String) to select a topic");
    }

    @Test
    void shouldFailOnBadInputName() {
        assertThatExceptionOfType(NoSuchElementException.class)
                .isThrownBy(() -> this.testTopology.input("bad-name"))
                .withMessage("Input topic 'bad-name' not found");
    }

    @Test
    void shouldFailOnBadOutputNameStream() {
        assertThatExceptionOfType(NoSuchElementException.class)
                .isThrownBy(() -> this.testTopology.streamOutput("bad-name"))
                .withMessage("Output topic 'bad-name' not found");
    }

    @Test
    void shouldFailOnBadOutputNameTable() {
        assertThatExceptionOfType(NoSuchElementException.class)
                .isThrownBy(() -> this.testTopology.tableOutput("bad-name"))
                .withMessage("Output topic 'bad-name' not found");
    }
}
