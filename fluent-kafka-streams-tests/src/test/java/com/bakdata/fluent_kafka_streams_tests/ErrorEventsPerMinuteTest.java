package com.bakdata.fluent_kafka_streams_tests;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.bakdata.fluent_kafka_streams_tests.serde.JsonSerde;
import com.bakdata.fluent_kafka_streams_tests.test_applications.ErrorEventsPerMinute;
import com.bakdata.fluent_kafka_streams_tests.test_types.ClickEvent;
import com.bakdata.fluent_kafka_streams_tests.test_types.ErrorOutput;
import com.bakdata.fluent_kafka_streams_tests.test_types.StatusCode;
import java.util.NoSuchElementException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.concurrent.TimeUnit;

class ErrorEventsPerMinuteTest {
    private static final int USER = 1;
    private static final int USER1 = 1;
    private static final int USER2 = 2;
    private static final int USER3 = 3;
    private final ErrorEventsPerMinute app = new ErrorEventsPerMinute();

    @RegisterExtension
    final TestTopology<Integer, ClickEvent> testTopology = new TestTopology<>(this.app::getTopology, this.app.getKafkaProperties());

    @Test
    void shouldCountSingleUserSingleErrorCorrectlyStream() {
        this.testTopology.input(this.app.getStatusInputTopic()).withValueSerde(new JsonSerde<>(StatusCode.class))
                .add(500, new StatusCode(500, "Internal Server Error"));

        final long time = TimeUnit.MINUTES.toMillis(1);
        this.testTopology.input(this.app.getClickInputTopic())
                .at(time).add(USER, new ClickEvent(USER, 500));

        // Test Stream semantics
        this.testTopology.streamOutput(this.app.getErrorOutputTopic()).withValueSerde(new JsonSerde<>(ErrorOutput.class))
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
        this.testTopology.streamOutput(this.app.getErrorOutputTopic()).withValueSerde(new JsonSerde<>(ErrorOutput.class))
                .expectNextRecord().hasKey(500).hasValue(new ErrorOutput(500, 1L, time, "Internal Server Error"))
                .expectNextRecord().hasKey(403).hasValue(new ErrorOutput(403, 1L, time, "Access Forbidden"))
                .expectNextRecord().hasKey(403).hasValue(new ErrorOutput(403, 2L, time, "Access Forbidden"))
                .expectNextRecord().hasKey(403).hasValue(new ErrorOutput(403, 3L, time, "Access Forbidden"))
                .expectNextRecord().hasKey(500).hasValue(new ErrorOutput(500, 2L, time, "Internal Server Error"))
                .expectNoMoreRecord();
    }

    @Test
    void shouldCountSingleUserMultipleCodesCorrectlyTable() {
        final TestInput<Integer, StatusCode> statusInput =
                this.testTopology.input(this.app.getStatusInputTopic()).withValueSerde(new JsonSerde<>(StatusCode.class));
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
        final TestInput<Integer, StatusCode> statusInput =
                this.testTopology.input(this.app.getStatusInputTopic()).withValueSerde(new JsonSerde<>(StatusCode.class));
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
                this.testTopology.streamOutput(this.app.getErrorOutputTopic()).withValueSerde(new JsonSerde<>(ErrorOutput.class));
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
        final TestInput<Integer, StatusCode> statusInput =
                this.testTopology.input(this.app.getStatusInputTopic()).withValueSerde(new JsonSerde<>(StatusCode.class));
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
        assertThrows(IllegalStateException.class, this.testTopology::input);
    }

    @Test
    void shouldFailOnTooManyOutputsForUnnamedCallStream() {
        assertThrows(IllegalStateException.class, this.testTopology::streamOutput);
    }

    @Test
    void shouldFailOnTooManyOutputsForUnnamedCallTable() {
        assertThrows(IllegalStateException.class, this.testTopology::tableOutput);
    }

    @Test
    void shouldFailOnBadInputName() {
        assertThrows(NoSuchElementException.class, () -> this.testTopology.input("bad-name"));
    }

    @Test
    void shouldFailOnBadOutputNameStream() {
        assertThrows(NoSuchElementException.class, () -> this.testTopology.streamOutput("bad-name"));
    }

    @Test
    void shouldFailOnBadOutputNameTable() {
        assertThrows(NoSuchElementException.class, () -> this.testTopology.tableOutput("bad-name"));
    }
}
