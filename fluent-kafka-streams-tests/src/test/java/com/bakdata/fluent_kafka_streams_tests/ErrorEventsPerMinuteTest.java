package com.bakdata.fluent_kafka_streams_tests;

import com.bakdata.fluent_kafka_streams_tests.testutils.ClickEvent;
import com.bakdata.fluent_kafka_streams_tests.testutils.ErrorEventsPerMinute;
import com.bakdata.fluent_kafka_streams_tests.testutils.ErrorOutput;
import com.bakdata.fluent_kafka_streams_tests.testutils.StatusCode;
import com.bakdata.fluent_kafka_streams_tests.testutils.serde.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.concurrent.TimeUnit;


class ErrorEventsPerMinuteTest {
    private final ErrorEventsPerMinute app = new ErrorEventsPerMinute();

    @RegisterExtension
    TestTopology<Integer, ClickEvent> testTopology = new TestTopology<>(app::getTopology, app.getKafkaProperties());

    @Test
    void shouldCountSingleUserSingleErrorCorrectlyStream() {
        TestInput<Integer, StatusCode> statusInput =
                testTopology.input(app.getStatusInputTopic()).withValueSerde(new JsonSerde<>(StatusCode.class));
        statusInput.add(500, new StatusCode(500, "Internal Server Error"));

        final long time = TimeUnit.MINUTES.toMillis(1);
        ClickEvent event1 = ClickEvent.builder().userId(1).timestamp(time).ip("100.100.100.100").status(500).build();
        testTopology.input(app.getClickInputTopic()).add(1, event1, time);

        // Test Stream semantics
        testTopology.streamOutput(app.getErrorOutputTopic()).withValueSerde(new JsonSerde<>(ErrorOutput.class))
                .expectNextRecord().hasKey(500).hasValue(new ErrorOutput(500, 1L, time, "Internal Server Error"))
                .expectNoMoreRecord();
    }

    @Test
    void shouldCountSingleUserSingleErrorCorrectlyTable() {
        TestInput<Integer, StatusCode> statusInput =
                testTopology.input(app.getStatusInputTopic()).withValueSerde(new JsonSerde<>(StatusCode.class));
        statusInput.add(500, new StatusCode(500, "Internal Server Error"));

        final long time = TimeUnit.MINUTES.toMillis(1);
        ClickEvent event1 = ClickEvent.builder().userId(1).timestamp(time).ip("100.100.100.100").status(500).build();
        testTopology.input(app.getClickInputTopic()).add(1, event1, time);

        // Test Stream semantics
        testTopology.tableOutput(app.getErrorOutputTopic()).withValueSerde(new JsonSerde<>(ErrorOutput.class))
                .expectNextRecord().hasKey(500).hasValue(new ErrorOutput(500, 1L, time, "Internal Server Error"))
                .expectNoMoreRecord();
    }

    @Test
    void shouldCountSingleUserMultipleCodesCorrectlyStream() {
        TestInput<Integer, StatusCode> statusInput =
                testTopology.input(app.getStatusInputTopic()).withValueSerde(new JsonSerde<>(StatusCode.class));
        statusInput.add(500, new StatusCode(500, "Internal Server Error"));
        statusInput.add(403, new StatusCode(403, "Access Forbidden"));

        final long time = TimeUnit.MINUTES.toMillis(1);
        ClickEvent event1 = ClickEvent.builder().userId(1).timestamp(time).ip("100.100.100.100").status(500).build();
        ClickEvent event2 = ClickEvent.builder().userId(1).timestamp(time).ip("100.100.100.100").status(403).build();
        ClickEvent event3 = ClickEvent.builder().userId(1).timestamp(time).ip("100.100.100.100").status(403).build();
        ClickEvent event4 = ClickEvent.builder().userId(1).timestamp(time).ip("100.100.100.100").status(403).build();
        ClickEvent event5 = ClickEvent.builder().userId(1).timestamp(time).ip("100.100.100.100").status(500).build();

        TestInput<Integer, ClickEvent> clickEventInput = testTopology.input(app.getClickInputTopic());
        clickEventInput.add(1, event1, time);
        clickEventInput.add(1, event2, time);
        clickEventInput.add(1, event3, time);
        clickEventInput.add(1, event4, time);
        clickEventInput.add(1, event5, time);

        // Test Stream semantics
        testTopology.streamOutput(app.getErrorOutputTopic()).withValueSerde(new JsonSerde<>(ErrorOutput.class))
                .expectNextRecord().hasKey(500).hasValue(new ErrorOutput(500, 1L, time, "Internal Server Error"))
                .expectNextRecord().hasKey(403).hasValue(new ErrorOutput(403, 1L, time, "Access Forbidden"))
                .expectNextRecord().hasKey(403).hasValue(new ErrorOutput(403, 2L, time, "Access Forbidden"))
                .expectNextRecord().hasKey(403).hasValue(new ErrorOutput(403, 3L, time, "Access Forbidden"))
                .expectNextRecord().hasKey(500).hasValue(new ErrorOutput(500, 2L, time, "Internal Server Error"))
                .expectNoMoreRecord();
    }

    @Test
    void shouldCountSingleUserMultipleCodesCorrectlyTable() {
        TestInput<Integer, StatusCode> statusInput =
                testTopology.input(app.getStatusInputTopic()).withValueSerde(new JsonSerde<>(StatusCode.class));
        statusInput.add(500, new StatusCode(500, "Internal Server Error"));
        statusInput.add(403, new StatusCode(403, "Access Forbidden"));

        final long time = TimeUnit.MINUTES.toMillis(1);
        ClickEvent event1 = ClickEvent.builder().userId(1).timestamp(time).ip("100.100.100.100").status(500).build();
        ClickEvent event2 = ClickEvent.builder().userId(1).timestamp(time).ip("100.100.100.100").status(403).build();
        ClickEvent event3 = ClickEvent.builder().userId(1).timestamp(time).ip("100.100.100.100").status(403).build();
        ClickEvent event4 = ClickEvent.builder().userId(1).timestamp(time).ip("100.100.100.100").status(403).build();
        ClickEvent event5 = ClickEvent.builder().userId(1).timestamp(time).ip("100.100.100.100").status(500).build();

        TestInput<Integer, ClickEvent> clickEventInput = testTopology.input(app.getClickInputTopic());
        clickEventInput.add(1, event1, time);
        clickEventInput.add(1, event2, time);
        clickEventInput.add(1, event3, time);
        clickEventInput.add(1, event4, time);
        clickEventInput.add(1, event5, time);

        // Test Stream semantics
        testTopology.tableOutput(app.getErrorOutputTopic()).withValueSerde(new JsonSerde<>(ErrorOutput.class))
                .expectNextRecord().hasKey(500).hasValue(new ErrorOutput(500, 2L, time, "Internal Server Error"))
                .expectNextRecord().hasKey(403).hasValue(new ErrorOutput(403, 3L, time, "Access Forbidden"))
                .expectNoMoreRecord();
    }

    @Test
    void shouldCountMultiUserMultipleCodesMultipleWindowCorrectly() {
        TestInput<Integer, StatusCode> statusInput =
                testTopology.input(app.getStatusInputTopic()).withValueSerde(new JsonSerde<>(StatusCode.class));
        statusInput.add(500, new StatusCode(500, "Internal Server Error"));
        statusInput.add(403, new StatusCode(403, "Access Forbidden"));
        statusInput.add(502, new StatusCode(502, "Bad Gateway"));

        // First window
        final long time1 = TimeUnit.MINUTES.toMillis(1);
        final long time2 = time1 + TimeUnit.SECONDS.toMillis(10);
        final long time3 = time2 + TimeUnit.SECONDS.toMillis(10);
        ClickEvent event1_1 = ClickEvent.builder().userId(1).timestamp(time1).ip("100.100.100.100").status(200).build();
        ClickEvent event1_2 = ClickEvent.builder().userId(2).timestamp(time1).ip("100.100.100.100").status(502).build();
        ClickEvent event1_3 = ClickEvent.builder().userId(1).timestamp(time1).ip("100.100.100.100").status(500).build();
        ClickEvent event1_4 = ClickEvent.builder().userId(1).timestamp(time2).ip("100.100.100.100").status(502).build();
        ClickEvent event1_5 = ClickEvent.builder().userId(1).timestamp(time2).ip("100.100.100.100").status(200).build();
        ClickEvent event1_6 = ClickEvent.builder().userId(1).timestamp(time2).ip("100.100.100.100").status(502).build();
        ClickEvent event1_7 = ClickEvent.builder().userId(3).timestamp(time3).ip("100.100.100.100").status(403).build();
        ClickEvent event1_8 = ClickEvent.builder().userId(2).timestamp(time3).ip("100.100.100.100").status(403).build();

        // Second window
        final long time4 = time1 + TimeUnit.MINUTES.toMillis(1);
        ClickEvent event2_1 = ClickEvent.builder().userId(3).timestamp(time4).ip("100.100.100.100").status(403).build();
        ClickEvent event2_2 = ClickEvent.builder().userId(3).timestamp(time4).ip("100.100.100.100").status(403).build();
        ClickEvent event2_3 = ClickEvent.builder().userId(3).timestamp(time4).ip("100.100.100.100").status(200).build();
        ClickEvent event2_4 = ClickEvent.builder().userId(2).timestamp(time4).ip("100.100.100.100").status(403).build();
        ClickEvent event2_5 = ClickEvent.builder().userId(3).timestamp(time4).ip("100.100.100.100").status(502).build();
        ClickEvent event2_6 = ClickEvent.builder().userId(2).timestamp(time4).ip("100.100.100.100").status(403).build();
        ClickEvent event2_7 = ClickEvent.builder().userId(2).timestamp(time4).ip("100.100.100.100").status(500).build();

        // Third window
        final long time5 = time4 + TimeUnit.MINUTES.toMillis(1);
        ClickEvent event3_1 = ClickEvent.builder().userId(3).timestamp(time5).ip("100.100.100.100").status(200).build();
        ClickEvent event3_2 = ClickEvent.builder().userId(1).timestamp(time5).ip("100.100.100.100").status(403).build();
        ClickEvent event3_3 = ClickEvent.builder().userId(2).timestamp(time5).ip("100.100.100.100").status(502).build();
        ClickEvent event3_4 = ClickEvent.builder().userId(1).timestamp(time5).ip("100.100.100.100").status(403).build();
        ClickEvent event3_5 = ClickEvent.builder().userId(1).timestamp(time5).ip("100.100.100.100").status(500).build();
        ClickEvent event3_6 = ClickEvent.builder().userId(1).timestamp(time5).ip("100.100.100.100").status(500).build();

        TestInput<Integer, ClickEvent> clickEventInput = testTopology.input(app.getClickInputTopic());
        // First window
        clickEventInput.add(event1_1.getUserId(), event1_1, event1_1.getTimestamp());
        clickEventInput.add(event1_2.getUserId(), event1_2, event1_2.getTimestamp());
        clickEventInput.add(event1_3.getUserId(), event1_3, event1_3.getTimestamp());
        clickEventInput.add(event1_4.getUserId(), event1_4, event1_4.getTimestamp());
        clickEventInput.add(event1_5.getUserId(), event1_5, event1_5.getTimestamp());
        clickEventInput.add(event1_6.getUserId(), event1_6, event1_6.getTimestamp());
        clickEventInput.add(event1_7.getUserId(), event1_7, event1_7.getTimestamp());
        clickEventInput.add(event1_8.getUserId(), event1_8, event1_8.getTimestamp());

        // Second window
        clickEventInput.add(event2_1.getUserId(), event2_1, event2_1.getTimestamp());
        clickEventInput.add(event2_2.getUserId(), event2_2, event2_2.getTimestamp());
        clickEventInput.add(event2_3.getUserId(), event2_3, event2_3.getTimestamp());
        clickEventInput.add(event2_4.getUserId(), event2_4, event2_4.getTimestamp());
        clickEventInput.add(event2_5.getUserId(), event2_5, event2_5.getTimestamp());
        clickEventInput.add(event2_6.getUserId(), event2_6, event2_6.getTimestamp());
        clickEventInput.add(event2_7.getUserId(), event2_7, event2_7.getTimestamp());

        // Third window
        clickEventInput.add(event3_1.getUserId(), event3_1, event3_1.getTimestamp());
        clickEventInput.add(event3_2.getUserId(), event3_2, event3_2.getTimestamp());
        clickEventInput.add(event3_3.getUserId(), event3_3, event3_3.getTimestamp());
        clickEventInput.add(event3_4.getUserId(), event3_4, event3_4.getTimestamp());
        clickEventInput.add(event3_5.getUserId(), event3_5, event3_5.getTimestamp());
        clickEventInput.add(event3_6.getUserId(), event3_6, event3_6.getTimestamp());


        // Test Stream semantics
        TestOutput<Integer, ErrorOutput> errorOutut =
                testTopology.streamOutput(app.getErrorOutputTopic()).withValueSerde(new JsonSerde<>(ErrorOutput.class));
        errorOutut
                // First window
                .expectNextRecord().hasKey(502).hasValue(new ErrorOutput(502, 1L, time1, "Bad Gateway"))
                .expectNextRecord().hasKey(500).hasValue(new ErrorOutput(500, 1L, time1, "Internal Server Error"))
                .expectNextRecord().hasKey(502).hasValue(new ErrorOutput(502, 2L, time1, "Bad Gateway"))
                .expectNextRecord().hasKey(502).hasValue(new ErrorOutput(502, 3L, time1, "Bad Gateway"))
                .expectNextRecord().hasKey(403).hasValue(new ErrorOutput(403, 1L, time1, "Access Forbidden"))
                .expectNextRecord().hasKey(403).hasValue(new ErrorOutput(403, 2L, time1, "Access Forbidden"))

                // Second window
                .expectNextRecord().hasKey(403).hasValue(new ErrorOutput(403, 1L, time4, "Access Forbidden"))
                .expectNextRecord().hasKey(403).hasValue(new ErrorOutput(403, 2L, time4, "Access Forbidden"))
                .expectNextRecord().hasKey(403).hasValue(new ErrorOutput(403, 3L, time4, "Access Forbidden"))
                .expectNextRecord().hasKey(502).hasValue(new ErrorOutput(502, 1L, time4, "Bad Gateway"))
                .expectNextRecord().hasKey(403).hasValue(new ErrorOutput(403, 4L, time4, "Access Forbidden"))
                .expectNextRecord().hasKey(500).hasValue(new ErrorOutput(500, 1L, time4, "Internal Server Error"))

                // Third window
                .expectNextRecord().hasKey(403).hasValue(new ErrorOutput(403, 1L, time5, "Access Forbidden"))
                .expectNextRecord().hasKey(502).hasValue(new ErrorOutput(502, 1L, time5, "Bad Gateway"))
                .expectNextRecord().hasKey(403).hasValue(new ErrorOutput(403, 2L, time5, "Access Forbidden"))
                .expectNextRecord().hasKey(500).hasValue(new ErrorOutput(500, 1L, time5, "Internal Server Error"))
                .expectNextRecord().hasKey(500).hasValue(new ErrorOutput(500, 2L, time5, "Internal Server Error"))
                .expectNoMoreRecord();
    }

    @Test
    void shouldAlertOnSixErrors() {
        TestInput<Integer, StatusCode> statusInput =
                testTopology.input(app.getStatusInputTopic()).withValueSerde(new JsonSerde<>(StatusCode.class));
        statusInput.add(500, new StatusCode(500, "Internal Server Error"));

        final long time = TimeUnit.MINUTES.toMillis(1);
        ClickEvent event1 = ClickEvent.builder().userId(1).timestamp(time).ip("100.100.100.100").status(500).build();
        ClickEvent event2 = ClickEvent.builder().userId(1).timestamp(time).ip("100.100.100.100").status(500).build();
        ClickEvent event3 = ClickEvent.builder().userId(1).timestamp(time).ip("100.100.100.100").status(200).build();
        ClickEvent event4 = ClickEvent.builder().userId(1).timestamp(time).ip("100.100.100.100").status(500).build();
        ClickEvent event5 = ClickEvent.builder().userId(1).timestamp(time).ip("100.100.100.100").status(500).build();
        ClickEvent event6 = ClickEvent.builder().userId(1).timestamp(time).ip("100.100.100.100").status(500).build();
        ClickEvent event7 = ClickEvent.builder().userId(1).timestamp(time).ip("100.100.100.100").status(500).build();
        testTopology.input(app.getClickInputTopic()).add(1, event1, time);
        testTopology.input(app.getClickInputTopic()).add(1, event2, time);
        testTopology.input(app.getClickInputTopic()).add(1, event3, time);
        testTopology.input(app.getClickInputTopic()).add(1, event4, time);
        testTopology.input(app.getClickInputTopic()).add(1, event5, time);
        testTopology.input(app.getClickInputTopic()).add(1, event6, time);
        testTopology.input(app.getClickInputTopic()).add(1, event7, time);

        // Test Stream semantics
        testTopology.tableOutput(app.getErrorOutputTopic()).withValueSerde(new JsonSerde<>(ErrorOutput.class))
                .expectNextRecord().hasKey(500).hasValue(new ErrorOutput(500, 6L, time, "Internal Server Error"))
                .expectNoMoreRecord();

        testTopology.streamOutput(app.getAlertTopic()).withValueSerde(new JsonSerde<>(ErrorOutput.class))
                .expectNextRecord().hasKey(500).hasValue(new ErrorOutput(500, 6L, time, "Internal Server Error"));
    }
}
