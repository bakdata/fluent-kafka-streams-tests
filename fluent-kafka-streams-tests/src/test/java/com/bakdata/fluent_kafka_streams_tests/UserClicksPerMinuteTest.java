package com.bakdata.fluent_kafka_streams_tests;

import com.bakdata.fluent_kafka_streams_tests.testutils.ClickEvent;
import com.bakdata.fluent_kafka_streams_tests.testutils.ClickOutput;
import com.bakdata.fluent_kafka_streams_tests.testutils.UserClicksPerMinute;
import com.bakdata.fluent_kafka_streams_tests.testutils.serde.JsonSerde;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.concurrent.TimeUnit;

class UserClicksPerMinuteTest {
    private final UserClicksPerMinute app = new UserClicksPerMinute();

    @RegisterExtension
    TestTopology<Integer, ClickEvent> testTopology = new TestTopology<>(app::getTopology, app.getKafkaProperties());

    @Test
    void shouldCountSingleUserSingleEventCorrectlyStream() {
        final long time = TimeUnit.MINUTES.toMillis(1);
        ClickEvent event1 = ClickEvent.builder().userId(1).timestamp(time).ip("100.100.100.100").build();
        testTopology.input().add(1, event1, time);

        // Test Stream semantics
        testTopology.streamOutput().withValueSerde(new JsonSerde<>(ClickOutput.class))
                .expectNextRecord().hasKey(1).hasValue(new ClickOutput(1, 1L, time))
                .expectNoMoreRecord();
    }

    @Test
    void shouldCountSingleUserSingleEventCorrectlyTable() {
        final long time = TimeUnit.MINUTES.toMillis(1);
        ClickEvent event1 = ClickEvent.builder().userId(1).timestamp(time).ip("100.100.100.100").build();
        testTopology.input().add(1, event1, time);

        // Test Stream semantics
        testTopology.tableOutput().withValueSerde(new JsonSerde<>(ClickOutput.class))
                .expectNextRecord().hasKey(1).hasValue(new ClickOutput(1, 1L, time))
                .expectNoMoreRecord();
    }

    @Test
    void shouldCountSingleUserMultipleEventCorrectly() {
        // First window
        final long time1 = TimeUnit.MINUTES.toMillis(1);
        final long time2 = time1 + TimeUnit.SECONDS.toMillis(10);
        final long time3 = time2 + TimeUnit.SECONDS.toMillis(10);

        // Second window
        final long time4 = time1 + TimeUnit.MINUTES.toMillis(1);

        ClickEvent event1 = ClickEvent.builder().userId(1).ip("100.100.100.100").build();
        ClickEvent event2 = ClickEvent.builder().userId(1).ip("100.100.100.100").build();
        ClickEvent event3 = ClickEvent.builder().userId(1).ip("100.100.100.100").build();
        ClickEvent event4 = ClickEvent.builder().userId(1).ip("100.100.100.100").build();
        testTopology.input()
                .add(1, event1, time1)
                .add(1, event2, time2)
                .add(1, event3, time3)
                .add(1, event4, time4);

        // Test Stream semantics
        testTopology.streamOutput().withValueSerde(new JsonSerde<>(ClickOutput.class))
                .expectNextRecord().hasKey(1).hasValue(new ClickOutput(1, 1L, time1))
                .expectNextRecord().hasKey(1).hasValue(new ClickOutput(1, 2L, time1))
                .expectNextRecord().hasKey(1).hasValue(new ClickOutput(1, 3L, time1))
                .expectNextRecord().hasKey(1).hasValue(new ClickOutput(1, 1L, time4))
                .expectNoMoreRecord();
    }

    @Test
    void shouldCountMultiUserMultipleEventCorrectly() {
        // First window
        final long time1 = TimeUnit.MINUTES.toMillis(1);
        final long time2 = time1 + TimeUnit.SECONDS.toMillis(10);
        final long time3 = time2 + TimeUnit.SECONDS.toMillis(10);

        // Second window
        final long time4 = time1 + TimeUnit.MINUTES.toMillis(1);
        final long time5 = time4 + TimeUnit.SECONDS.toMillis(10);
        final long time6 = time5 + TimeUnit.SECONDS.toMillis(10);

        // First window
        ClickEvent event1 = ClickEvent.builder().userId(1).ip("100.100.100.100").build();
        ClickEvent event2 = ClickEvent.builder().userId(2).ip("100.100.100.100").build();
        ClickEvent event3 = ClickEvent.builder().userId(1).ip("100.100.100.100").build();

        // Second window
        ClickEvent event4 = ClickEvent.builder().userId(2).ip("100.100.100.100").build();
        ClickEvent event5 = ClickEvent.builder().userId(1).ip("100.100.100.100").build();
        ClickEvent event6 = ClickEvent.builder().userId(2).ip("100.100.100.100").build();

        testTopology.input()
                .add(event1.getUserId(), event1, time1)
                .add(event2.getUserId(), event2, time2)
                .add(event3.getUserId(), event3, time3)
                .add(event4.getUserId(), event4, time4)
                .add(event5.getUserId(), event5, time5)
                .add(event6.getUserId(), event6, time6);

        // Test Stream semantics
        testTopology.streamOutput().withValueSerde(new JsonSerde<>(ClickOutput.class))
                .expectNextRecord().hasKey(1).hasValue(new ClickOutput(1, 1L, time1))
                .expectNextRecord().hasKey(2).hasValue(new ClickOutput(2, 1L, time1))
                .expectNextRecord().hasKey(1).hasValue(new ClickOutput(1, 2L, time1))

                .expectNextRecord().hasKey(2).hasValue(new ClickOutput(2, 1L, time4))
                .expectNextRecord().hasKey(1).hasValue(new ClickOutput(1, 1L, time4))
                .expectNextRecord().hasKey(2).hasValue(new ClickOutput(2, 2L, time4))
                .expectNoMoreRecord();
    }
}
