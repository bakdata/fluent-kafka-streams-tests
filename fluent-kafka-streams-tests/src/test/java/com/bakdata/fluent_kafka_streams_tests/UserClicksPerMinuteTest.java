package com.bakdata.fluent_kafka_streams_tests;

import com.bakdata.fluent_kafka_streams_tests.testutils.ClickEvent;
import com.bakdata.fluent_kafka_streams_tests.testutils.ClickOutput;
import com.bakdata.fluent_kafka_streams_tests.testutils.UserClicksPerMinute;
import com.bakdata.fluent_kafka_streams_tests.testutils.serde.JsonSerde;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.concurrent.TimeUnit;

class UserClicksPerMinuteTest {
    private static final int USER = 1;
    private static final int USER1 = 1;
    private static final int USER2 = 2;
    private final UserClicksPerMinute app = new UserClicksPerMinute();

    @RegisterExtension
    TestTopology<Integer, ClickEvent> testTopology = new TestTopology<>(app::getTopology, app.getKafkaProperties());

    @Test
    void shouldCountSingleUserSingleEventCorrectlyStream() {
        final long time = TimeUnit.MINUTES.toMillis(1);
        testTopology.input().at(time).add(USER, new ClickEvent(USER));

        // Test Stream semantics
        testTopology.streamOutput().withValueSerde(new JsonSerde<>(ClickOutput.class))
                .expectNextRecord().hasKey(USER).hasValue(new ClickOutput(USER, 1L, time))
                .expectNoMoreRecord();
    }

    @Test
    void shouldCountSingleUserSingleEventCorrectlyTable() {
        final long time = TimeUnit.MINUTES.toMillis(1);
        testTopology.input().at(time).add(USER, new ClickEvent(USER));

        // Test Stream semantics
        testTopology.tableOutput().withValueSerde(new JsonSerde<>(ClickOutput.class))
                .expectNextRecord().hasKey(USER).hasValue(new ClickOutput(USER, 1L, time))
                .expectNoMoreRecord();
    }

    @Test
    void shouldCountSingleUserMultipleEventCorrectly() {
        // Window timestamps
        final long time1 = TimeUnit.MINUTES.toMillis(1);
        final long time2 = time1 + TimeUnit.MINUTES.toMillis(1);

        testTopology.input()
                .at(time1).add(USER, new ClickEvent(USER))
                .at(time1 + 10).add(USER, new ClickEvent(USER))
                .at(time1 + 20).add(USER, new ClickEvent(USER))
                .at(time2).add(USER, new ClickEvent(USER));

        // Test Stream semantics
        testTopology.streamOutput().withValueSerde(new JsonSerde<>(ClickOutput.class))
                .expectNextRecord().hasKey(USER).hasValue(new ClickOutput(USER, 1L, time1))
                .expectNextRecord().hasKey(USER).hasValue(new ClickOutput(USER, 2L, time1))
                .expectNextRecord().hasKey(USER).hasValue(new ClickOutput(USER, 3L, time1))
                .expectNextRecord().hasKey(USER).hasValue(new ClickOutput(USER, 1L, time2))
                .expectNoMoreRecord();
    }

    @Test
    void shouldCountMultiUserMultipleEventCorrectly() {
        // Window timestamps
        final long time1 = TimeUnit.MINUTES.toMillis(1);
        final long time2 = time1 + TimeUnit.MINUTES.toMillis(1);

        testTopology.input()
                // First window
                .at(time1).add(new ClickEvent(USER1).getUserId(), new ClickEvent(USER1))
                .at(time1 + 10).add(new ClickEvent(USER2).getUserId(), new ClickEvent(USER2))
                .at(time1 + 20).add(new ClickEvent(USER1).getUserId(), new ClickEvent(USER1))
                // Second window
                .at(time2).add(new ClickEvent(USER2).getUserId(), new ClickEvent(USER2))
                .at(time2 + 10).add(new ClickEvent(USER1).getUserId(), new ClickEvent(USER1))
                .at(time2 + 20).add(new ClickEvent(USER2).getUserId(), new ClickEvent(USER2));

        // Test Stream semantics
        testTopology.streamOutput().withValueSerde(new JsonSerde<>(ClickOutput.class))
                .expectNextRecord().hasKey(USER1).hasValue(new ClickOutput(USER1, 1L, time1))
                .expectNextRecord().hasKey(USER2).hasValue(new ClickOutput(USER2, 1L, time1))
                .expectNextRecord().hasKey(USER1).hasValue(new ClickOutput(USER1, 2L, time1))

                .expectNextRecord().hasKey(USER2).hasValue(new ClickOutput(USER2, 1L, time2))
                .expectNextRecord().hasKey(USER1).hasValue(new ClickOutput(USER1, 1L, time2))
                .expectNextRecord().hasKey(USER2).hasValue(new ClickOutput(USER2, 2L, time2))
                .expectNoMoreRecord();
    }
}
