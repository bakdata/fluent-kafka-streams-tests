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

import com.bakdata.fluent_kafka_streams_tests.serde.JsonSerde;
import com.bakdata.fluent_kafka_streams_tests.test_applications.UserClicksPerMinute;
import com.bakdata.fluent_kafka_streams_tests.test_types.ClickEvent;
import com.bakdata.fluent_kafka_streams_tests.test_types.ClickOutput;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class UserClicksPerMinuteTest {
    private static final int USER = 1;
    private static final int USER1 = 1;
    private static final int USER2 = 2;

    private final TestTopology<Integer, ClickEvent> testTopology =
            new TestTopology<>(UserClicksPerMinute::getTopology, UserClicksPerMinute.getKafkaProperties());

    @BeforeEach
    void start() {
        this.testTopology.start();
    }

    @AfterEach
    void stop() {
        this.testTopology.stop();
    }

    @Test
    void shouldCountSingleUserSingleEventCorrectlyStream() {
        final long time = TimeUnit.MINUTES.toMillis(1);
        this.testTopology.input().at(time).add(USER, new ClickEvent(USER));

        this.testTopology.streamOutput().withValueSerde(new JsonSerde<>(ClickOutput.class))
                .expectNextRecord().hasKey(USER).hasValue(new ClickOutput(USER, 1L, time))
                .expectNoMoreRecord();
    }

    @Test
    void shouldCountSingleUserSingleEventCorrectlyTable() {
        final long time = TimeUnit.MINUTES.toMillis(1);
        this.testTopology.input().at(time).add(USER, new ClickEvent(USER));

        this.testTopology.tableOutput().withValueSerde(new JsonSerde<>(ClickOutput.class))
                .expectNextRecord().hasKey(USER).hasValue(new ClickOutput(USER, 1L, time))
                .expectNoMoreRecord();
    }

    @Test
    void shouldCountSingleUserSingleEventCorrectlyExplicitTime() {
        this.testTopology.input().at(1, TimeUnit.HOURS).add(USER, new ClickEvent(USER));

        this.testTopology.tableOutput().withValueSerde(new JsonSerde<>(ClickOutput.class))
            .expectNextRecord().hasKey(USER).hasValue(new ClickOutput(USER, 1L, TimeUnit.HOURS.toMillis(1)))
            .expectNoMoreRecord();
    }

    @Test
    void shouldCountSingleUserSingleEventCorrectlyExplicitTimeWithoutAt() {
        final long time = TimeUnit.MINUTES.toMillis(1);
        this.testTopology.input()
            .add(USER, new ClickEvent(USER), time)
            .add(USER, new ClickEvent(USER), time);

        this.testTopology.tableOutput().withValueSerde(new JsonSerde<>(ClickOutput.class))
            .expectNextRecord().hasKey(USER).hasValue(new ClickOutput(USER, 2L, time))
            .expectNoMoreRecord();
    }

    @Test
    void shouldCountSingleUserMultipleEventCorrectly() {
        // Window timestamps
        final long time1 = TimeUnit.MINUTES.toMillis(1);
        final long time2 = time1 + TimeUnit.MINUTES.toMillis(1);

        this.testTopology.input()
                .at(time1).add(USER, new ClickEvent(USER))
                .at(time1 + 10).add(USER, new ClickEvent(USER))
                .at(time1 + 20).add(USER, new ClickEvent(USER))
                .at(time2).add(USER, new ClickEvent(USER));

        this.testTopology.streamOutput().withValueSerde(new JsonSerde<>(ClickOutput.class))
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

        this.testTopology.input()
                // First window
                .at(time1).add(new ClickEvent(USER1).getUserId(), new ClickEvent(USER1))
                .at(time1 + 10).add(new ClickEvent(USER2).getUserId(), new ClickEvent(USER2))
                .at(time1 + 20).add(new ClickEvent(USER1).getUserId(), new ClickEvent(USER1))
                // Second window
                .at(time2).add(new ClickEvent(USER2).getUserId(), new ClickEvent(USER2))
                .at(time2 + 10).add(new ClickEvent(USER1).getUserId(), new ClickEvent(USER1))
                .at(time2 + 20).add(new ClickEvent(USER2).getUserId(), new ClickEvent(USER2));

        this.testTopology.streamOutput().withValueSerde(new JsonSerde<>(ClickOutput.class))
                .expectNextRecord().hasKey(USER1).hasValue(new ClickOutput(USER1, 1L, time1))
                .expectNextRecord().hasKey(USER2).hasValue(new ClickOutput(USER2, 1L, time1))
                .expectNextRecord().hasKey(USER1).hasValue(new ClickOutput(USER1, 2L, time1))

                .expectNextRecord().hasKey(USER2).hasValue(new ClickOutput(USER2, 1L, time2))
                .expectNextRecord().hasKey(USER1).hasValue(new ClickOutput(USER1, 1L, time2))
                .expectNextRecord().hasKey(USER2).hasValue(new ClickOutput(USER2, 2L, time2))
                .expectNoMoreRecord();
    }

    @Test
    void shouldWorkWithExplicitKeySerdes() {
        final long time = TimeUnit.MINUTES.toMillis(1);
        this.testTopology.input().withKeySerde(Serdes.Integer())
            .at(time).add(USER, new ClickEvent(USER));

        this.testTopology.streamOutput()
            .withKeySerde(Serdes.Integer())
            .withValueSerde(new JsonSerde<>(ClickOutput.class))
            .expectNextRecord().hasKey(USER).hasValue(new ClickOutput(USER, 1, time));
    }
}
