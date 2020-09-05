package com.bakdata.fluent_kafka_streams_tests;

import com.bakdata.fluent_kafka_streams_tests.test_applications.SuppressionApplication;
import com.bakdata.fluent_kafka_streams_tests.test_applications.SuppressionApplication.AggEvent;
import com.bakdata.fluent_kafka_streams_tests.test_applications.SuppressionApplication.AggEventSerde;
import com.bakdata.fluent_kafka_streams_tests.test_applications.SuppressionApplication.InputEvent;
import com.bakdata.fluent_kafka_streams_tests.test_applications.SuppressionApplication.InputEventSerde;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SuppressionTest {
    private final SuppressionApplication app = new SuppressionApplication();

    private final TestTopology<String, InputEvent> testTopology =
            new TestTopology<>(this.app::getTopology, this.app.getKafkaProperties());

    @BeforeEach
    void start() {
        this.testTopology.start();
    }

    @AfterEach
    void stop() {
        this.testTopology.stop();
    }

    @Test
    void shouldForwardSuppressedValues() {
        final InputEvent firstInputEvent = new InputEvent("first", "a");
        final InputEvent secondInputEvent = new InputEvent("first", "b");
        final InputEvent dummyInput = new InputEvent("dummy-key", "dummy-value");

        final AggEvent expectedEvent = new AggEvent().add(firstInputEvent).add(secondInputEvent);

        this.testTopology.input().withSerde(Serdes.String(), new InputEventSerde())
                .at(TimeUnit.SECONDS.toMillis(1)).add(firstInputEvent)
                .at(TimeUnit.SECONDS.toMillis(2)).add(secondInputEvent)
                .at(System.currentTimeMillis()).add(dummyInput); // flush KTable

        this.testTopology.streamOutput()
                .withSerde(Serdes.String(), new AggEventSerde())
                .expectNextRecord()
                .hasValue(expectedEvent)
                .expectNoMoreRecord();
    }
}
