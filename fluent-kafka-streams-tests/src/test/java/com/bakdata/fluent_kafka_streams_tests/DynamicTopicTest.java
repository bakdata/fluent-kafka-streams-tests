package com.bakdata.fluent_kafka_streams_tests;

import com.bakdata.fluent_kafka_streams_tests.test_applications.TopicExtractorApplication;
import java.util.NoSuchElementException;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DynamicTopicTest {

    private final String key = "key";
    private final String value = "value";
    private final TopicExtractorApplication app = new TopicExtractorApplication();
    private final TestTopology<String, String> testTopology =
            new TestTopology<>(this.app::getTopology, this.app.getProperties());

    @BeforeEach
    void start() {
        this.testTopology.start();
        this.testTopology.input().add(this.key, this.value);
        this.testTopology.getOutputTopics().add(this.app.getOutputTopic());
    }

    @AfterEach
    void stop() {
        this.testTopology.stop();
    }

    @Test
    void shouldHaveOutputForTopicName() {
        this.testTopology.streamOutput(this.app.getOutputTopic())
                .expectNextRecord()
                .hasKey(this.key).and().hasValue(this.value);
    }

    @Test
    void shouldHaveOutputWithoutTopicName() {
        this.testTopology.streamOutput()
                .expectNextRecord()
                .hasKey(this.key).and().hasValue(this.value);
    }

    @Test
    void shouldThrowExceptionForNonExistingStreamOutputTopic() {
        Assertions.assertThatExceptionOfType(NoSuchElementException.class)
                .isThrownBy(() -> this.testTopology.streamOutput("non-existing"))
                .withMessage("Output topic 'non-existing' not found");
    }

    @Test
    void shouldThrowExceptionForNonExistingTableOutputTopic() {
        Assertions.assertThatExceptionOfType(NoSuchElementException.class)
                .isThrownBy(() -> this.testTopology.streamOutput("non-existing"))
                .withMessage("Output topic 'non-existing' not found");
    }

}
