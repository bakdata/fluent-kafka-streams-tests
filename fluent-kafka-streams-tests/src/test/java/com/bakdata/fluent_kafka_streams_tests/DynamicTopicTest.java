package com.bakdata.fluent_kafka_streams_tests;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.bakdata.fluent_kafka_streams_tests.test_applications.TopicExtractorApplication;
import java.util.NoSuchElementException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DynamicTopicTest {

    private static final String KEY = "key";
    private static final String VALUE = "value";
    private final TestTopology<String, String> testTopology =
            new TestTopology<>(TopicExtractorApplication::getTopology, TopicExtractorApplication.getProperties());

    @BeforeEach
    void start() {
        this.testTopology.start();
        this.testTopology.input().add(KEY, VALUE);
        this.testTopology.getOutputTopics().add(TopicExtractorApplication.OUTPUT_TOPIC);
    }

    @AfterEach
    void stop() {
        this.testTopology.stop();
    }

    @Test
    void shouldHaveOutputForTopicName() {
        this.testTopology.streamOutput(TopicExtractorApplication.OUTPUT_TOPIC)
                .expectNextRecord()
                .hasKey(KEY).and().hasValue(VALUE);
    }

    @Test
    void shouldHaveOutputWithoutTopicName() {
        this.testTopology.streamOutput()
                .expectNextRecord()
                .hasKey(KEY).and().hasValue(VALUE);
    }

    @Test
    void shouldThrowExceptionForNonExistingStreamOutputTopic() {
        assertThatExceptionOfType(NoSuchElementException.class)
                .isThrownBy(() -> this.testTopology.streamOutput("non-existing"))
                .withMessage("Output topic 'non-existing' not found");
    }

    @Test
    void shouldThrowExceptionForNonExistingTableOutputTopic() {
        assertThatExceptionOfType(NoSuchElementException.class)
                .isThrownBy(() -> this.testTopology.tableOutput("non-existing"))
                .withMessage("Output topic 'non-existing' not found");
    }

}
