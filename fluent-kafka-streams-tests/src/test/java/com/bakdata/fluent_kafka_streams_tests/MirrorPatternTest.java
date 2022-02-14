package com.bakdata.fluent_kafka_streams_tests;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.bakdata.fluent_kafka_streams_tests.test_applications.MirrorPattern;
import java.util.NoSuchElementException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MirrorPatternTest {
    private final MirrorPattern app = new MirrorPattern();

    private final TestTopology<String, String> testTopology = new TestTopology<>(this.app::getTopology,
            MirrorPattern.getKafkaProperties());

    @BeforeEach
    void start() {
        this.testTopology.start();
    }

    @AfterEach
    void stop() {
        this.testTopology.stop();
    }

    @Test
    void shouldConsumeFromPattern() {
        this.testTopology.input("example-input1")
                .add("key1", "value1")
                .add("key2", "value2");
        this.testTopology.input("another-input1")
                .add("key3", "value3");
        this.testTopology.input("example-input2")
                .add("key4", "value4");

        this.testTopology.streamOutput()
                .expectNextRecord().hasKey("key1").hasValue("value1")
                .expectNextRecord().hasKey("key2").hasValue("value2")
                .expectNextRecord().hasKey("key3").hasValue("value3")
                .expectNextRecord().hasKey("key4").hasValue("value4")
                .expectNoMoreRecord();
    }

    @Test
    void shouldThrowIfInputDoesNotMatchPattern() {
        assertThatExceptionOfType(NoSuchElementException.class)
                .isThrownBy(() -> this.testTopology.input("not-matching"));
    }
}
