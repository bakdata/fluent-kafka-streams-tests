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

import com.bakdata.fluent_kafka_streams_tests.test_applications.MirrorPatternTopicMixed;
import java.util.NoSuchElementException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MirrorPatternTopicMixedTest {
    private final MirrorPatternTopicMixed app = new MirrorPatternTopicMixed();

    private final TestTopology<String, String> testTopology = new TestTopology<>(this.app::getTopology,
            MirrorPatternTopicMixed.getKafkaProperties());

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
        this.testTopology.input("input2")
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
