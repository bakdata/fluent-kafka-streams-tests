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

import static org.assertj.core.api.Assertions.assertThatCode;

import com.bakdata.fluent_kafka_streams_tests.test_applications.MirrorAvro;
import com.bakdata.fluent_kafka_streams_tests.test_types.City;
import com.bakdata.fluent_kafka_streams_tests.test_types.Person;
import java.util.Map;
import org.junit.jupiter.api.Test;


class TestTopologyTest {

    @Test
    void shouldUseImmutableProperties() {
        final TestTopology<Person, City> testTopology =
                new TestTopology<>(MirrorAvro::getTopology, Map.copyOf(MirrorAvro.getKafkaProperties()));
        assertThatCode(testTopology::start).doesNotThrowAnyException();
        testTopology.stop();
    }
}
