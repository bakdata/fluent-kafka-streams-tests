/*
 * MIT License
 *
 * Copyright (c) 2025 bakdata
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

import static org.assertj.core.api.Assertions.assertThat;

import com.bakdata.fluent_kafka_streams_tests.test_applications.Mirror;
import com.google.common.collect.Lists;
import java.util.List;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class HeaderTest {
    private final Mirror app = new Mirror();

    private final TestTopology<String, String> testTopology =
            new TestTopology<>(this.app::getTopology, Mirror.getKafkaProperties());

    @BeforeEach
    void start() {
        this.testTopology.start();
    }

    @AfterEach
    void stop() {
        this.testTopology.stop();
    }

    @Test
    void shouldAddHeaders() {
        this.testTopology.input()
                .add("key1", "value1", new RecordHeaders()
                        .add("header1", new byte[]{0}))
                .add("key2", "value2", 1L, new RecordHeaders()
                        .add("header1", new byte[]{1})
                        .add("header2", new byte[]{2, 3}));

        final List<ProducerRecord<String, String>> records = Lists.newArrayList(this.testTopology.streamOutput());
        assertThat(records)
                .hasSize(2)
                .anySatisfy(producerRecord -> {
                    assertThat(producerRecord.key()).isEqualTo("key1");
                    assertThat(producerRecord.value()).isEqualTo("value1");
                    assertThat(producerRecord.timestamp()).isZero();
                    assertThat(producerRecord.headers().toArray())
                            .hasSize(1)
                            .anySatisfy(header -> {
                                assertThat(header.key()).isEqualTo("header1");
                                assertThat(header.value()).isEqualTo(new byte[]{0});
                            });
                })
                .anySatisfy(producerRecord -> {
                    assertThat(producerRecord.key()).isEqualTo("key2");
                    assertThat(producerRecord.value()).isEqualTo("value2");
                    assertThat(producerRecord.timestamp()).isEqualTo(1L);
                    assertThat(producerRecord.headers().toArray())
                            .hasSize(2)
                            .anySatisfy(header -> {
                                assertThat(header.key()).isEqualTo("header1");
                                assertThat(header.value()).isEqualTo(new byte[]{1});
                            })
                            .anySatisfy(header -> {
                                assertThat(header.key()).isEqualTo("header2");
                                assertThat(header.value()).isEqualTo(new byte[]{2, 3});
                            });
                });
    }
}
