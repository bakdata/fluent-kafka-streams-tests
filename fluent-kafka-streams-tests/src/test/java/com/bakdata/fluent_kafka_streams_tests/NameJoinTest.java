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

import com.bakdata.fluent_kafka_streams_tests.test_applications.NameJoinGlobalKTable;
import com.bakdata.fluent_kafka_streams_tests.test_types.Person;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class NameJoinTest {
    private final NameJoinGlobalKTable app = new NameJoinGlobalKTable();

    private final TestTopology<String, Person> testTopology =
            new TestTopology<>(NameJoinGlobalKTable::getTopology, NameJoinGlobalKTable.getKafkaProperties());

    @BeforeEach
    void start() {
        this.testTopology.start();
    }

    @AfterEach
    void stop() {
        this.testTopology.stop();
    }

    @Test
    void testTopology() {
        this.testTopology.input(NameJoinGlobalKTable.NAME_INPUT).withSerde(Serdes.Long(), Serdes.String())
                .add(1L, "Robinson")
                .add(2L, "Walker");

        this.testTopology.input(NameJoinGlobalKTable.INPUT_TOPIC).withSerde(Serdes.Long(), Serdes.Long())
                .add(1L, 1L)
                .add(2L, 2L);

        this.testTopology.streamOutput(NameJoinGlobalKTable.OUTPUT_TOPIC).withSerde(Serdes.Long(), Serdes.String())
                .expectNextRecord().hasKey(1L).hasValue("Robinson")
                .expectNextRecord().hasKey(2L).hasValue("Walker")
                .expectNoMoreRecord();
    }

}
