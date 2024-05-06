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

package com.bakdata.fluent_kafka_streams_tests.test_applications;

import com.bakdata.fluent_kafka_streams_tests.serde.JsonSerde;
import com.bakdata.fluent_kafka_streams_tests.test_types.ClickEvent;
import com.bakdata.fluent_kafka_streams_tests.test_types.ClickOutput;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serdes.IntegerSerde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;

public class UserClicksPerMinute {
    private static final String INPUT_TOPIC = "user-click-input";

    private static final String OUTPUT_TOPIC = "user-click-output";

    public static Map<String, Object> getKafkaProperties() {
        final String brokers = "localhost:9092";
        final Map<String, Object> kafkaConfig = new HashMap<>();
        kafkaConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "user-clicks-per-minute");
        kafkaConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        kafkaConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, IntegerSerde.class);
        kafkaConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class);
        return kafkaConfig;
    }

    public static Topology getTopology() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<Integer, ClickEvent> clickEvents = builder.stream(INPUT_TOPIC);

        final KTable<Windowed<Integer>, Long> counts = clickEvents
                .groupByKey()
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
                .count();

        counts.toStream()
                .map((key, value) -> KeyValue.pair(
                        key.key(),
                        new ClickOutput(key.key(), value, key.window().start())))
                .to(OUTPUT_TOPIC, Produced.with(Serdes.Integer(), new JsonSerde<>(ClickOutput.class)));

        return builder.build();
    }

}
