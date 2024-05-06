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

import java.util.HashMap;
import java.util.Map;
import lombok.experimental.UtilityClass;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serdes.LongSerde;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

@UtilityClass
public class NameJoinGlobalKTable {
    public static final String INPUT_TOPIC = "id-input";
    public static final String NAME_INPUT = "name-input";
    public static final String INTERMEDIATE_TOPIC = "upper-case-input";
    public static final String OUTPUT_TOPIC = "join-output";

    public static Map<String, Object> getKafkaProperties() {
        final String brokers = "localhost:9092";
        final Map<String, Object> kafkaConfig = new HashMap<>();
        kafkaConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "globalKTableJoin");
        kafkaConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        kafkaConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, LongSerde.class);
        kafkaConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, StringSerde.class);
        return kafkaConfig;
    }

    public static Topology getTopology() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<Long, Long> inputStream =
                builder.stream(INPUT_TOPIC, Consumed.with(Serdes.Long(), Serdes.Long()));

        final GlobalKTable<Long, String> joinTable = builder.globalTable(NAME_INPUT);

        inputStream
                .join(joinTable,
                        (id, valueId) -> valueId,
                        (id, name) -> name)
                .to(OUTPUT_TOPIC, Produced.with(Serdes.Long(), Serdes.String()));

        return builder.build();
    }

    public static Topology getTopologyWithIntermediateTopic() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<Long, Long> inputStream =
                builder.stream(INPUT_TOPIC, Consumed.with(Serdes.Long(), Serdes.Long()));

        builder.stream(NAME_INPUT, Consumed.with(Serdes.Long(), Serdes.String()))
                .mapValues(name -> name.toUpperCase())
                .to(INTERMEDIATE_TOPIC);

        final GlobalKTable<Long, String> joinTable = builder.globalTable(INTERMEDIATE_TOPIC);

        inputStream
                .join(joinTable,
                        (id, valueId) -> valueId,
                        (id, name) -> name)
                .to(OUTPUT_TOPIC, Produced.with(Serdes.Long(), Serdes.String()));

        return builder.build();
    }

}
