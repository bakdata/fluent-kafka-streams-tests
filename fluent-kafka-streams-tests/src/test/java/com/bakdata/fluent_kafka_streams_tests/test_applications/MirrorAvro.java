/*
 * The MIT License
 *
 * Copyright (c) 2022 bakdata GmbH
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

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.Properties;
import lombok.experimental.UtilityClass;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

@UtilityClass
public class MirrorAvro {
    private static final String INPUT_TOPIC = "input";
    private static final String OUTPUT_TOPIC = "output";

    public static Properties getKafkaProperties() {
        final String brokers = "localhost:9092";
        final Properties kafkaConfig = new Properties();
        kafkaConfig.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "mirror");
        kafkaConfig.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        kafkaConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        kafkaConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        return kafkaConfig;
    }

    public static Topology getTopology() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<SpecificRecord, SpecificRecord> input = builder.stream(INPUT_TOPIC);

        input.to(OUTPUT_TOPIC);
        return builder.build();
    }
}
