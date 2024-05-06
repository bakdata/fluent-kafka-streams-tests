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

import com.bakdata.fluent_kafka_streams_tests.test_types.City;
import com.bakdata.fluent_kafka_streams_tests.test_types.Person;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

@Getter
public class CountInhabitantsWithAvro {

    private final String inputTopic = "person-input";

    private final String outputTopic = "city-output";

    private final String schemaRegistryUrl = "http://dummy";

    public Map<String, Object> getKafkaProperties() {
        final String brokers = "localhost:9092";
        final Map<String, Object> kafkaConfig = new HashMap<>();
        kafkaConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "inhabitants-per-city");
        kafkaConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        kafkaConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerde.class);
        kafkaConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        kafkaConfig.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, this.schemaRegistryUrl);
        return kafkaConfig;
    }

    public Topology getTopology() {
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, Person> persons = builder.stream(this.inputTopic);

        final KTable<String, Long> counts = persons
                .groupBy((name, person) -> person.getCity())
                .count();

        counts.toStream()
                .map((cityName, count) -> KeyValue.pair(cityName, new City(cityName, Math.toIntExact(count))))
                .to(this.outputTopic);

        return builder.build();
    }
}
