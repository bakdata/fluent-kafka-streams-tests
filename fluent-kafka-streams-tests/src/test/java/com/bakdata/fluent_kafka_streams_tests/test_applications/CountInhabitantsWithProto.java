/*
 * MIT License
 *
 * Copyright (c) 2023 bakdata
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

import static com.bakdata.fluent_kafka_streams_tests.test_types.proto.CityOuterClass.City;
import static com.bakdata.fluent_kafka_streams_tests.test_types.proto.PersonOuterClass.Person;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

public class CountInhabitantsWithProto {
    @Getter
    private final String inputTopic = "person-input";

    @Getter
    private final String outputTopic = "city-output";
    @Setter
    private String schemaRegistryUrl;


    public static void main(final String[] args) {
        final CountInhabitantsWithAvro app = new CountInhabitantsWithAvro();
        final KafkaStreams streams = new KafkaStreams(app.getTopology(), app.getKafkaProperties());
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    public KafkaProtobufSerde<Person> newPersonSerde() {
        final KafkaProtobufSerde<Person> serde = new KafkaProtobufSerde<>(Person.class);
        final Map<String, String> config = new HashMap<>();
        config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, this.schemaRegistryUrl);
        serde.configure(config, false);
        return serde;
    }

    public KafkaProtobufSerde<City> newCitySerde() {
        final KafkaProtobufSerde<City> serde = new KafkaProtobufSerde<>(City.class);
        final Map<String, String> config = new HashMap<>();
        config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, this.schemaRegistryUrl);
        serde.configure(config, false);
        return serde;
    }

    public Properties getKafkaProperties() {
        final String brokers = "localhost:9092";
        final Properties kafkaConfig = new Properties();
        kafkaConfig.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "inhabitants-per-city");
        kafkaConfig.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        kafkaConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerde.class);
        kafkaConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, KafkaProtobufSerde.class);
        kafkaConfig.setProperty(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, this.schemaRegistryUrl);
        return kafkaConfig;
    }

    public Topology getTopology() {
        final KafkaProtobufSerde<Person> personSerde = this.newPersonSerde();
        final KafkaProtobufSerde<City> citySerde = this.newCitySerde();
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, Person> persons =
                builder.stream(this.inputTopic, Consumed.with(stringSerde, personSerde));

        final KTable<String, Long> counts = persons
                .groupBy((name, person) -> person.getCity(), Grouped.with(stringSerde, personSerde))
                .count(Materialized.with(stringSerde, longSerde));

        counts.toStream()
                .map((cityName, count) -> KeyValue.pair(
                        cityName,
                        City.newBuilder().setName(cityName).setInhabitants(Math.toIntExact(count)).build()
                ))
                .to(this.outputTopic, Produced.with(stringSerde, citySerde));

        return builder.build();
    }
}
