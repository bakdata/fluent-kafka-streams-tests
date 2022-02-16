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
                .anySatisfy(record -> {
                    assertThat(record.key()).isEqualTo("key1");
                    assertThat(record.value()).isEqualTo("value1");
                    assertThat(record.timestamp()).isEqualTo(0L);
                    assertThat(record.headers().toArray())
                            .hasSize(1)
                            .anySatisfy(header -> {
                                assertThat(header.key()).isEqualTo("header1");
                                assertThat(header.value()).isEqualTo(new byte[]{0});
                            });
                })
                .anySatisfy(record -> {
                    assertThat(record.key()).isEqualTo("key2");
                    assertThat(record.value()).isEqualTo("value2");
                    assertThat(record.timestamp()).isEqualTo(1L);
                    assertThat(record.headers().toArray())
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
