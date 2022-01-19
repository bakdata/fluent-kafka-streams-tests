package com.bakdata.schemaregistrymock;

import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ProtobufRegistryMockTest {
    private final SchemaRegistryMock schemaRegistry;
    private final ParsedSchema schema;

    ProtobufRegistryMockTest() throws IOException {
        this.schemaRegistry = new SchemaRegistryMock(Collections.singletonList(new ProtobufSchemaProvider()));

        try (final InputStream input = ProtobufRegistryMockTest.class.getResourceAsStream("/record.proto");
                final BufferedReader reader = new BufferedReader(new InputStreamReader(input))) {
            this.schema = new ProtobufSchema(reader.lines().collect(Collectors.joining("\n")));
        }
    }

    @BeforeEach
    void start() {
        this.schemaRegistry.start();
    }

    @AfterEach
    void stop() {
        this.schemaRegistry.stop();
    }

    @Test
    void shouldRegisterKeySchema() throws IOException, RestClientException {
        final int id = this.schemaRegistry.registerKeySchema("test-topic", this.schema);

        final ParsedSchema retrievedSchema = this.schemaRegistry.getSchemaRegistryClient().getSchemaById(id);
        assertThat(retrievedSchema).isEqualTo(this.schema);
    }

    @Test
    void shouldRegisterValueSchema() throws IOException, RestClientException {
        final int id = this.schemaRegistry.registerValueSchema("test-topic", this.schema);

        final ParsedSchema retrievedSchema = this.schemaRegistry.getSchemaRegistryClient().getSchemaById(id);
        assertThat(retrievedSchema).isEqualTo(this.schema);
    }

    @Test
    void shouldRegisterKeySchemaWithClient() throws IOException, RestClientException {
        final int id = this.schemaRegistry.getSchemaRegistryClient().register("test-topic-key", this.schema);

        final ParsedSchema retrievedSchema = this.schemaRegistry.getSchemaRegistryClient().getSchemaById(id);
        assertThat(retrievedSchema).isEqualTo(this.schema);
    }

    @Test
    void shouldRegisterValueSchemaWithClient() throws IOException, RestClientException {
        final int id = this.schemaRegistry.getSchemaRegistryClient().register("test-topic-value", this.schema);

        final ParsedSchema retrievedSchema = this.schemaRegistry.getSchemaRegistryClient().getSchemaById(id);
        assertThat(retrievedSchema).isEqualTo(this.schema);
    }

    @Test
    void shouldHaveSchemaVersions() throws IOException, RestClientException {
        final String topic = "test-topic";
        final int id = this.schemaRegistry.registerValueSchema(topic, this.schema);

        final List<Integer> versions = this.schemaRegistry.getSchemaRegistryClient().getAllVersions(topic + "-value");
        assertThat(versions.size()).isOne();

        final SchemaMetadata metadata =
                this.schemaRegistry.getSchemaRegistryClient().getSchemaMetadata(topic + "-value", versions.get(0));
        assertThat(metadata.getId()).isEqualTo(id);
        final String schemaString = metadata.getSchema();
        final ParsedSchema retrievedSchema = new ProtobufSchema(schemaString);
        assertThat(retrievedSchema).isEqualTo(this.schema);
    }

    @Test
    void shouldReturnAllSubjects() throws IOException, RestClientException {
        this.schemaRegistry.registerKeySchema("test-topic", this.schema);
        this.schemaRegistry.registerValueSchema("test-topic", this.schema);
        final Collection<String> allSubjects = this.schemaRegistry.getSchemaRegistryClient().getAllSubjects();
        assertThat(allSubjects).hasSize(2).containsExactly("test-topic-key", "test-topic-value");
    }


    @Test
    void shouldDeleteKeySchema() throws IOException, RestClientException {
        this.schemaRegistry.registerKeySchema("test-topic", this.schema);
        final SchemaRegistryClient client = this.schemaRegistry.getSchemaRegistryClient();
        final Collection<String> allSubjects = client.getAllSubjects();
        assertThat(allSubjects).hasSize(1).containsExactly("test-topic-key");
        this.schemaRegistry.deleteKeySchema("test-topic");
        final Collection<String> subjectsAfterDeletion = client.getAllSubjects();
        assertThat(subjectsAfterDeletion).isEmpty();
    }

    @Test
    void shouldDeleteValueSchema() throws IOException, RestClientException {
        final SchemaRegistryClient client = this.schemaRegistry.getSchemaRegistryClient();
        this.schemaRegistry.registerValueSchema("test-topic", this.schema);
        final Collection<String> allSubjects = client.getAllSubjects();
        assertThat(allSubjects).hasSize(1).containsExactly("test-topic-value");
        this.schemaRegistry.deleteValueSchema("test-topic");
        final Collection<String> subjectsAfterDeletion = client.getAllSubjects();
        assertThat(subjectsAfterDeletion).isEmpty();
    }

    @Test
    void shouldDeleteKeySchemaWithClient() throws IOException, RestClientException {
        final SchemaRegistryClient client = this.schemaRegistry.getSchemaRegistryClient();
        this.schemaRegistry.registerKeySchema("test-topic", this.schema);
        final Collection<String> allSubjects = client.getAllSubjects();
        assertThat(allSubjects).hasSize(1).containsExactly("test-topic-key");
        client.deleteSubject("test-topic-key");
        final Collection<String> subjectsAfterDeletion = client.getAllSubjects();
        assertThat(subjectsAfterDeletion).isEmpty();
    }

    @Test
    void shouldDeleteValueSchemaWithClient() throws IOException, RestClientException {
        final SchemaRegistryClient client = this.schemaRegistry.getSchemaRegistryClient();
        this.schemaRegistry.registerValueSchema("test-topic", this.schema);
        final Collection<String> allSubjects = client.getAllSubjects();
        assertThat(allSubjects).hasSize(1).containsExactly("test-topic-value");
        client.deleteSubject("test-topic-value");
        final Collection<String> subjectsAfterDeletion = client.getAllSubjects();
        assertThat(subjectsAfterDeletion).isEmpty();
    }

    @Test
    void shouldNotHaveSchemaVersionsForDeletedSubject() throws IOException, RestClientException {
        final String topic = "test-topic";
        final int id = this.schemaRegistry.registerValueSchema(topic, this.schema);

        final List<Integer> versions = this.schemaRegistry.getSchemaRegistryClient().getAllVersions(topic + "-value");
        assertThat(versions.size()).isOne();

        final SchemaMetadata metadata =
                this.schemaRegistry.getSchemaRegistryClient().getSchemaMetadata(topic + "-value", versions.get(0));
        assertThat(metadata.getId()).isEqualTo(id);
        assertThat(this.schemaRegistry.getSchemaRegistryClient().getLatestSchemaMetadata(topic + "-value"))
                .isNotNull();
        this.schemaRegistry.deleteValueSchema(topic);
        assertThatExceptionOfType(RestClientException.class)
                .isThrownBy(() -> this.schemaRegistry.getSchemaRegistryClient().getAllVersions(topic + "-value"))
                .satisfies(e -> assertThat(e.getStatus()).isEqualTo(HTTP_NOT_FOUND));
        assertThatExceptionOfType(RestClientException.class)
                .isThrownBy(() -> this.schemaRegistry.getSchemaRegistryClient()
                        .getSchemaMetadata(topic + "-value", versions.get(0)))
                .satisfies(e -> assertThat(e.getStatus()).isEqualTo(HTTP_NOT_FOUND));
        assertThatExceptionOfType(RestClientException.class)
                .isThrownBy(
                        () -> this.schemaRegistry.getSchemaRegistryClient().getLatestSchemaMetadata(topic + "-value"))
                .satisfies(e -> assertThat(e.getStatus()).isEqualTo(HTTP_NOT_FOUND));
    }

}
