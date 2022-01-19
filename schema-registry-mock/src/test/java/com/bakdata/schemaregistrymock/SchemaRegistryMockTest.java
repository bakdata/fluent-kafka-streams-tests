/*
 * The MIT License
 *
 * Copyright (c) 2019 bakdata GmbH
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
package com.bakdata.schemaregistrymock;

import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.avro.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SchemaRegistryMockTest {
    private final SchemaRegistryMock schemaRegistry = new SchemaRegistryMock();

    private static Schema createSchema(final String name) {
        return Schema.createRecord(name, "no doc", "", false, Collections.emptyList());
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
        final Schema keySchema = createSchema("key_schema");
        final int id = this.schemaRegistry.registerKeySchema("test-topic", keySchema);

        final AvroSchema retrievedSchema = (AvroSchema) this.schemaRegistry.getSchemaRegistryClient().getSchemaById(id);
        assertThat(retrievedSchema.rawSchema()).isEqualTo(keySchema);
    }

    @Test
    void shouldRegisterValueSchema() throws IOException, RestClientException {
        final Schema valueSchema = createSchema("value_schema");
        final int id = this.schemaRegistry.registerValueSchema("test-topic", valueSchema);

        final AvroSchema retrievedSchema = (AvroSchema) this.schemaRegistry.getSchemaRegistryClient().getSchemaById(id);
        assertThat(retrievedSchema.rawSchema()).isEqualTo(valueSchema);
    }

    @Test
    void shouldRegisterKeySchemaWithClient() throws IOException, RestClientException {
        final Schema keySchema = createSchema("key_schema");
        final int id =
                this.schemaRegistry.getSchemaRegistryClient().register("test-topic-key", new AvroSchema(keySchema));

        final AvroSchema retrievedSchema = (AvroSchema) this.schemaRegistry.getSchemaRegistryClient().getSchemaById(id);
        assertThat(retrievedSchema.rawSchema()).isEqualTo(keySchema);
    }

    @Test
    void shouldRegisterValueSchemaWithClient() throws IOException, RestClientException {
        final Schema valueSchema = createSchema("value_schema");
        final int id =
                this.schemaRegistry.getSchemaRegistryClient().register("test-topic-value", new AvroSchema(valueSchema));

        final AvroSchema retrievedSchema = (AvroSchema) this.schemaRegistry.getSchemaRegistryClient().getSchemaById(id);
        assertThat(retrievedSchema.rawSchema()).isEqualTo(valueSchema);
    }

    @Test
    void shouldHaveSchemaVersions() throws IOException, RestClientException {
        final Schema valueSchema = createSchema("value_schema");
        final String topic = "test-topic";
        final int id = this.schemaRegistry.registerValueSchema(topic, valueSchema);

        final List<Integer> versions = this.schemaRegistry.getSchemaRegistryClient().getAllVersions(topic + "-value");
        assertThat(versions.size()).isOne();

        final SchemaMetadata metadata =
                this.schemaRegistry.getSchemaRegistryClient().getSchemaMetadata(topic + "-value", versions.get(0));
        assertThat(metadata.getId()).isEqualTo(id);
        final String schemaString = metadata.getSchema();
        final Schema retrievedSchema = new Schema.Parser().parse(schemaString);
        assertThat(retrievedSchema).isEqualTo(valueSchema);
    }

    @Test
    void shouldNotHaveSchemaVersionsForUnknownSubject() {
        assertThatExceptionOfType(RestClientException.class)
                .isThrownBy(() -> this.schemaRegistry.getSchemaRegistryClient().getAllVersions("does_not_exist"))
                .satisfies(e -> assertThat(e.getStatus()).isEqualTo(HTTP_NOT_FOUND));
        assertThatExceptionOfType(RestClientException.class)
                .isThrownBy(() -> this.schemaRegistry.getSchemaRegistryClient().getSchemaMetadata("does_not_exist", 0))
                .satisfies(e -> assertThat(e.getStatus()).isEqualTo(HTTP_NOT_FOUND));
    }

    @Test
    void shouldHaveLatestSchemaVersion() throws IOException, RestClientException {
        final Schema valueSchema1 = createSchema("value_schema");
        final String topic = "test-topic";
        final int id1 = this.schemaRegistry.registerValueSchema(topic, valueSchema1);

        final List<Schema.Field> fields = Collections.singletonList(
                new Schema.Field("f1", Schema.create(Schema.Type.STRING), "", (Object) null));
        final Schema valueSchema2 = Schema.createRecord("value_schema", "no doc", "", false, fields);
        final int id2 = this.schemaRegistry.registerValueSchema(topic, valueSchema2);

        final List<Integer> versions = this.schemaRegistry.getSchemaRegistryClient().getAllVersions(topic + "-value");
        assertThat(versions.size()).isEqualTo(2);

        final SchemaMetadata metadata =
                this.schemaRegistry.getSchemaRegistryClient().getLatestSchemaMetadata(topic + "-value");
        final int metadataId = metadata.getId();
        assertThat(metadataId).isNotEqualTo(id1);
        assertThat(metadataId).isEqualTo(id2);
        final String schemaString = metadata.getSchema();
        final Schema retrievedSchema = new Schema.Parser().parse(schemaString);
        assertThat(retrievedSchema).isEqualTo(valueSchema2);
    }

    @Test
    void shouldNotHaveLatestSchemaVersionForUnknownSubject() {
        assertThatExceptionOfType(RestClientException.class)
                .isThrownBy(
                        () -> this.schemaRegistry.getSchemaRegistryClient().getLatestSchemaMetadata("does_not_exist"))
                .satisfies(e -> assertThat(e.getStatus()).isEqualTo(HTTP_NOT_FOUND));
    }

    @Test
    void shouldReturnAllSubjects() throws IOException, RestClientException {
        this.schemaRegistry.registerKeySchema("test-topic", createSchema("key_schema"));
        this.schemaRegistry.registerValueSchema("test-topic", createSchema("value_schema"));
        final Collection<String> allSubjects = this.schemaRegistry.getSchemaRegistryClient().getAllSubjects();
        assertThat(allSubjects).hasSize(2).containsExactly("test-topic-key", "test-topic-value");
    }

    @Test
    void shouldReturnEmptyListForNoSubjects() throws IOException, RestClientException {
        final Collection<String> allSubjects = this.schemaRegistry.getSchemaRegistryClient().getAllSubjects();
        assertThat(allSubjects).isEmpty();
    }

    @Test
    void shouldDeleteKeySchema() throws IOException, RestClientException {
        this.schemaRegistry.registerKeySchema("test-topic", createSchema("key_schema"));
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
        this.schemaRegistry.registerValueSchema("test-topic", createSchema("value_schema"));
        final Collection<String> allSubjects = client.getAllSubjects();
        assertThat(allSubjects).hasSize(1).containsExactly("test-topic-value");
        this.schemaRegistry.deleteValueSchema("test-topic");
        final Collection<String> subjectsAfterDeletion = client.getAllSubjects();
        assertThat(subjectsAfterDeletion).isEmpty();
    }

    @Test
    void shouldDeleteKeySchemaWithClient() throws IOException, RestClientException {
        final SchemaRegistryClient client = this.schemaRegistry.getSchemaRegistryClient();
        this.schemaRegistry.registerKeySchema("test-topic", createSchema("key_schema"));
        final Collection<String> allSubjects = client.getAllSubjects();
        assertThat(allSubjects).hasSize(1).containsExactly("test-topic-key");
        client.deleteSubject("test-topic-key");
        final Collection<String> subjectsAfterDeletion = client.getAllSubjects();
        assertThat(subjectsAfterDeletion).isEmpty();
    }

    @Test
    void shouldDeleteValueSchemaWithClient() throws IOException, RestClientException {
        final SchemaRegistryClient client = this.schemaRegistry.getSchemaRegistryClient();
        this.schemaRegistry.registerValueSchema("test-topic", createSchema("value_schema"));
        final Collection<String> allSubjects = client.getAllSubjects();
        assertThat(allSubjects).hasSize(1).containsExactly("test-topic-value");
        client.deleteSubject("test-topic-value");
        final Collection<String> subjectsAfterDeletion = client.getAllSubjects();
        assertThat(subjectsAfterDeletion).isEmpty();
    }

    @Test
    void shouldNotDeleteUnknownSubject() {
        assertThatExceptionOfType(RestClientException.class)
                .isThrownBy(() -> this.schemaRegistry.getSchemaRegistryClient().deleteSubject("does_not_exist"))
                .satisfies(e -> assertThat(e.getStatus()).isEqualTo(HTTP_NOT_FOUND));
    }

    @Test
    void shouldNotHaveSchemaVersionsForDeletedSubject() throws IOException, RestClientException {
        final Schema valueSchema = createSchema("value_schema");
        final String topic = "test-topic";
        final int id = this.schemaRegistry.registerValueSchema(topic, valueSchema);

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
        assertThatExceptionOfType(RestClientException.class)
                .isThrownBy(() -> this.schemaRegistry.getSchemaRegistryClient()
                        .getVersion(topic + "-value", new AvroSchema(valueSchema)))
                .satisfies(e -> assertThat(e.getStatus()).isEqualTo(HTTP_NOT_FOUND));
    }

    @Test
    void shouldReturnVersion() throws IOException, RestClientException {
        final ParsedSchema keySchema = new AvroSchema(createSchema("key_schema"));
        final ParsedSchema valueSchema = new AvroSchema(createSchema("value_schema"));
        this.schemaRegistry.registerKeySchema("test-topic", keySchema);
        this.schemaRegistry.registerValueSchema("test-topic", valueSchema);
        assertThat(this.schemaRegistry.getSchemaRegistryClient().getVersion("test-topic-key", keySchema)).isEqualTo(1);
        assertThat(this.schemaRegistry.getSchemaRegistryClient().getVersion("test-topic-value", valueSchema))
                .isEqualTo(1);
    }

    @Test
    void shouldUpdateVersion() throws IOException, RestClientException {
        final ParsedSchema keySchema1 = new AvroSchema(createSchema("key_schema1"));
        final ParsedSchema keySchema2 = new AvroSchema(createSchema("key_schema2"));
        this.schemaRegistry.registerKeySchema("test-topic", keySchema1);
        this.schemaRegistry.registerKeySchema("test-topic", keySchema2);
        assertThat(this.schemaRegistry.getSchemaRegistryClient().getVersion("test-topic-key", keySchema1)).isEqualTo(1);
        assertThat(this.schemaRegistry.getSchemaRegistryClient().getVersion("test-topic-key", keySchema2)).isEqualTo(2);
    }
}
