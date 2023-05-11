/*
 * MIT License
 *
 * Copyright (c) 2023 bakdata GmbH
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

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.extension.Extension;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;

/**
 * <p>The schema registry mock implements a few basic HTTP endpoints that are used by the Avro serdes.</p>
 * In particular,
 * <ul>
 * <li>you can register a schema</li>
 * <li>retrieve a schema by id.</li>
 * <li>list and get schema versions of a subject</li>
 * </ul>
 *
 * <p>If you use the TestTopology of the fluent Kafka Streams test, you don't have to interact with this class at
 * all.</p>
 *
 * <p>Without the test framework, you can use the mock as follows:</p>
 * <pre><code>
 * class SchemaRegistryMockTest {
 *     private final SchemaRegistryMock schemaRegistry = new SchemaRegistryMock();
 *
 *     {@literal @BeforeEach}
 *     void setup() {
 *         schemaRegistry.start();
 *     }
 *
 *     {@literal @AfterEach}
 *     void teardown() {
 *         schemaRegistry.stop();
 *     }
 *
 *     {@literal @Test}
 *     void shouldRegisterKeySchema() throws IOException, RestClientException {
 *         final Schema keySchema = this.createSchema("key_schema");
 *         final int id = this.schemaRegistry.registerKeySchema("test-topic", keySchema);
 *
 *         final Schema retrievedSchema = this.schemaRegistry.getSchemaRegistryClient().getById(id);
 *         assertThat(retrievedSchema).isEqualTo(keySchema);
 *     }
 * }</code></pre>
 *
 * To retrieve the url of the schema registry for a Kafka Streams config, please use {@link #getUrl()}
 */
@Slf4j
public class SchemaRegistryMock {
    private static final String ALL_SUBJECT_PATTERN = "/subjects";
    private static final String SCHEMA_PATH_PATTERN = "/subjects/[^/]+/versions";
    private static final String SCHEMA_BY_ID_PATTERN = "/schemas/ids/";
    private static final int IDENTITY_MAP_CAPACITY = 1000;
    private static final String BOOLEAN_PATTERN = "false|true";

    private final List<SchemaProvider> schemaProviders;
    private final SchemaRegistryClient client;
    private final ListVersionsHandler listVersionsHandler;
    private final GetVersionHandler getVersionHandler;
    private final GetSubjectSchemaVersionHandler getSubjectSchemaVersionHandler;
    private final AutoRegistrationHandler autoRegistrationHandler;
    private final DeleteSubjectHandler deleteSubjectHandler;
    private final AllSubjectsHandler allSubjectsHandler;
    private final WireMockServer mockSchemaRegistry;

    /**
     * Create a new {@code SchemaRegistryMock} from {@link SchemaProvider SchemaProviders}.
     *
     * @param schemaProviders List of {@link SchemaProvider}. If null, {@link AvroSchemaProvider} will be used.
     */
    public SchemaRegistryMock(final List<SchemaProvider> schemaProviders) {
        this.schemaProviders = Optional.ofNullable(schemaProviders)
                .orElseGet(() -> Collections.singletonList(new AvroSchemaProvider()));
        this.client = new MockSchemaRegistryClient(schemaProviders);
        this.listVersionsHandler = new ListVersionsHandler(this);
        this.getVersionHandler = new GetVersionHandler(this);
        this.getSubjectSchemaVersionHandler = new GetSubjectSchemaVersionHandler(this);
        this.autoRegistrationHandler = new AutoRegistrationHandler(this);
        this.deleteSubjectHandler = new DeleteSubjectHandler(this);
        this.allSubjectsHandler = new AllSubjectsHandler(this);
        this.mockSchemaRegistry =
                new WireMockServer(WireMockConfiguration.wireMockConfig().dynamicPort().extensions(this.extensions()));
    }

    /**
     * Create a new {@code SchemaRegistryMock} with default {@link SchemaProvider SchemaProviders}.
     *
     * @see #SchemaRegistryMock(List)
     */
    public SchemaRegistryMock() {
        this(null);
    }

    private static MappingBuilder getSchema(final int id) {
        return WireMock.get(WireMock.urlPathEqualTo(SCHEMA_BY_ID_PATTERN + (Integer) id))
                .withQueryParam("fetchMaxId", WireMock.matching(BOOLEAN_PATTERN));
    }

    private static MappingBuilder deleteSubject(final String subject) {
        return WireMock.delete(WireMock.urlPathEqualTo(ALL_SUBJECT_PATTERN + "/" + subject))
                .withQueryParam("permanent", WireMock.equalTo("false"));
    }

    private static MappingBuilder postSubjectVersion(final String subject) {
        return WireMock.post(WireMock.urlPathMatching(ALL_SUBJECT_PATTERN + "/" + subject))
                .withQueryParam("deleted", WireMock.matching(BOOLEAN_PATTERN))
                .withQueryParam("normalize", WireMock.matching(BOOLEAN_PATTERN));
    }

    private static MappingBuilder getSubjectVersions(final String subject) {
        return WireMock.get(WireMock.urlPathEqualTo(ALL_SUBJECT_PATTERN + "/" + subject + "/versions"))
                .withQueryParam("deletedOnly", WireMock.matching(BOOLEAN_PATTERN))
                .withQueryParam("deleted", WireMock.matching(BOOLEAN_PATTERN));
    }

    private static MappingBuilder getSubject(final String subject) {
        return WireMock.get(
                WireMock.urlPathMatching(ALL_SUBJECT_PATTERN + "/" + subject + "/versions/(?:latest|\\d+)"));
    }

    private static SchemaString parsedSchemaToSchemaString(final ParsedSchema parsedSchema) {
        final SchemaString schemaString = new SchemaString(parsedSchema.toString());
        schemaString.setSchemaType(parsedSchema.schemaType());
        schemaString.setReferences(parsedSchema.references());
        return schemaString;
    }

    private static IllegalStateException internalError(final Exception e) {
        return new IllegalStateException("Internal error in mock schema registry client", e);
    }

    private static String getKeySubject(final String topic) {
        return topic + "-key";
    }

    private static String getValueSubject(final String topic) {
        return topic + "-value";
    }

    /**
     * Start the {@code SchemaRegistryMock}. Subsequent calls will have no effect.
     */
    public void start() {
        if (this.mockSchemaRegistry.isRunning()) {
            return;
        }

        this.mockSchemaRegistry.start();
        this.mockSchemaRegistry.stubFor(WireMock.get(WireMock.urlPathMatching(SCHEMA_PATH_PATTERN))
                .willReturn(WireMock.aResponse().withStatus(HTTP_NOT_FOUND)));
        this.mockSchemaRegistry.stubFor(WireMock.post(WireMock.urlPathMatching(SCHEMA_PATH_PATTERN))
                .willReturn(WireMock.aResponse().withTransformers(this.autoRegistrationHandler.getName())));
        this.mockSchemaRegistry.stubFor(WireMock.get(WireMock.urlPathMatching(SCHEMA_PATH_PATTERN + "/(?:latest|\\d+)"))
                .willReturn(WireMock.aResponse().withStatus(HTTP_NOT_FOUND)));
        this.mockSchemaRegistry.stubFor(WireMock.get(WireMock.urlPathMatching(SCHEMA_BY_ID_PATTERN + "\\d+"))
                .willReturn(WireMock.aResponse().withStatus(HTTP_NOT_FOUND)));
        this.mockSchemaRegistry.stubFor(WireMock.delete(WireMock.urlPathMatching(ALL_SUBJECT_PATTERN + "/[^/]+"))
                .willReturn(WireMock.aResponse().withStatus(HTTP_NOT_FOUND)));
        this.mockSchemaRegistry.stubFor(WireMock.get(WireMock.urlPathMatching(ALL_SUBJECT_PATTERN))
                .willReturn(WireMock.aResponse().withTransformers(this.allSubjectsHandler.getName())));
        this.mockSchemaRegistry.stubFor(postSubjectVersion("[^/]+")
                .willReturn(WireMock.aResponse().withStatus(HTTP_NOT_FOUND)));
    }

    /**
     * Stop the {@code SchemaRegistryMock}.
     */
    public void stop() {
        this.mockSchemaRegistry.stop();
    }

    /**
     * Register a key {@link Schema} for the topic
     *
     * @param topic topic to register key schema for
     * @param schema schema to be registered
     * @return Schema id
     * @see #registerKeySchema(String, ParsedSchema)
     */
    public int registerKeySchema(final String topic, final Schema schema) {
        return this.register(getKeySubject(topic), new AvroSchema(schema));
    }

    /**
     * Register a value {@link Schema} for the topic
     *
     * @param topic topic to register value schema for
     * @param schema schema to be registered
     * @return Schema id
     * @see #registerValueSchema(String, ParsedSchema)
     */
    public int registerValueSchema(final String topic, final Schema schema) {
        return this.register(getValueSubject(topic), new AvroSchema(schema));
    }

    /**
     * Register a key {@link ParsedSchema} for the topic using {@link TopicNameStrategy}
     *
     * @param topic topic to register key schema for
     * @param schema schema to be registered
     * @return Schema id
     */
    public int registerKeySchema(final String topic, final ParsedSchema schema) {
        return this.register(getKeySubject(topic), schema);
    }

    /**
     * Register a value {@link ParsedSchema} for the topic using {@link TopicNameStrategy}
     *
     * @param topic topic to register value schema for
     * @param schema schema to be registered
     * @return Schema id
     */
    public int registerValueSchema(final String topic, final ParsedSchema schema) {
        return this.register(getValueSubject(topic), schema);
    }

    /**
     * Delete all key schemas associated with the given subject. Uses {@link TopicNameStrategy}.
     *
     * @param subject subject to delete key schemas for
     * @return Ids of deleted schemas
     */
    public List<Integer> deleteKeySchema(final String subject) {
        return this.delete(getKeySubject(subject));
    }

    /**
     * Delete all value schemas associated with the given subject. Uses {@link TopicNameStrategy}.
     *
     * @param subject subject to delete value schemas for
     * @return Ids of deleted schemas
     */
    public List<Integer> deleteValueSchema(final String subject) {
        return this.delete(getValueSubject(subject));
    }

    /**
     * Create {@link CachedSchemaRegistryClient} for this mock using no additional config.
     *
     * @return {@code SchemaRegistryClient}
     * @see #getSchemaRegistryClient(Map)
     */
    public SchemaRegistryClient getSchemaRegistryClient() {
        return this.getSchemaRegistryClient(Collections.emptyMap());
    }

    public String getUrl() {
        return "http://localhost:" + this.mockSchemaRegistry.port();
    }

    /**
     * Create {@link CachedSchemaRegistryClient} for this mock.
     *
     * @param config config passed to
     * {@link CachedSchemaRegistryClient#CachedSchemaRegistryClient(List, int, List, Map)}
     * @return {@code SchemaRegistryClient}
     */
    public SchemaRegistryClient getSchemaRegistryClient(final Map<String, ?> config) {
        return new CachedSchemaRegistryClient(Collections.singletonList(this.getUrl()), IDENTITY_MAP_CAPACITY,
                this.schemaProviders,
                config);
    }

    int register(final String subject, final ParsedSchema schema) {
        try {
            final int id = this.client.register(subject, schema);
            log.debug("Registered schema {}", id);
            // add stubs for the new subject
            this.mockSchemaRegistry.stubFor(getSchema(id)
                    .willReturn(ResponseDefinitionBuilder.okForJson(parsedSchemaToSchemaString(schema))));
            this.mockSchemaRegistry.stubFor(deleteSubject(subject)
                    .willReturn(WireMock.aResponse().withTransformers(this.deleteSubjectHandler.getName())));
            this.mockSchemaRegistry.stubFor(getSubjectVersions(subject)
                    .willReturn(WireMock.aResponse().withTransformers(this.listVersionsHandler.getName())));
            this.mockSchemaRegistry.stubFor(getSubject(subject)
                    .willReturn(WireMock.aResponse().withTransformers(this.getVersionHandler.getName())));
            this.mockSchemaRegistry.stubFor(postSubjectVersion(subject)
                    .willReturn(WireMock.aResponse().withTransformers(this.getSubjectSchemaVersionHandler.getName())));

            return id;
        } catch (final IOException | RestClientException e) {
            throw internalError(e);
        }
    }

    List<Integer> delete(final String subject) {
        try {
            final List<Integer> ids = this.client.deleteSubject(subject);

            // remove stub for each version as well as stubs for subject
            ids.forEach(id -> this.mockSchemaRegistry.removeStub(getSchema(id)));
            this.mockSchemaRegistry.removeStub(deleteSubject(subject));
            this.mockSchemaRegistry.removeStub(getSubjectVersions(subject));
            this.mockSchemaRegistry.removeStub(getSubject(subject));
            this.mockSchemaRegistry.removeStub(postSubjectVersion(subject));
            return ids;
        } catch (final IOException | RestClientException e) {
            throw internalError(e);
        }
    }

    List<Integer> listVersions(final String subject) {
        log.debug("Listing all versions for subject {}", subject);
        try {
            return this.client.getAllVersions(subject);
        } catch (final IOException | RestClientException e) {
            throw internalError(e);
        }
    }

    io.confluent.kafka.schemaregistry.client.rest.entities.Schema getSchema(final String subject,
            final ParsedSchema parsedSchema) {
        log.debug("Getting schema version for subject {}", subject);
        try {
            final int version = this.client.getVersion(subject, parsedSchema);
            final int id = this.client.getId(subject, parsedSchema);
            return new io.confluent.kafka.schemaregistry.client.rest.entities.Schema(subject, version, id,
                    parsedSchema.schemaType(), parsedSchema.references(), parsedSchema.canonicalString());
        } catch (final IOException | RestClientException e) {
            throw internalError(e);
        }
    }

    SchemaMetadata getSubjectVersion(final String subject, final Object version) {
        log.debug("Requesting version {} for subject {}", version, subject);
        try {
            if (version instanceof String && "latest".equals(version)) {
                return this.client.getLatestSchemaMetadata(subject);
            } else if (version instanceof Number) {
                return this.client.getSchemaMetadata(subject, ((Number) version).intValue());
            } else {
                throw new IllegalArgumentException("Only 'latest' or integer versions are allowed");
            }
        } catch (final IOException | RestClientException e) {
            throw internalError(e);
        }
    }

    Collection<String> listAllSubjects() {
        try {
            return this.client.getAllSubjects();
        } catch (final IOException | RestClientException e) {
            throw internalError(e);
        }
    }

    ParsedSchema parseSchema(final RegisterSchemaRequest registerSchemaRequest) {
        final String schemaType = registerSchemaRequest.getSchemaType();
        final String schema = registerSchemaRequest.getSchema();
        final List<SchemaReference> references = Optional.ofNullable(registerSchemaRequest.getReferences())
                .orElse(Collections.emptyList());
        final Optional<ParsedSchema> schemaOptional = this.client.parseSchema(schemaType, schema, references);
        return schemaOptional.orElseThrow(() -> new RuntimeException("Could not parse schema"));
    }

    private Extension[] extensions() {
        return Stream.of(
                this.autoRegistrationHandler,
                this.listVersionsHandler,
                this.getVersionHandler,
                this.getSubjectSchemaVersionHandler,
                this.deleteSubjectHandler,
                this.allSubjectsHandler
        ).map(ErrorResponseTransformer::new).toArray(Extension[]::new);
    }

}
