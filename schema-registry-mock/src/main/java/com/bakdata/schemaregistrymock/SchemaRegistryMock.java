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

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.extension.ResponseDefinitionTransformer;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;
import com.github.tomakehurst.wiremock.matching.UrlPattern;
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
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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

    private final List<SchemaProvider> schemaProviders;
    private final SchemaRegistryClient client;
    private final ListVersionsHandler listVersionsHandler;
    private final GetVersionHandler getVersionHandler;
    private final GetSubjectSchemaVersionHandler getSubjectSchemaVersionHandler;
    private final AutoRegistrationHandler autoRegistrationHandler;
    private final DeleteSubjectHandler deleteSubjectHandler;
    private final AllSubjectsHandler allSubjectsHandler;
    private final WireMockServer mockSchemaRegistry;

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

    public SchemaRegistryMock() {
        this(null);
    }

    private static UrlPattern getSchemaPattern(final Integer id) {
        return WireMock.urlPathEqualTo(SCHEMA_BY_ID_PATTERN + id);
    }

    private static UrlPattern getDeleteSubjectPattern(final String subject) {
        return WireMock.urlEqualTo(ALL_SUBJECT_PATTERN + "/" + subject + "?permanent=false");
    }

    private static UrlPattern getSubjectSchemaVersionPattern(final String subject) {
        return WireMock.urlPathMatching(ALL_SUBJECT_PATTERN + "/" + subject
                + "(?:\\?(?:deleted|normalize)=(?:true|false)(?:&(?:deleted|normalize)=(?:true|false))*)?");
    }

    private static UrlPattern getSubjectVersionsPattern(final String subject) {
        return WireMock.urlEqualTo(ALL_SUBJECT_PATTERN + "/" + subject + "/versions");
    }

    private static UrlPathPattern getSubjectVersionPattern(final String subject) {
        return WireMock.urlPathMatching(ALL_SUBJECT_PATTERN + "/" + subject + "/versions/(?:latest|\\d+)");
    }

    private static SchemaString parsedSchemaToSchemaString(final ParsedSchema parsedSchema) {
        final SchemaString schemaString = new SchemaString(parsedSchema.toString());
        schemaString.setSchemaType(parsedSchema.schemaType());
        schemaString.setReferences(parsedSchema.references());
        return schemaString;
    }

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
        this.mockSchemaRegistry.stubFor(WireMock.post(getSubjectSchemaVersionPattern("[^/]+"))
                .willReturn(WireMock.aResponse().withStatus(HTTP_NOT_FOUND)));
    }

    public void stop() {
        this.mockSchemaRegistry.stop();
    }

    public int registerKeySchema(final String topic, final Schema schema) {
        return this.register(topic + "-key", new AvroSchema(schema));
    }

    public int registerValueSchema(final String topic, final Schema schema) {
        return this.register(topic + "-value", new AvroSchema(schema));
    }

    public int registerKeySchema(final String topic, final ParsedSchema schema) {
        return this.register(topic + "-key", schema);
    }

    public int registerValueSchema(final String topic, final ParsedSchema schema) {
        return this.register(topic + "-value", schema);
    }

    public List<Integer> deleteKeySchema(final String subject) {
        return this.delete(subject + "-key");
    }

    public List<Integer> deleteValueSchema(final String subject) {
        return this.delete(subject + "-value");
    }

    public SchemaRegistryClient getSchemaRegistryClient() {
        return this.getSchemaRegistryClient(Collections.emptyMap());
    }

    public SchemaRegistryClient getSchemaRegistryClient(final Map<String, ?> config) {
        return new CachedSchemaRegistryClient(Collections.singletonList(this.getUrl()), IDENTITY_MAP_CAPACITY,
                this.schemaProviders,
                config);
    }

    public String getUrl() {
        return "http://localhost:" + this.mockSchemaRegistry.port();
    }

    int register(final String subject, final ParsedSchema schema) {
        try {
            final int id = this.client.register(subject, schema);
            log.debug("Registered schema {}", id);
            // add stubs for the new subject
            this.mockSchemaRegistry.stubFor(WireMock.get(getSchemaPattern(id))
                    .withQueryParam("fetchMaxId", WireMock.matching("false|true"))
                    .willReturn(ResponseDefinitionBuilder.okForJson(parsedSchemaToSchemaString(schema))));
            this.mockSchemaRegistry.stubFor(WireMock.delete(getDeleteSubjectPattern(subject))
                    .willReturn(WireMock.aResponse().withTransformers(this.deleteSubjectHandler.getName())));
            this.mockSchemaRegistry.stubFor(WireMock.get(getSubjectVersionsPattern(subject))
                    .willReturn(WireMock.aResponse().withTransformers(this.listVersionsHandler.getName())));
            this.mockSchemaRegistry.stubFor(WireMock.get(getSubjectVersionPattern(subject))
                    .willReturn(WireMock.aResponse().withTransformers(this.getVersionHandler.getName())));
            this.mockSchemaRegistry.stubFor(WireMock.post(getSubjectSchemaVersionPattern(subject))
                    .willReturn(WireMock.aResponse().withTransformers(this.getSubjectSchemaVersionHandler.getName())));

            return id;
        } catch (final IOException | RestClientException e) {
            throw new IllegalStateException("Internal error in mock schema registry client", e);
        }
    }

    List<Integer> delete(final String subject) {
        try {
            final List<Integer> ids = this.client.deleteSubject(subject);

            // remove stub for each version as well as stubs for subject
            ids.forEach(id -> this.mockSchemaRegistry.removeStub(WireMock.get(getSchemaPattern(id))));
            this.mockSchemaRegistry.removeStub(WireMock.delete(getDeleteSubjectPattern(subject)));
            this.mockSchemaRegistry.removeStub(WireMock.get(getSubjectVersionsPattern(subject)));
            this.mockSchemaRegistry.removeStub(WireMock.get(getSubjectVersionPattern(subject)));
            this.mockSchemaRegistry.removeStub(WireMock.post(getSubjectSchemaVersionPattern(subject)));
            return ids;
        } catch (final IOException | RestClientException e) {
            throw new IllegalStateException("Internal error in mock schema registry client", e);
        }
    }

    List<Integer> listVersions(final String subject) {
        log.debug("Listing all versions for subject {}", subject);
        try {
            return this.client.getAllVersions(subject);
        } catch (final IOException | RestClientException e) {
            throw new IllegalStateException("Internal error in mock schema registry client", e);
        }
    }

    io.confluent.kafka.schemaregistry.client.rest.entities.Schema getSchema(final String subject,
            final ParsedSchema parsedSchema) {
        log.debug("Getting version for subject {}", subject);
        try {
            final int version = this.client.getVersion(subject, parsedSchema);
            final int id = this.client.getId(subject, parsedSchema);
            return new io.confluent.kafka.schemaregistry.client.rest.entities.Schema(subject, version, id,
                    parsedSchema.schemaType(), parsedSchema.references(), parsedSchema.canonicalString());
        } catch (final IOException | RestClientException e) {
            throw new IllegalStateException("Internal error in mock schema registry client", e);
        }
    }

    SchemaMetadata getSubjectVersion(final String subject, final Object version) {
        log.debug("Requesting version {} for subject {}", version, subject);
        try {
            if (version instanceof String && version.equals("latest")) {
                return this.client.getLatestSchemaMetadata(subject);
            } else if (version instanceof Number) {
                return this.client.getSchemaMetadata(subject, ((Number) version).intValue());
            } else {
                throw new IllegalArgumentException("Only 'latest' or integer versions are allowed");
            }
        } catch (final IOException | RestClientException e) {
            throw new IllegalStateException("Internal error in mock schema registry client", e);
        }
    }

    Collection<String> listAllSubjects() {
        try {
            return this.client.getAllSubjects();
        } catch (final IOException | RestClientException e) {
            throw new IllegalStateException("Internal error in mock schema registry client", e);
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

    private ResponseDefinitionTransformer[] extensions() {
        return new ResponseDefinitionTransformer[]{
                this.autoRegistrationHandler,
                this.listVersionsHandler,
                this.getVersionHandler,
                this.getSubjectSchemaVersionHandler,
                this.deleteSubjectHandler,
                this.allSubjectsHandler
        };
    }
}
