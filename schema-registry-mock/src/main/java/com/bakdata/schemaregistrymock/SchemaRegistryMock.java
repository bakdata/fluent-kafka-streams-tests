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

import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.FileSource;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.extension.Parameters;
import com.github.tomakehurst.wiremock.extension.ResponseDefinitionTransformer;
import com.github.tomakehurst.wiremock.http.Request;
import com.github.tomakehurst.wiremock.http.ResponseDefinition;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;
import com.github.tomakehurst.wiremock.matching.UrlPattern;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.ErrorMessage;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaResponse;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
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
    private static final String SCHEMA_VERSION_PATTERN = "/subjects/[^/]+\\?deleted=true";
    private static final int IDENTITY_MAP_CAPACITY = 1000;

    private final ListVersionsHandler listVersionsHandler = new ListVersionsHandler();
    private final GetVersionHandler getVersionHandler = new GetVersionHandler();
    private final AutoRegistrationHandler autoRegistrationHandler = new AutoRegistrationHandler();
    private final DeleteSubjectHandler deleteSubjectHandler = new DeleteSubjectHandler();
    private final AllSubjectsHandler allSubjectsHandler = new AllSubjectsHandler();
    private final SchemaVersionHandler schemaVersionHandler = new SchemaVersionHandler();
    private final WireMockServer mockSchemaRegistry = new WireMockServer(
            WireMockConfiguration.wireMockConfig().dynamicPort()
                    .extensions(this.autoRegistrationHandler, this.listVersionsHandler, this.getVersionHandler,
                            this.deleteSubjectHandler, this.allSubjectsHandler, this.schemaVersionHandler));
    private final SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();

    private static UrlPattern getSchemaPattern(final Integer id) {
        return WireMock.urlEqualTo(SCHEMA_BY_ID_PATTERN + id);
    }

    private static UrlPattern getSubjectPattern(final String subject) {
        return WireMock.urlEqualTo(ALL_SUBJECT_PATTERN + "/" + subject);
    }

    private static UrlPattern getSubjectVersionsPattern(final String subject) {
        return WireMock.urlEqualTo(ALL_SUBJECT_PATTERN + "/" + subject + "/versions");
    }

    private static UrlPathPattern getSubjectVersionPattern(final String subject) {
        return WireMock.urlPathMatching(ALL_SUBJECT_PATTERN + "/" + subject + "/versions/(?:latest|\\d+)");
    }

    public void start() {
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
        this.mockSchemaRegistry.stubFor(WireMock.post(WireMock.urlMatching(SCHEMA_VERSION_PATTERN))
                .willReturn(WireMock.aResponse().withTransformers(this.schemaVersionHandler.getName())));
    }

    public void stop() {
        this.mockSchemaRegistry.stop();
    }

    public int registerKeySchema(final String topic, final Schema schema) {
        return this.register(topic + "-key", schema);
    }

    public int registerValueSchema(final String topic, final Schema schema) {
        return this.register(topic + "-value", schema);
    }

    public List<Integer> deleteKeySchema(final String subject) {
        return this.delete(subject + "-key");
    }

    public List<Integer> deleteValueSchema(final String subject) {
        return this.delete(subject + "-value");
    }

    public SchemaRegistryClient getSchemaRegistryClient() {
        return new CachedSchemaRegistryClient(this.getUrl(), IDENTITY_MAP_CAPACITY);
    }

    public String getUrl() {
        return "http://localhost:" + this.mockSchemaRegistry.port();
    }

    private int register(final String subject, final Schema schema) {
        try {
            final int id = this.schemaRegistryClient.register(subject, schema);
            this.mockSchemaRegistry.stubFor(WireMock.get(getSchemaPattern(id))
                    .willReturn(ResponseDefinitionBuilder.okForJson(new SchemaString(schema.toString()))));
            this.mockSchemaRegistry.stubFor(WireMock.delete(getSubjectPattern(subject))
                    .willReturn(WireMock.aResponse().withTransformers(this.deleteSubjectHandler.getName())));
            this.mockSchemaRegistry.stubFor(WireMock.get(getSubjectVersionsPattern(subject))
                    .willReturn(WireMock.aResponse().withTransformers(this.listVersionsHandler.getName())));
            this.mockSchemaRegistry.stubFor(WireMock.get(getSubjectVersionPattern(subject))
                    .willReturn(WireMock.aResponse().withTransformers(this.getVersionHandler.getName())));
            log.debug("Registered schema {}", id);
            return id;
        } catch (final IOException | RestClientException e) {
            throw new IllegalStateException("Internal error in mock schema registry client", e);
        }
    }

    private List<Integer> delete(final String subject) {
        try {
            final List<Integer> ids = this.schemaRegistryClient.getAllVersions(subject);
            ids.forEach(id -> this.mockSchemaRegistry.removeStub(WireMock.get(getSchemaPattern(id))));
            this.mockSchemaRegistry.removeStub(WireMock.delete(getSubjectPattern(subject)));
            this.mockSchemaRegistry.removeStub(WireMock.get(getSubjectVersionsPattern(subject)));
            this.mockSchemaRegistry.removeStub(WireMock.get(getSubjectVersionPattern(subject)));
            this.schemaRegistryClient.deleteSubject(subject);
            return ids;
        } catch (final IOException | RestClientException e) {
            throw new IllegalStateException("Internal error in mock schema registry client", e);
        }
    }

    private List<Integer> listVersions(final String subject) {
        log.debug("Listing all versions for subject {}", subject);
        try {
            return this.schemaRegistryClient.getAllVersions(subject);
        } catch (final IOException | RestClientException e) {
            throw new IllegalStateException("Internal error in mock schema registry client", e);
        }
    }

    private SchemaMetadata getSubjectVersion(final String subject, final Object version) {
        log.debug("Requesting version {} for subject {}", version, subject);
        try {
            if (version instanceof String && version.equals("latest")) {
                return this.schemaRegistryClient.getLatestSchemaMetadata(subject);
            } else if (version instanceof Number) {
                return this.schemaRegistryClient.getSchemaMetadata(subject, ((Number) version).intValue());
            } else {
                throw new IllegalArgumentException("Only 'latest' or integer versions are allowed");
            }
        } catch (final IOException | RestClientException e) {
            throw new IllegalStateException("Internal error in mock schema registry client", e);
        }
    }

    private Collection<String> listAllSubjects() {
        try {
            return this.schemaRegistryClient.getAllSubjects();
        } catch (final IOException | RestClientException e) {
            throw new IllegalStateException("Internal error in mock schema registry client", e);
        }
    }

    private abstract static class SubjectsHandler extends ResponseDefinitionTransformer {
        // Expected url pattern /subjects(/.*-value/versions)
        protected final Splitter urlSplitter = Splitter.on('/').omitEmptyStrings();

        @Override
        public boolean applyGlobally() {
            return false;
        }

        protected String getSubject(final Request request) {
            return Iterables.get(this.urlSplitter.split(request.getUrl()), 1);
        }
    }

    private class AutoRegistrationHandler extends SubjectsHandler {

        @Override
        public ResponseDefinition transform(final Request request, final ResponseDefinition responseDefinition,
                                            final FileSource files, final Parameters parameters) {
            final String subject = Iterables.get(this.urlSplitter.split(request.getUrl()), 1);
            try {
                final int id = SchemaRegistryMock.this.register(subject,
                        new Schema.Parser()
                                .parse(RegisterSchemaRequest.fromJson(request.getBodyAsString()).getSchema()));
                final RegisterSchemaResponse registerSchemaResponse = new RegisterSchemaResponse();
                registerSchemaResponse.setId(id);
                return ResponseDefinitionBuilder.jsonResponse(registerSchemaResponse);
            } catch (final IOException e) {
                throw new IllegalArgumentException("Cannot parse schema registration request", e);
            }
        }

        @Override
        public String getName() {
            return AutoRegistrationHandler.class.getSimpleName();
        }
    }

    private class ListVersionsHandler extends SubjectsHandler {

        @Override
        public ResponseDefinition transform(final Request request, final ResponseDefinition responseDefinition,
                                            final FileSource files, final Parameters parameters) {
            final List<Integer> versions = SchemaRegistryMock.this.listVersions(this.getSubject(request));
            log.debug("Got versions {}", versions);
            return ResponseDefinitionBuilder.jsonResponse(versions);
        }

        @Override
        public String getName() {
            return ListVersionsHandler.class.getSimpleName();
        }
    }

    private class GetVersionHandler extends SubjectsHandler {

        @Override
        public ResponseDefinition transform(final Request request, final ResponseDefinition responseDefinition,
                                            final FileSource files, final Parameters parameters) {
            final String versionStr = Iterables.get(this.urlSplitter.split(request.getUrl()), 3);
            final SchemaMetadata metadata;
            if (versionStr.equals("latest")) {
                metadata = SchemaRegistryMock.this.getSubjectVersion(this.getSubject(request), versionStr);
            } else {
                final int version = Integer.parseInt(versionStr);
                metadata = SchemaRegistryMock.this.getSubjectVersion(this.getSubject(request), version);
            }
            return ResponseDefinitionBuilder.jsonResponse(metadata);
        }

        @Override
        public String getName() {
            return GetVersionHandler.class.getSimpleName();
        }
    }

    private class DeleteSubjectHandler extends SubjectsHandler {
        @Override
        public ResponseDefinition transform(final Request request, final ResponseDefinition responseDefinition,
                                            final FileSource files, final Parameters parameters) {
            final List<Integer> ids = SchemaRegistryMock.this.delete(this.getSubject(request));
            return ResponseDefinitionBuilder.jsonResponse(ids);
        }

        @Override
        public String getName() {
            return DeleteSubjectHandler.class.getSimpleName();
        }
    }

    private class AllSubjectsHandler extends SubjectsHandler {
        @Override
        public ResponseDefinition transform(final Request request, final ResponseDefinition responseDefinition,
                                            final FileSource files, final Parameters parameters) {
            final Collection<String> body = SchemaRegistryMock.this.listAllSubjects();
            return ResponseDefinitionBuilder.jsonResponse(body);
        }

        @Override
        public String getName() {
            return AllSubjectsHandler.class.getSimpleName();
        }
    }

    private class SchemaVersionHandler extends SubjectsHandler {
        @Override
        public ResponseDefinition transform(final Request request, final ResponseDefinition responseDefinition,
                                            final FileSource files, final Parameters parameters) {
            try {
                final Schema schema = new Schema.Parser()
                        .parse(RegisterSchemaRequest.fromJson(request.getBodyAsString()).getSchema());
                final String subject = this.getSubject(request);
                final int schemaVersion = SchemaRegistryMock.this.schemaRegistryClient.getVersion(subject, schema);
                final int schemaId = SchemaRegistryMock.this.schemaRegistryClient.getId(subject, schema);

                return ResponseDefinitionBuilder
                        .jsonResponse(new io.confluent.kafka.schemaregistry.client.rest.entities.Schema(
                                subject, schemaVersion, schemaId, schema.toString()
                        ));
            } catch (final IOException | RestClientException e) {
                final ErrorMessage error = new ErrorMessage(HTTP_BAD_REQUEST, "Cannot fetch schema version");
                return ResponseDefinitionBuilder.jsonResponse(error, HTTP_BAD_REQUEST);
            }
        }

        @Override
        public String getName() {
            return SchemaVersionHandler.class.getSimpleName();
        }

        @Override
        protected String getSubject(final Request request) {
            String subject = super.getSubject(request);
            // remove request parameters
            if (subject.contains("?")) {
                subject = subject.split("\\?")[0];
            }
            return subject;
        }
    }

}
