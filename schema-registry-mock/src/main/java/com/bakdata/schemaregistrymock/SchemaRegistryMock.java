package com.bakdata.schemaregistrymock;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.FileSource;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.extension.Parameters;
import com.github.tomakehurst.wiremock.extension.ResponseDefinitionTransformer;
import com.github.tomakehurst.wiremock.http.Request;
import com.github.tomakehurst.wiremock.http.ResponseDefinition;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaResponse;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.IOException;

import static java.net.HttpURLConnection.HTTP_NOT_FOUND;


@Slf4j
public class SchemaRegistryMock implements BeforeEachCallback, AfterEachCallback {
    public static final String SCHEMA_REGISTRATION_PATTERN = "/subjects/[^/]+/versions";
    public static final String SCHEMA_BY_ID_PATTERN = "/schemas/ids/";
    private final AutoRegistrationHandler autoRegistrationHandler = new AutoRegistrationHandler();
    private final WireMockServer mockSchemaRegistry = new WireMockServer(WireMockConfiguration.wireMockConfig().dynamicPort().extensions(this.autoRegistrationHandler));
    private final SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();

    @Override
    public void afterEach(ExtensionContext context) {
        this.mockSchemaRegistry.stop();
    }

    @Override
    public void beforeEach(ExtensionContext context) {
        this.mockSchemaRegistry.start();
        this.mockSchemaRegistry.stubFor(WireMock.post(WireMock.urlPathMatching(SCHEMA_REGISTRATION_PATTERN))
                .willReturn(WireMock.aResponse().withTransformers(this.autoRegistrationHandler.getName())));
        this.mockSchemaRegistry.stubFor(WireMock.get(WireMock.urlPathMatching(SCHEMA_BY_ID_PATTERN + "*"))
                .willReturn(WireMock.aResponse().withStatus(HTTP_NOT_FOUND)));
    }

    public int registerKeySchema(String topic, Schema schema) throws IOException, RestClientException {
        return register(topic + "-key", schema);
    }

    public int registerValueSchema(String topic, Schema schema) throws IOException, RestClientException {
        return register(topic + "-value", schema);
    }

    private int register(String subject, Schema schema) throws IOException, RestClientException {
        final int id = this.schemaRegistryClient.register(subject, schema);
        this.mockSchemaRegistry.stubFor(WireMock.get(WireMock.urlEqualTo(SCHEMA_BY_ID_PATTERN + id))
                .willReturn(ResponseDefinitionBuilder.okForJson(new SchemaString(schema.toString()))));
        log.debug("Registered schema " + id);
        return id;
    }

    public SchemaRegistryClient getSchemaRegistryClient() {
        return new CachedSchemaRegistryClient(getUrl(), 1000);
    }

    public String getUrl() {
        return "http://localhost:" + this.mockSchemaRegistry.port();
    }

    public class AutoRegistrationHandler extends ResponseDefinitionTransformer {

        private Splitter urlSplitter;

        @Override
        public boolean applyGlobally() {
            return false;
        }

        @Override
        public ResponseDefinition transform(Request request, ResponseDefinition responseDefinition, FileSource files, Parameters parameters) {
            // Expected url pattern /subjects/.*-value/versions
            urlSplitter = Splitter.on('/').omitEmptyStrings();
            final String subject = Iterables.get(urlSplitter.split(request.getUrl()), 1);
            try {
                final int id = register(subject, new Schema.Parser().parse(RegisterSchemaRequest.fromJson(request.getBodyAsString()).getSchema()));
                final RegisterSchemaResponse registerSchemaResponse = new RegisterSchemaResponse();
                registerSchemaResponse.setId(id);
                return ResponseDefinitionBuilder.jsonResponse(registerSchemaResponse);
            } catch (IOException | RestClientException e) {
                throw new IllegalStateException("Error while registering the schema of " + subject, e);
            }
        }

        @Override
        public String getName() {
            return AutoRegistrationHandler.class.getSimpleName();
        }
    }
}
