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
    private final RegistrationHandler registrationHandler = new RegistrationHandler();
    private final WireMockServer httpMock = new WireMockServer(WireMockConfiguration.wireMockConfig().dynamicPort().extensions(this.registrationHandler));
    private final SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();

    @Override
    public void afterEach(ExtensionContext context) {
        this.httpMock.stop();
    }

    @Override
    public void beforeEach(ExtensionContext context) {
        this.httpMock.start();
        this.httpMock.stubFor(WireMock.post(WireMock.urlPathMatching("/subjects/[^/]+/versions"))
                .willReturn(WireMock.aResponse().withTransformers(this.registrationHandler.getName())));
        this.httpMock.stubFor(WireMock.get(WireMock.urlPathMatching("/schemas/ids/.*")).willReturn(WireMock.aResponse().withStatus(HTTP_NOT_FOUND)));
    }

    public int register(String subject, Schema schema) throws IOException, RestClientException {
        final int id = this.schemaRegistryClient.register(subject, schema);
        this.httpMock.stubFor(WireMock.get(WireMock.urlEqualTo("/schemas/ids/" + id)).willReturn(ResponseDefinitionBuilder.okForJson(new SchemaString(schema.toString()))));
        log.debug("Registered schema " + id);
        return id;
    }

    public SchemaRegistryClient getSchemaRegistryClient() {
        return new CachedSchemaRegistryClient(url(), 1000);
    }

    public String url() {
        return "http://localhost:" + this.httpMock.port();
    }

    public class RegistrationHandler extends ResponseDefinitionTransformer {
        @Override
        public boolean applyGlobally() {
            return false;
        }

        @Override
        public ResponseDefinition transform(Request request, ResponseDefinition responseDefinition, FileSource files, Parameters parameters) {
            // url = "/subjects/.*-value/versions"
            final String subject = Iterables.get(Splitter.on('/').omitEmptyStrings().split(request.getUrl()), 1);
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
            return "RegistrationHandler";
        }
    }
}
