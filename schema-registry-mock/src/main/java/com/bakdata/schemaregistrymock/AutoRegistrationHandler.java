package com.bakdata.schemaregistrymock;

import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.common.FileSource;
import com.github.tomakehurst.wiremock.extension.Parameters;
import com.github.tomakehurst.wiremock.http.Request;
import com.github.tomakehurst.wiremock.http.ResponseDefinition;
import com.google.common.collect.Iterables;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaResponse;
import java.io.IOException;

class AutoRegistrationHandler extends SubjectsHandler {

    private final SchemaRegistryMock schemaRegistryMock;

    AutoRegistrationHandler(final SchemaRegistryMock schemaRegistryMock) {
        this.schemaRegistryMock = schemaRegistryMock;
    }

    @Override
    public ResponseDefinition transform(final Request request, final ResponseDefinition responseDefinition,
            final FileSource files, final Parameters parameters) {
        final String subject = Iterables.get(this.urlSplitter.split(request.getUrl()), 1);
        try {
            final RegisterSchemaRequest schemaRequest = RegisterSchemaRequest.fromJson(request.getBodyAsString());
            final ParsedSchema schema = this.schemaRegistryMock.parseSchema(schemaRequest);
            final int id = this.schemaRegistryMock.register(subject, schema);
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
