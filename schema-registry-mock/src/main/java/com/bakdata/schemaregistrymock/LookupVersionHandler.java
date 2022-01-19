package com.bakdata.schemaregistrymock;

import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.common.FileSource;
import com.github.tomakehurst.wiremock.extension.Parameters;
import com.github.tomakehurst.wiremock.http.Request;
import com.github.tomakehurst.wiremock.http.ResponseDefinition;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import java.io.IOException;

class LookupVersionHandler extends SubjectsHandler {

    private final SchemaRegistryMock schemaRegistryMock;

    LookupVersionHandler(final SchemaRegistryMock schemaRegistryMock) {
        this.schemaRegistryMock = schemaRegistryMock;
    }

    @Override
    public ResponseDefinition transform(final Request request, final ResponseDefinition responseDefinition,
            final FileSource files, final Parameters parameters) {
        final String subject = removeQueryParameters(this.getSubject(request));
        try {
            final RegisterSchemaRequest schemaRequest = RegisterSchemaRequest.fromJson(request.getBodyAsString());
            final ParsedSchema parsedSchema = this.schemaRegistryMock.parseSchema(schemaRequest);
            final Schema schema = this.schemaRegistryMock.getSchema(subject, parsedSchema);
            return ResponseDefinitionBuilder.jsonResponse(schema);
        } catch (final IOException e) {
            throw new IllegalArgumentException("Cannot parse schema registration request", e);
        }
    }

    @Override
    public String getName() {
        return LookupVersionHandler.class.getSimpleName();
    }
}
