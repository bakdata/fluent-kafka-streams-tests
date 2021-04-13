package com.bakdata.schemaregistrymock;

import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.common.FileSource;
import com.github.tomakehurst.wiremock.extension.Parameters;
import com.github.tomakehurst.wiremock.http.Request;
import com.github.tomakehurst.wiremock.http.ResponseDefinition;
import java.util.Collection;

class AllSubjectsHandler extends SubjectsHandler {
    private final SchemaRegistryMock schemaRegistryMock;

    AllSubjectsHandler(final SchemaRegistryMock schemaRegistryMock) {
        this.schemaRegistryMock = schemaRegistryMock;
    }

    @Override
    public ResponseDefinition transform(final Request request, final ResponseDefinition responseDefinition,
            final FileSource files, final Parameters parameters) {
        final Collection<String> body = this.schemaRegistryMock.listAllSubjects();
        return ResponseDefinitionBuilder.jsonResponse(body);
    }

    @Override
    public String getName() {
        return AllSubjectsHandler.class.getSimpleName();
    }
}
