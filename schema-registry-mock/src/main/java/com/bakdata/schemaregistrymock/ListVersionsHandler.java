package com.bakdata.schemaregistrymock;

import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.common.FileSource;
import com.github.tomakehurst.wiremock.extension.Parameters;
import com.github.tomakehurst.wiremock.http.Request;
import com.github.tomakehurst.wiremock.http.ResponseDefinition;
import java.util.List;

class ListVersionsHandler extends SubjectsHandler {

    private final SchemaRegistryMock schemaRegistryMock;

    ListVersionsHandler(final SchemaRegistryMock schemaRegistryMock) {
        this.schemaRegistryMock = schemaRegistryMock;
    }

    @Override
    public ResponseDefinition transform(final Request request, final ResponseDefinition responseDefinition,
            final FileSource files, final Parameters parameters) {
        final List<Integer> versions = this.schemaRegistryMock.listVersions(this.getSubject(request));
        return ResponseDefinitionBuilder.jsonResponse(versions);
    }

    @Override
    public String getName() {
        return ListVersionsHandler.class.getSimpleName();
    }
}
