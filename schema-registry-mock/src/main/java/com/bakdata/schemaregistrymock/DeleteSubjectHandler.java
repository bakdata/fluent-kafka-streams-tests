package com.bakdata.schemaregistrymock;

import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.common.FileSource;
import com.github.tomakehurst.wiremock.extension.Parameters;
import com.github.tomakehurst.wiremock.http.Request;
import com.github.tomakehurst.wiremock.http.ResponseDefinition;
import java.util.List;

class DeleteSubjectHandler extends SubjectsHandler {
    private final SchemaRegistryMock schemaRegistryMock;

    DeleteSubjectHandler(final SchemaRegistryMock schemaRegistryMock) {
        this.schemaRegistryMock = schemaRegistryMock;
    }

    @Override
    public ResponseDefinition transform(final Request request, final ResponseDefinition responseDefinition,
            final FileSource files, final Parameters parameters) {
        final String subject = removeQueryParameters(this.getSubject(request));
        final List<Integer> ids = this.schemaRegistryMock.delete(subject);
        return ResponseDefinitionBuilder.jsonResponse(ids);
    }

    @Override
    public String getName() {
        return DeleteSubjectHandler.class.getSimpleName();
    }
}
