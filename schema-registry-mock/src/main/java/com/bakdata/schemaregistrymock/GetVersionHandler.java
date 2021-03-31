package com.bakdata.schemaregistrymock;

import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.common.FileSource;
import com.github.tomakehurst.wiremock.extension.Parameters;
import com.github.tomakehurst.wiremock.http.Request;
import com.github.tomakehurst.wiremock.http.ResponseDefinition;
import com.google.common.collect.Iterables;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class GetVersionHandler extends SubjectsHandler {

    private final SchemaRegistryMock schemaRegistryMock;

    GetVersionHandler(final SchemaRegistryMock schemaRegistryMock) {
        this.schemaRegistryMock = schemaRegistryMock;
    }

    @Override
    public ResponseDefinition transform(final Request request, final ResponseDefinition responseDefinition,
            final FileSource files, final Parameters parameters) {
        final String versionStr = Iterables
                .get(this.urlSplitter.split(removeQueryParameters(request.getUrl())), 3);
        final SchemaMetadata metadata;
        if ("latest".equals(versionStr)) {
            metadata = this.schemaRegistryMock.getSubjectVersion(this.getSubject(request), versionStr);
        } else {
            final int version = Integer.parseInt(versionStr);
            metadata = this.schemaRegistryMock.getSubjectVersion(this.getSubject(request), version);
        }
        return ResponseDefinitionBuilder.jsonResponse(metadata);
    }

    @Override
    public String getName() {
        return GetVersionHandler.class.getSimpleName();
    }
}
