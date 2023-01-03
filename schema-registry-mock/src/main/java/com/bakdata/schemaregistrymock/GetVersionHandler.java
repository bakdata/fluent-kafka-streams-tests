/*
 * MIT License
 *
 * Copyright (c) 2023 bakdata GmbH
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

import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.common.FileSource;
import com.github.tomakehurst.wiremock.extension.Parameters;
import com.github.tomakehurst.wiremock.http.Request;
import com.github.tomakehurst.wiremock.http.ResponseDefinition;
import com.google.common.collect.Iterables;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;

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
