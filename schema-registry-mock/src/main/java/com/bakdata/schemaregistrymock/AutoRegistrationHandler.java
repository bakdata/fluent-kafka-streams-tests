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
import com.github.tomakehurst.wiremock.http.ResponseDefinition;
import com.github.tomakehurst.wiremock.stubbing.ServeEvent;
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
    public ResponseDefinition transform(final ServeEvent serveEvent) {
        final String subject = Iterables.get(this.urlSplitter.split(serveEvent.getRequest().getUrl()), 1);
        try {
            final RegisterSchemaRequest schemaRequest =
                    RegisterSchemaRequest.fromJson(serveEvent.getRequest().getBodyAsString());
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
