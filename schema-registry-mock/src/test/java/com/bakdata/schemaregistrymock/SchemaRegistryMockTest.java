/*
 * The MIT License
 *
 * Copyright (c) 2019 bakdata GmbH
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

import static org.assertj.core.api.Assertions.assertThat;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.util.Collections;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class SchemaRegistryMockTest {
    @RegisterExtension
    final SchemaRegistryMock schemaRegistry = new SchemaRegistryMock();

    @Test
    void shouldRegisterKeySchema() throws IOException, RestClientException {
        final Schema keySchema = this.createSchema("key_schema");
        final int id = this.schemaRegistry.registerKeySchema("test-topic", keySchema);

        final Schema retrievedSchema = this.schemaRegistry.getSchemaRegistryClient().getById(id);
        assertThat(retrievedSchema).isEqualTo(keySchema);
    }

    @Test
    void shouldRegisterValueSchema() throws IOException, RestClientException {
        final Schema valueSchema = this.createSchema("value_schema");
        final int id = this.schemaRegistry.registerValueSchema("test-topic", valueSchema);

        final Schema retrievedSchema = this.schemaRegistry.getSchemaRegistryClient().getById(id);
        assertThat(retrievedSchema).isEqualTo(valueSchema);
    }

    @Test
    void shouldRegisterKeySchemaWithClient() throws IOException, RestClientException {
        final Schema keySchema = this.createSchema("key_schema");
        final int id = this.schemaRegistry.getSchemaRegistryClient().register("test-topic-key", keySchema);

        final Schema retrievedSchema = this.schemaRegistry.getSchemaRegistryClient().getById(id);
        assertThat(retrievedSchema).isEqualTo(keySchema);
    }

    @Test
    void shouldRegisterValueSchemaWithClient() throws IOException, RestClientException {
        final Schema valueSchema = this.createSchema("value_schema");
        final int id = this.schemaRegistry.getSchemaRegistryClient().register("test-topic-value", valueSchema);

        final Schema retrievedSchema = this.schemaRegistry.getSchemaRegistryClient().getById(id);
        assertThat(retrievedSchema).isEqualTo(valueSchema);
    }

    private Schema createSchema(final String name) {
        return Schema.createRecord(name, "no doc", "", false, Collections.emptyList());
    }
}