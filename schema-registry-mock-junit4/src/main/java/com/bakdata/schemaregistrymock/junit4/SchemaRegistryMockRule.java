/*
 * MIT License
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
package com.bakdata.schemaregistrymock.junit4;

import com.bakdata.schemaregistrymock.SchemaRegistryMock;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * <p>The schema registry mock implements a few basic HTTP endpoints that are used by the Avro serdes.</p>
 * In particular,
 * <ul>
 * <li>you can register a schema</li>
 * <li>retrieve a schema by id.</li>
 * <li>list and get schema versions of a subject</li>
 * </ul>
 *
 * <p>If you use the TestTopology of the fluent Kafka Streams test, you don't have to interact with this class at
 * all.</p>
 *
 * <p>Without the test framework, you can use the mock as follows:</p>
 * <pre><code>
 * public class SchemaRegistryMockTest {
 *     {@literal @Rule}
 *     public final SchemaRegistryMockRule schemaRegistry = new SchemaRegistryMockRule();
 *
 *     {@literal @Test}
 *     public void shouldRegisterKeySchema() throws IOException, RestClientException {
 *         final Schema keySchema = this.createSchema("key_schema");
 *         final int id = this.schemaRegistry.registerKeySchema("test-topic", keySchema);
 *
 *         final Schema retrievedSchema = this.schemaRegistry.getSchemaRegistryClient().getById(id);
 *         assertThat(retrievedSchema).isEqualTo(keySchema);
 *     }
 * }</code></pre>
 *
 * To retrieve the url of the schema registry for a Kafka Streams config, please use {@link #getUrl()}
 */
public class SchemaRegistryMockRule extends SchemaRegistryMock implements TestRule {

    @Override
    public Statement apply(final Statement base, final Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                SchemaRegistryMockRule.this.start();
                try {
                    base.evaluate();
                } finally {
                    SchemaRegistryMockRule.this.stop();
                }
            }
        };
    }
}
