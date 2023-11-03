/*
 * MIT License
 *
 * Copyright (c) 2023 bakdata
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

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.extension.ResponseDefinitionTransformerV2;
import com.github.tomakehurst.wiremock.http.ResponseDefinition;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import com.github.tomakehurst.wiremock.stubbing.ServeEvent;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

@ExtendWith(SoftAssertionsExtension.class)
class ErrorResponseTransformerTest {
    @InjectSoftAssertions
    private SoftAssertions softly;

    static final ErrorResponseTransformer TRANSFORMER =
            new ErrorResponseTransformer(new FailingResponseTransformer());

    @RegisterExtension
    static WireMockExtension wireMock = WireMockExtension.newInstance()
            .options(WireMockConfiguration.wireMockConfig()
                    .dynamicPort()
                    .extensions(TRANSFORMER))
            .build();

    @Test
    void shouldTransformError() {
        wireMock.stubFor(WireMock.any(WireMock.anyUrl())
                .willReturn(WireMock.aResponse().withTransformers(TRANSFORMER.getName())));

        final CachedSchemaRegistryClient cachedSchemaRegistryClient =
                new CachedSchemaRegistryClient(wireMock.baseUrl(), 10);

        this.softly.assertThatExceptionOfType(RestClientException.class)
                .isThrownBy(cachedSchemaRegistryClient::getAllSubjects)
                .satisfies(e -> {
                    this.softly.assertThat(e.getErrorCode()).isEqualTo(500);
                    this.softly.assertThat(e.getStatus()).isEqualTo(500);
                    this.softly.assertThat(e).hasMessageContaining("Test error");
                });
    }

    static class FailingResponseTransformer implements ResponseDefinitionTransformerV2 {

        @Override
        public ResponseDefinition transform(final ServeEvent serveEvent) {
            throw new RuntimeException("Test error");
        }

        @Override
        public String getName() {
            return this.getClass().getSimpleName();
        }
    }
}
