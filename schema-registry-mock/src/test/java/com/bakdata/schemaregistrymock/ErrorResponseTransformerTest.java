package com.bakdata.schemaregistrymock;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.FileSource;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.extension.Parameters;
import com.github.tomakehurst.wiremock.extension.ResponseDefinitionTransformer;
import com.github.tomakehurst.wiremock.http.Request;
import com.github.tomakehurst.wiremock.http.ResponseDefinition;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
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

    static class FailingResponseTransformer extends ResponseDefinitionTransformer {
        @Override
        public ResponseDefinition transform(final Request request, final ResponseDefinition responseDefinition,
                final FileSource files,
                final Parameters parameters) {
            throw new RuntimeException("Test error");
        }

        @Override
        public String getName() {
            return this.getClass().getSimpleName();
        }
    }
}
