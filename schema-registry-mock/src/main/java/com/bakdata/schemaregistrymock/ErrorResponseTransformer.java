package com.bakdata.schemaregistrymock;

import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.extension.ResponseDefinitionTransformerV2;
import com.github.tomakehurst.wiremock.http.ResponseDefinition;
import com.github.tomakehurst.wiremock.stubbing.ServeEvent;
import io.confluent.kafka.schemaregistry.client.rest.entities.ErrorMessage;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * {@link ResponseDefinitionTransformerV2} that wraps other {@link ResponseDefinitionTransformerV2} and transforms
 * potential errors.
 *
 * <p>
 * The {@link io.confluent.kafka.schemaregistry.client.SchemaRegistryClient} requires errors to be in the format of
 * {@link ErrorMessage}, so this class transforms any exception into this format.
 */
@Slf4j
@AllArgsConstructor
class ErrorResponseTransformer implements ResponseDefinitionTransformerV2 {
    // Confluent's error codes are a superset of HTTP error codes.
    // see https://docs.confluent.io/platform/current/kafka-rest/api.html#errors
    public static final int INTERNAL_SERVER_ERROR_CODE = 500;
    private final ResponseDefinitionTransformerV2 transformer;

    @Override
    public ResponseDefinition transform(final ServeEvent serveEvent) {
        try {
            return this.transformer.transform(serveEvent);
        } catch (final RuntimeException e) {
            log.warn("An exception occurred while handling the schema registry request '{} {}'",
                    serveEvent.getRequest().getMethod(), serveEvent.getRequest().getUrl(), e);
            final ErrorMessage body = new ErrorMessage(INTERNAL_SERVER_ERROR_CODE, e.getMessage());
            return ResponseDefinitionBuilder.jsonResponse(body, INTERNAL_SERVER_ERROR_CODE);
        }
    }

    @Override
    public String getName() {
        return this.transformer.getName();
    }

    @Override
    public boolean applyGlobally() {
        return this.transformer.applyGlobally();
    }
}
