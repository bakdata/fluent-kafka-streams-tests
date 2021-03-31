package com.bakdata.schemaregistrymock;

import com.github.tomakehurst.wiremock.extension.ResponseDefinitionTransformer;
import com.github.tomakehurst.wiremock.http.Request;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;

abstract class SubjectsHandler extends ResponseDefinitionTransformer {
    // Expected url pattern /subjects(/.*-value/versions)
    protected final Splitter urlSplitter = Splitter.on('/').omitEmptyStrings();

    @Override
    public boolean applyGlobally() {
        return false;
    }

    protected String getSubject(final Request request) {
        return Iterables.get(this.urlSplitter.split(request.getUrl()), 1);
    }

    static String removeQueryParameters(final String url) {
        final int index = url.indexOf('?');
        return index == -1 ? url : url.substring(0, index);
    }
}
