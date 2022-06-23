/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.verifier.resolver;

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.json.JsonObjectMapperProvider;
import com.facebook.presto.verifier.prestoaction.PrestoActionConfig;
import com.facebook.presto.verifier.prestoaction.PrestoExceptionClassifier;
import com.facebook.presto.verifier.retry.RetryConfig;
import com.facebook.presto.verifier.retry.RetryDriver;
import com.fasterxml.jackson.core.type.TypeReference;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.List;
import java.util.Map;

import static com.facebook.airlift.http.client.Request.Builder.prepareGet;
import static com.facebook.airlift.http.client.StringResponseHandler.StringResponse;
import static com.facebook.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.Response.Status.OK;

public class ClusterSizeFetcher
        implements ClusterSizeSupplier
{
    private static final String PATH = "/v1/node";

    private final HttpClient httpClient;
    private final URI nodeResourceUri;
    private final RetryDriver<RuntimeException> networkRetry;

    public ClusterSizeFetcher(
            HttpClient httpClient,
            PrestoActionConfig prestoAddress,
            RetryConfig networkRetryConfig)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.nodeResourceUri = requireNonNull(getOnlyElement(prestoAddress.getHttpUris(PATH)), "nodeResourceUri is null");
        this.networkRetry = new RetryDriver<>(networkRetryConfig, PrestoExceptionClassifier::isClusterConnectionException, RuntimeException.class, e -> {});
    }

    @Override
    public int getClusterSize()
    {
        return networkRetry.run("fetchClusterSize", this::fetchClusterSize);
    }

    private int fetchClusterSize()
    {
        Request request = prepareGet()
                .setUri(nodeResourceUri)
                .setHeader(CONTENT_TYPE, APPLICATION_JSON)
                .build();

        StringResponse response = httpClient.execute(request, createStringResponseHandler());
        checkState(
                response.getStatusCode() == OK.getStatusCode(),
                "Invalid response: %s %s",
                response.getStatusCode(),
                response.getStatusMessage());

        List<Map<String, Object>> values;
        try {
            values = new JsonObjectMapperProvider().get().readValue(response.getBody(), new TypeReference<List<Map<String, Object>>>() {});
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return values.size();
    }
}
