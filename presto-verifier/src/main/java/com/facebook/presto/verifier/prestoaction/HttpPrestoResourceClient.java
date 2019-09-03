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
package com.facebook.presto.verifier.prestoaction;

import com.facebook.presto.verifier.retry.RetryConfig;
import com.facebook.presto.verifier.retry.RetryDriver;
import com.google.inject.Inject;
import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.Response.Status.OK;

public class HttpPrestoResourceClient
        implements PrestoResourceClient
{
    private final HttpClient httpClient;
    private final PrestoAddress prestoAddress;
    private final RetryDriver<RuntimeException> networkRetry;

    @Inject
    public HttpPrestoResourceClient(
            HttpClient httpClient,
            PrestoClusterConfig prestoAddress,
            RetryConfig networkRetryConfig)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.prestoAddress = requireNonNull(prestoAddress, "prestoAddress is null");
        this.networkRetry = new RetryDriver<>(networkRetryConfig, PrestoExceptionClassifier::isClusterConnectionException, RuntimeException.class, e -> {});
    }

    @Override
    public <R> R getJsonResponse(String path, JsonCodec<R> responseCodec)
    {
        return networkRetry.run(format("getJsonResponse()", path), () -> getJsonResponseOnce(path, responseCodec));
    }

    private <R> R getJsonResponseOnce(String path, JsonCodec<R> responseCodec)
    {
        Request request = prepareGet()
                .setUri(prestoAddress.getHttpUri(path))
                .setHeader(CONTENT_TYPE, APPLICATION_JSON)
                .build();

        JsonResponse<R> response = httpClient.execute(request, createFullJsonResponseHandler(responseCodec));
        checkState(
                response.getStatusCode() == OK.getStatusCode(),
                "Invalid response: %s %s",
                response.getStatusCode(),
                response.getStatusMessage());
        return response.getValue();
    }
}
