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
package com.facebook.presto.tests.querystats;

import com.facebook.presto.execution.QueryStats;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;

import java.io.IOException;
import java.net.URI;
import java.util.Optional;

import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.ResponseHandlerUtils.propagate;

/**
 * Implementation of {@link QueryStatsClient} using Presto's /v1/query HTTP API.
 */
public class HttpQueryStatsClient
        implements QueryStatsClient
{
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final URI baseUri;

    public HttpQueryStatsClient(HttpClient httpClient, ObjectMapper objectMapper, URI baseUri)
    {
        this.httpClient = httpClient;
        this.objectMapper = objectMapper;
        this.baseUri = baseUri;
    }

    @Override
    public Optional<QueryStats> getQueryStats(String queryId)
    {
        URI uri = uriBuilderFrom(baseUri).appendPath("/v1/query").appendPath(queryId).build();
        Request request = prepareGet().setUri(uri).build();
        return httpClient.execute(request, new GetQueryStatsResponseHandler());
    }

    private final class GetQueryStatsResponseHandler
            implements ResponseHandler<Optional<QueryStats>, RuntimeException>
    {
        @Override
        public Optional<QueryStats> handleException(Request request, Exception exception)
        {
            throw propagate(request, exception);
        }

        @Override
        public Optional<QueryStats> handle(Request request, Response response)
        {
            if (response.getStatusCode() == HttpStatus.GONE.code()) {
                return Optional.empty();
            }
            else if (response.getStatusCode() != HttpStatus.OK.code()) {
                throw new RuntimeException("unexpected error code " + response.getStatusCode() + "; reason=" + response.getStatusMessage());
            }

            try {
                JsonNode rootNode = objectMapper.readTree(response.getInputStream());
                JsonNode queryStatsNode = rootNode.get("queryStats");
                if (queryStatsNode == null) {
                    return Optional.empty();
                }
                QueryStats queryStats = objectMapper.treeToValue(queryStatsNode, QueryStats.class);
                return Optional.of(queryStats);
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
    }
}
