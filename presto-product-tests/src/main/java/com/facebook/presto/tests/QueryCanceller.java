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
package com.facebook.presto.tests;

import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;
import io.airlift.http.client.jetty.JettyHttpClient;

import java.io.Closeable;
import java.net.URI;

import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.ResponseHandlerUtils.propagate;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class QueryCanceller
        implements Closeable
{
    private final HttpClient httpClient;
    private final URI baseUri;

    public QueryCanceller(String serverAddress)
    {
        this.httpClient = new JettyHttpClient(new HttpClientConfig());
        requireNonNull(serverAddress, "Server Address is null");
        baseUri = URI.create(format("%s", serverAddress));
    }

    public Response cancel(String queryId)
    {
        requireNonNull(queryId, "QueryId is null");
        URI uri = uriBuilderFrom(baseUri).appendPath("/v1/query").appendPath(queryId).build();
        Request request = prepareDelete().setUri(uri).build();
        return httpClient.execute(request, new ResponseHandler<Response, RuntimeException>()
        {
            @Override
            public Response handleException(Request request, Exception exception)
            {
                throw propagate(request, exception);
            }

            @Override
            public Response handle(Request request, Response response)
            {
                return response;
            }
        });
    }

    @Override
    public void close()
    {
        httpClient.close();
    }
}
