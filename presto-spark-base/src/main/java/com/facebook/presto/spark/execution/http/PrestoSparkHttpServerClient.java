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
package com.facebook.presto.spark.execution.http;

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.client.ServerInfo;
import com.facebook.presto.server.smile.BaseResponse;
import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.concurrent.ThreadSafe;

import java.net.URI;

import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static com.facebook.airlift.http.client.Request.Builder.prepareGet;
import static com.facebook.presto.server.RequestHelpers.setContentTypeHeaders;
import static com.facebook.presto.server.smile.AdaptingJsonResponseHandler.createAdaptingJsonResponseHandler;
import static java.util.Objects.requireNonNull;

/**
 * An abstraction of HTTP client that communicates with the locally running Presto worker process. It exposes worker's server level endpoints to simple method calls.
 */
@ThreadSafe
public class PrestoSparkHttpServerClient
{
    private static final Logger log = Logger.get(PrestoSparkHttpServerClient.class);
    private static final String SERVER_URI = "/v1/info";

    private final HttpClient httpClient;
    private final URI location;
    private final URI serverUri;
    private final JsonCodec<ServerInfo> serverInfoCodec;

    public PrestoSparkHttpServerClient(
            HttpClient httpClient,
            URI location,
            JsonCodec<ServerInfo> serverInfoCodec)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.location = requireNonNull(location, "location is null");
        this.serverInfoCodec = requireNonNull(serverInfoCodec, "serverInfoCodec is null");
        this.serverUri = getServerUri(location);
    }

    public ListenableFuture<BaseResponse<ServerInfo>> getServerInfo()
    {
        Request request = setContentTypeHeaders(false, prepareGet())
                .setUri(serverUri)
                .build();
        return httpClient.executeAsync(request, createAdaptingJsonResponseHandler(serverInfoCodec));
    }

    public URI getLocation()
    {
        return location;
    }

    private URI getServerUri(URI baseUri)
    {
        return uriBuilderFrom(baseUri)
                .appendPath(SERVER_URI)
                .build();
    }
}
