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
import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.client.ServerInfo;
import com.facebook.presto.spark.execution.http.server.smile.BaseResponse;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.net.URI;

import static com.google.common.net.HttpHeaders.ACCEPT;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * An abstraction of HTTP client that communicates with the locally running Presto worker process. It exposes worker's server level endpoints to simple method calls.
 */
@ThreadSafe
public class PrestoSparkHttpServerClient
{
    private final OkHttpClient httpClient;
    private final URI location;
    private final URI serverUri;
    private final JsonCodec<ServerInfo> serverInfoCodec;

    public PrestoSparkHttpServerClient(
            OkHttpClient httpClient,
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
        HttpUrl url = HttpUrl.get(serverUri);
        Request request = new Request.Builder()
                .url(url)
                .get()
                .addHeader(ACCEPT, JSON_UTF_8.toString())
                .addHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .build();

        SettableFuture<BaseResponse<ServerInfo>> future = SettableFuture.create();

        httpClient.newCall(request).enqueue(new Callback() {
            @Override
            public void onFailure(Call call, IOException e)
            {
                future.setException(e);
            }

            @Override
            public void onResponse(Call call, Response response)
            {
                try {
                    OkHttpBaseResponse<ServerInfo> baseResponse = new OkHttpBaseResponse<>(response, serverInfoCodec);
                    future.set(baseResponse);
                }
                catch (Exception e) {
                    future.setException(e);
                }
                finally {
                    response.close();
                }
            }
        });

        return future;
    }

    public URI getLocation()
    {
        return location;
    }

    private URI getServerUri(URI baseUri)
    {
        return HttpUrl.get(baseUri).newBuilder()
                .addPathSegment("v1")
                .addPathSegment("info")
                .build()
                .uri();
    }
}
