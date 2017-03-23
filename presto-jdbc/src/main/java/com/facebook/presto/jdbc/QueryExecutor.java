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
package com.facebook.presto.jdbc;

import com.facebook.presto.client.ClientSession;
import com.facebook.presto.client.JsonResponse;
import com.facebook.presto.client.ServerInfo;
import com.facebook.presto.client.StatementClient;
import io.airlift.json.JsonCodec;
import okhttp3.HttpUrl;
import okhttp3.Interceptor;
import okhttp3.OkHttpClient;
import okhttp3.Request;

import java.io.Closeable;
import java.net.URI;

import static com.google.common.net.HttpHeaders.USER_AGENT;
import static io.airlift.json.JsonCodec.jsonCodec;
import static java.util.Objects.requireNonNull;

class QueryExecutor
        implements Closeable
{
    private static final JsonCodec<ServerInfo> SERVER_INFO_CODEC = jsonCodec(ServerInfo.class);

    private final OkHttpClient httpClient;

    private QueryExecutor(OkHttpClient httpClient)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
    }

    public StatementClient startQuery(ClientSession session, String query)
    {
        return new StatementClient(httpClient, session, query);
    }

    @Override
    public void close()
    {
        httpClient.dispatcher().executorService().shutdown();
        httpClient.connectionPool().evictAll();
    }

    public ServerInfo getServerInfo(URI server)
    {
        HttpUrl url = HttpUrl.get(server).newBuilder()
                .encodedPath("/v1/info")
                .build();

        Request request = new Request.Builder()
                .url(url)
                .get()
                .build();

        return JsonResponse.execute(SERVER_INFO_CODEC, httpClient, request)
                .getValue();
    }

    static QueryExecutor create(String userAgent)
    {
        return new QueryExecutor(new OkHttpClient.Builder()
                .addInterceptor(userAgentInterceptor(userAgent))
                .build());
    }

    private static Interceptor userAgentInterceptor(String userAgent)
    {
        return chain -> chain.proceed(chain.request().newBuilder()
                .header(USER_AGENT, userAgent)
                .build());
    }
}
