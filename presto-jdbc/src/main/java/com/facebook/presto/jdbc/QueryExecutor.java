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
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.client.ServerInfo;
import com.facebook.presto.client.StatementClient;
import com.google.common.net.HostAndPort;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.URI;
import java.util.concurrent.TimeUnit;

import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.json.JsonCodec.jsonCodec;
import static java.util.Objects.requireNonNull;

class QueryExecutor
        implements Closeable
{
    private final JsonCodec<QueryResults> queryInfoCodec;
    private final JsonCodec<ServerInfo> serverInfoCodec;
    private final HttpClient httpClient;

    private QueryExecutor(JsonCodec<QueryResults> queryResultsCodec, JsonCodec<ServerInfo> serverInfoCodec, HttpClient httpClient)
    {
        this.queryInfoCodec = requireNonNull(queryResultsCodec, "queryResultsCodec is null");
        this.serverInfoCodec = requireNonNull(serverInfoCodec, "serverInfoCodec is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
    }

    public StatementClient startQuery(ClientSession session, String query)
    {
        return new StatementClient(httpClient, queryInfoCodec, session, query);
    }

    @Override
    public void close()
    {
        httpClient.close();
    }

    public ServerInfo getServerInfo(URI server)
    {
        URI uri = uriBuilderFrom(server).replacePath("/v1/info").build();
        Request request = prepareGet().setUri(uri).build();
        return httpClient.execute(request, createJsonResponseHandler(serverInfoCodec));
    }

    // TODO: replace this with a phantom reference
    @SuppressWarnings("FinalizeDeclaration")
    @Override
    protected void finalize()
    {
        close();
    }

    static HttpClientConfig baseClientConfig()
    {
        return new HttpClientConfig()
                .setConnectTimeout(new Duration(10, TimeUnit.SECONDS))
                .setSocksProxy(getSystemSocksProxy());
    }

    static QueryExecutor create(HttpClient httpClient)
    {
        return new QueryExecutor(jsonCodec(QueryResults.class), jsonCodec(ServerInfo.class), httpClient);
    }

    @Nullable
    private static HostAndPort getSystemSocksProxy()
    {
        URI uri = URI.create("socket://0.0.0.0:80");
        for (Proxy proxy : ProxySelector.getDefault().select(uri)) {
            if (proxy.type() == Proxy.Type.SOCKS) {
                if (proxy.address() instanceof InetSocketAddress) {
                    InetSocketAddress address = (InetSocketAddress) proxy.address();
                    return HostAndPort.fromParts(address.getHostString(), address.getPort());
                }
            }
        }
        return null;
    }
}
