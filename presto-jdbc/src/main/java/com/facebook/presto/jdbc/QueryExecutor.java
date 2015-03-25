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

import com.facebook.presto.client.UniversalStatementClient;
import com.facebook.presto.client.ClientSession;
import org.apache.http.HttpHost;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.DefaultProxyRoutePlanner;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.URI;

import static com.google.common.base.Preconditions.checkNotNull;

class QueryExecutor
        implements Closeable
{
    private final ObjectMapper mapper;
    private final CloseableHttpAsyncClient httpClient;
    private final String userAgent;

    private QueryExecutor(String userAgent, ObjectMapper mapper, HttpHost proxy)
    {
        checkNotNull(userAgent, "userAgent is null");
        checkNotNull(mapper, "mapper is null");

        this.userAgent = userAgent;
        this.mapper = mapper;

        HttpClientBuilder builder = HttpClients.custom();
        HttpAsyncClientBuilder asyncBuilder = HttpAsyncClients.custom();

        if (proxy != null) {
            DefaultProxyRoutePlanner routePlanner = new DefaultProxyRoutePlanner(proxy);
            builder.setRoutePlanner(routePlanner);
            asyncBuilder.setRoutePlanner(routePlanner);
        }

        this.httpClient = asyncBuilder.build();
        this.httpClient.start();
    }

    public UniversalStatementClient startQuery(ClientSession session, String query)
    {
        return new UniversalStatementClient(
                new ApacheQueryHttpClient(httpClient, mapper, userAgent),
                session,
                query);
    }

    @Override
    public void close() throws IOException
    {
        httpClient.close();
    }

    // TODO: replace this with a phantom reference
    @SuppressWarnings("FinalizeDeclaration")
    @Override
    protected void finalize() throws IOException
    {
        close();
    }

    static QueryExecutor create(String userAgent)
    {
        return new QueryExecutor(userAgent, new ObjectMapper(), getSystemSocksProxy());
    }

    @Nullable
    private static HttpHost getSystemSocksProxy()
    {
        URI uri = URI.create("socket://0.0.0.0:80");
        for (Proxy proxy : ProxySelector.getDefault().select(uri)) {
            if (proxy.type() == Proxy.Type.SOCKS) {
                if (proxy.address() instanceof InetSocketAddress) {
                    InetSocketAddress address = (InetSocketAddress) proxy.address();
                    return new HttpHost(address.getHostString(), address.getPort());
                }
            }
        }
        return null;
    }
}
