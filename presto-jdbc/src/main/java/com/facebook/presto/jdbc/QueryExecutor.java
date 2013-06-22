package com.facebook.presto.jdbc;

import com.facebook.presto.client.ClientSession;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.client.StatementClient;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import io.airlift.http.client.AsyncHttpClient;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.netty.NettyAsyncHttpClientConfig;
import io.airlift.http.client.netty.NettyIoPoolConfig;
import io.airlift.http.client.netty.StandaloneNettyAsyncHttpClient;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.URI;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.json.JsonCodec.jsonCodec;

public class QueryExecutor
        implements Closeable
{
    private final JsonCodec<QueryResults> queryInfoCodec;
    private final AsyncHttpClient httpClient;

    private QueryExecutor(String userAgent, JsonCodec<QueryResults> queryResultsCodec, HostAndPort socksProxy)
    {
        checkNotNull(userAgent, "userAgent is null");
        checkNotNull(queryResultsCodec, "queryResultsCodec is null");

        this.queryInfoCodec = queryResultsCodec;
        this.httpClient = new StandaloneNettyAsyncHttpClient("jdbc",
                new HttpClientConfig()
                        .setConnectTimeout(new Duration(10, TimeUnit.SECONDS))
                        .setSocksProxy(socksProxy),
                new NettyAsyncHttpClientConfig(),
                new NettyIoPoolConfig(),
                ImmutableSet.of(new UserAgentRequestFilter(userAgent)));
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

    // TODO: replace this with a phantom reference
    @SuppressWarnings("FinalizeDeclaration")
    @Override
    protected void finalize()
    {
        close();
    }

    public static QueryExecutor create(String userAgent)
    {
        return new QueryExecutor(userAgent, jsonCodec(QueryResults.class), getSystemSocksProxy());
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
