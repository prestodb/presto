package com.facebook.presto.jdbc;

import com.facebook.presto.client.ClientSession;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.client.StatementClient;
import com.google.common.collect.ImmutableSet;
import io.airlift.http.client.AsyncHttpClient;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.netty.NettyAsyncHttpClientConfig;
import io.airlift.http.client.netty.NettyIoPoolConfig;
import io.airlift.http.client.netty.StandaloneNettyAsyncHttpClient;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.units.Duration;

import java.io.Closeable;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

public class QueryExecutor
        implements Closeable
{
    private final JsonCodec<QueryResults> queryInfoCodec;
    private final AsyncHttpClient httpClient;

    private QueryExecutor(String userAgent, JsonCodec<QueryResults> queryResultsCodec)
    {
        checkNotNull(userAgent, "userAgent is null");
        checkNotNull(queryResultsCodec, "queryResultsCodec is null");

        this.queryInfoCodec = queryResultsCodec;
        this.httpClient = new StandaloneNettyAsyncHttpClient("jdbc",
                new HttpClientConfig()
                        .setConnectTimeout(new Duration(10, TimeUnit.SECONDS)),
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

    public static QueryExecutor create(String userAgent)
    {
        return new QueryExecutor(userAgent, new JsonCodecFactory().jsonCodec(QueryResults.class));
    }
}
