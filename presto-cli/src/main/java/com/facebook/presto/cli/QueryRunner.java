package com.facebook.presto.cli;

import com.facebook.presto.client.ClientSession;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.client.StatementClient;
import io.airlift.http.client.AsyncHttpClient;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.netty.StandaloneNettyAsyncHttpClient;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;

import java.io.Closeable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.json.JsonCodec.jsonCodec;

public class QueryRunner
        implements Closeable
{
    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final JsonCodec<QueryResults> queryResultsCodec;
    private final ClientSession session;
    private final AsyncHttpClient httpClient;

    public QueryRunner(ClientSession session, JsonCodec<QueryResults> queryResultsCodec)
    {
        this.session = checkNotNull(session, "session is null");
        this.queryResultsCodec = checkNotNull(queryResultsCodec, "queryResultsCodec is null");
        this.httpClient = new StandaloneNettyAsyncHttpClient("cli",
                new HttpClientConfig().setConnectTimeout(new Duration(10, TimeUnit.SECONDS)));
    }

    public ClientSession getSession()
    {
        return session;
    }

    public Query startQuery(String query)
    {
        return new Query(new StatementClient(httpClient, queryResultsCodec, session, query));
    }

    @Override
    public void close()
    {
        executor.shutdownNow();
        httpClient.close();
    }

    public static QueryRunner create(ClientSession session)
    {
        return new QueryRunner(session, jsonCodec(QueryResults.class));
    }
}
