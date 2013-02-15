package com.facebook.presto.jdbc;

import com.facebook.presto.metadata.TableHandleModule;
import com.facebook.presto.sql.tree.Serialization.ExpressionDeserializer;
import com.facebook.presto.sql.tree.Serialization.FunctionCallDeserializer;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Stage;
import io.airlift.json.JsonBinder;
import io.airlift.json.JsonModule;

import com.facebook.presto.cli.ClientSession;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.cli.HttpQueryClient;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.Serialization;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.http.client.AsyncHttpClient;
import io.airlift.http.client.AsyncHttpClientConfig;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.http.client.HttpRequestFilter;
import io.airlift.http.client.netty.NettyAsyncHttpClient;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.units.Duration;

import javax.annotation.PreDestroy;
import java.io.Closeable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

public class QueryExecutor
        implements Closeable
{
    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final JsonCodec<QueryInfo> queryInfoCodec;
    private final AsyncHttpClient httpClient;

    public QueryExecutor(String userAgent, JsonCodec<QueryInfo> queryInfoCodec)
    {
        checkNotNull(userAgent, "userAgent is null");
        this.queryInfoCodec = checkNotNull(queryInfoCodec, "queryInfoCodec is null");
        this.httpClient = new NettyAsyncHttpClient(new HttpClientConfig()
                .setConnectTimeout(new Duration(1, TimeUnit.DAYS))
                .setReadTimeout(new Duration(10, TimeUnit.DAYS)),
                new AsyncHttpClientConfig(),
                ImmutableSet.<HttpRequestFilter>of(new UserAgentRequestFilter(userAgent)));
    }

    public HttpQueryClient startQuery(ClientSession session, String query)
    {
        return new HttpQueryClient(session, query, httpClient, queryInfoCodec);
    }

    @PreDestroy
    @Override
    public void close()
    {
        executor.shutdownNow();
        httpClient.close();
    }

    public static QueryExecutor create(String userAgent)
    {
        JsonCodecFactory codecs = createCodecFactory();
        JsonCodec<QueryInfo> queryInfoCodec = codecs.jsonCodec(QueryInfo.class);
        return new QueryExecutor(userAgent, queryInfoCodec);
    }

    private static JsonCodecFactory createCodecFactory()
    {
        Injector injector = Guice.createInjector(Stage.PRODUCTION,
                new JsonModule(),
                new TableHandleModule(),
                new Module() {
                    @Override
                    public void configure(Binder binder)
                    {
                        JsonBinder.jsonBinder(binder).addDeserializerBinding(Expression.class).to(ExpressionDeserializer.class);
                        JsonBinder.jsonBinder(binder).addDeserializerBinding(FunctionCall.class).to(FunctionCallDeserializer.class);
                    }
                });

        return injector.getInstance(JsonCodecFactory.class);
    }
}
