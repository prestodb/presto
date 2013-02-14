package com.facebook.presto.cli;

import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.server.HttpQueryClient;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.airlift.http.client.AsyncHttpClient;
import io.airlift.http.client.HttpClientConfig;
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

import static com.facebook.presto.sql.tree.Serialization.ExpressionDeserializer;
import static com.facebook.presto.sql.tree.Serialization.FunctionCallDeserializer;
import static com.google.common.base.Preconditions.checkNotNull;

public class QueryRunner
        implements Closeable
{
    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final JsonCodec<QueryInfo> queryInfoCodec;
    private final ClientSession session;
    private final AsyncHttpClient httpClient;

    public QueryRunner(
            ClientSession session,
            JsonCodec<QueryInfo> queryInfoCodec)
    {
        this.session = checkNotNull(session, "session is null");
        this.queryInfoCodec = checkNotNull(queryInfoCodec, "queryInfoCodec is null");
        this.httpClient = new NettyAsyncHttpClient(new HttpClientConfig()
                .setConnectTimeout(new Duration(1, TimeUnit.DAYS))
                .setReadTimeout(new Duration(10, TimeUnit.DAYS)));
    }

    public ClientSession getSession()
    {
        return session;
    }

    public Query startQuery(String query)
    {
        Preconditions.checkNotNull(query, "query is null");
        HttpQueryClient client = new HttpQueryClient(session, query, httpClient, queryInfoCodec);
        return new Query(client);
    }

    @PreDestroy
    @Override
    public void close()
    {
        executor.shutdownNow();
        httpClient.close();
    }

    public static QueryRunner create(ClientSession session)
    {
        JsonCodecFactory codecs = createCodecFactory();
        JsonCodec<QueryInfo> queryInfoCodec = codecs.jsonCodec(QueryInfo.class);
        return new QueryRunner(session, queryInfoCodec);
    }

    private static JsonCodecFactory createCodecFactory()
    {
        ObjectMapperProvider objectMapperProvider = new ObjectMapperProvider();
        ImmutableMap.Builder<Class<?>, JsonDeserializer<?>> deserializers = ImmutableMap.builder();
        deserializers.put(Expression.class, new ExpressionDeserializer());
        deserializers.put(FunctionCall.class, new FunctionCallDeserializer());
        objectMapperProvider.setJsonDeserializers(deserializers.build());
        return new JsonCodecFactory(objectMapperProvider);
    }
}
