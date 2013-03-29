/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.cli;

import com.facebook.presto.PrestoHeaders;
import com.facebook.presto.execution.BufferInfo;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.operator.ExchangeOperator;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.PageIterator;
import com.facebook.presto.operator.PageIterators;
import com.facebook.presto.split.RemoteSplit;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.net.HttpHeaders;
import io.airlift.http.client.AsyncHttpClient;
import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;

import javax.annotation.concurrent.ThreadSafe;
import javax.ws.rs.core.Response.Status;

import java.io.Closeable;
import java.net.URI;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;

@ThreadSafe
public class HttpQueryClient
        implements Closeable
{
    private final AsyncHttpClient httpClient;
    private final URI queryLocation;
    private final JsonCodec<QueryInfo> queryInfoCodec;
    private final boolean debug;
    private final AtomicReference<QueryInfo> finalQueryInfo = new AtomicReference<>();
    private final AtomicBoolean canceled = new AtomicBoolean();

    public HttpQueryClient(ClientSession session,
            String query,
            AsyncHttpClient httpClient,
            JsonCodec<QueryInfo> queryInfoCodec)
    {
        checkNotNull(session, "session is null");
        checkNotNull(query, "query is null");
        checkNotNull(httpClient, "httpClient is null");
        checkNotNull(queryInfoCodec, "queryInfoCodec is null");

        this.httpClient = httpClient;
        this.queryInfoCodec = queryInfoCodec;
        this.debug = session.isDebug();

        URI queryUri = HttpUriBuilder.uriBuilderFrom(session.getServer()).appendPath("/v1/query").build();
        Request.Builder requestBuilder = preparePost()
                .setUri(queryUri)
                .setBodyGenerator(createStaticBodyGenerator(query, Charsets.UTF_8));
        if (session.getUser() != null) {
            requestBuilder.setHeader(PrestoHeaders.PRESTO_USER, session.getUser());
        }
        if (session.getCatalog() != null) {
            requestBuilder.setHeader(PrestoHeaders.PRESTO_CATALOG, session.getCatalog());
        }
        if (session.getSchema() != null) {
            requestBuilder.setHeader(PrestoHeaders.PRESTO_SCHEMA, session.getSchema());
        }
        Request request = requestBuilder.build();

        JsonResponse<QueryInfo> response = httpClient.execute(request, createFullJsonResponseHandler(queryInfoCodec));
        Preconditions.checkState(response.getStatusCode() == 201,
                "Expected response code to be 201, but was %s: %s",
                response.getStatusCode(),
                response.getStatusMessage());
        String location = response.getHeader("Location");
        Preconditions.checkState(location != null);

        this.queryLocation = URI.create(location);
    }

    public boolean isDebug()
    {
        return debug;
    }

    public URI getQueryLocation()
    {
        return queryLocation;
    }

    public QueryInfo getQueryInfo(boolean forceRefresh)
    {
        QueryInfo queryInfo = finalQueryInfo.get();
        if (queryInfo != null) {
            return queryInfo;
        }

        if (isCanceled()) {
            return null;
        }

        URI statusUri = uriBuilderFrom(queryLocation).build();
        Request.Builder requestBuilder = prepareGet().setUri(statusUri);
        if (forceRefresh) {
            requestBuilder.addHeader(HttpHeaders.CACHE_CONTROL, "no-cache");
        }
        JsonResponse<QueryInfo> response = httpClient.execute(requestBuilder.build(), createFullJsonResponseHandler(queryInfoCodec));

        if (response.getStatusCode() == Status.GONE.getStatusCode()) {
            // query has failed, been deleted, or something, and is no longer being tracked by the server
            return null;
        }

        Preconditions.checkState(response.getStatusCode() == Status.OK.getStatusCode(),
                "Expected response code to be 201, but was %s: %s",
                response.getStatusCode(),
                response.getStatusMessage());

        queryInfo = response.getValue();

        // query info for a done query can be cached forever
        if (queryInfo.getState().isDone()) {
            finalQueryInfo.set(queryInfo);
        }

        return queryInfo;
    }

    public Operator getResultsOperator()
    {
        QueryInfo queryInfo = getQueryInfo(false);
        if (queryInfo == null || queryInfo.getOutputStage() == null) {
            return new Operator()
            {
                @Override
                public int getChannelCount()
                {
                    return 0;
                }

                @Override
                public List<TupleInfo> getTupleInfos()
                {
                    return ImmutableList.of();
                }

                @Override
                public PageIterator iterator(OperatorStats operatorStats)
                {
                    return PageIterators.emptyIterator(ImmutableList.<TupleInfo>of());
                }
            };
        }

        StageInfo outputStage = queryInfo.getOutputStage();
        ExchangeOperator exchangeOperator = new ExchangeOperator(httpClient,
                outputStage.getTupleInfos(),
                100,
                10,
                3);

        for (TaskInfo taskInfo : outputStage.getTasks()) {
            List<BufferInfo> buffers = taskInfo.getOutputBuffers().getBuffers();
            Preconditions.checkState(buffers.size() == 1,
                    "Expected a single output buffer for task %s, but found %s",
                    taskInfo.getTaskId(),
                    buffers);

            String bufferId = Iterables.getOnlyElement(buffers).getBufferId();
            URI uri = uriBuilderFrom(taskInfo.getSelf()).appendPath("results").appendPath(bufferId).build();
            exchangeOperator.addSplit(new RemoteSplit(uri, outputStage.getTupleInfos()));
        }
        exchangeOperator.noMoreSplits();
        return exchangeOperator;
    }

    public boolean isCanceled()
    {
        return canceled.get();
    }

    public void close()
    {
        canceled.set(true);
        try {
            Request.Builder requestBuilder = prepareDelete().setUri(queryLocation);
            Request request = requestBuilder.build();
            httpClient.execute(request, createStatusResponseHandler());
        }
        catch (RuntimeException ignored) {
        }
    }

}
