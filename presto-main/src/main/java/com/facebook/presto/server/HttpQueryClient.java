/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.cli.ClientSession;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.PageIterator;
import com.facebook.presto.operator.PageIterators;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.net.HttpHeaders;
import io.airlift.http.client.AsyncHttpClient;
import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;

import javax.annotation.concurrent.ThreadSafe;
import javax.ws.rs.core.Response.Status;
import java.net.URI;
import java.util.ArrayList;
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
{
    private final AsyncHttpClient httpClient;
    private final URI queryLocation;
    private final JsonCodec<QueryInfo> queryInfoCodec;
    private final JsonCodec<TaskInfo> taskInfoCodec;
    private final boolean debug;
    private final AtomicReference<QueryInfo> finalQueryInfo = new AtomicReference<>();
    private final AtomicBoolean canceled = new AtomicBoolean();

    public HttpQueryClient(ClientSession session,
            String query,
            AsyncHttpClient httpClient,
            JsonCodec<QueryInfo> queryInfoCodec,
            JsonCodec<TaskInfo> taskInfoCodec)
    {
        checkNotNull(session, "session is null");
        checkNotNull(query, "query is null");
        checkNotNull(httpClient, "httpClient is null");
        checkNotNull(queryInfoCodec, "queryInfoCodec is null");
        checkNotNull(taskInfoCodec, "taskInfoCodec is null");

        this.httpClient = httpClient;
        this.queryInfoCodec = queryInfoCodec;
        this.taskInfoCodec = taskInfoCodec;
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
        return new QueryDriversOperator(10, outputStage.getTupleInfos(), Iterables.transform(outputStage.getTasks(), new Function<TaskInfo, QueryDriverProvider>()
        {
            @Override
            public QueryDriverProvider apply(TaskInfo taskInfo)
            {
                Preconditions.checkState(taskInfo.getOutputBuffers().size() == 1,
                        "Expected a single output buffer for task %s, but found %s",
                        taskInfo.getTaskId(),
                        taskInfo.getOutputBuffers());

                return new HttpTaskClient(taskInfo.getTaskId(),
                        taskInfo.getSelf(),
                        Iterables.getOnlyElement(taskInfo.getOutputBuffers()).getBufferId(),
                        httpClient,
                        taskInfoCodec);
            }
        }));
    }

    public boolean isCanceled()
    {
        return canceled.get();
    }

    public boolean cancelLeafStage()
    {
        QueryInfo queryInfo = getQueryInfo(false);
        if (queryInfo == null) {
            return false;
        }

        if (queryInfo.getOutputStage() == null) {
            // query is not running yet, cannot cancel leaf stage
            return false;
        }

        // query is running, cancel the leaf-most running stage
        return cancelLeafStage(queryInfo.getOutputStage());
    }

    private boolean cancelLeafStage(StageInfo stage)
    {
        // if this stage is already done, we can't cancel it
        if (stage.getState().isDone()) {
            return false;
        }

        // attempt to cancel a sub stage
        List<StageInfo> subStages = new ArrayList<>(stage.getSubStages());
        // check in reverse order since build side of a join will be later in the list
        subStages = Lists.reverse(subStages);
        for (StageInfo subStage : subStages) {
            if (cancelLeafStage(subStage)) {
                return true;
            }
        }

        // cancel this stage
        Request.Builder requestBuilder = prepareDelete().setUri(stage.getSelf());
        Request request = requestBuilder.build();
        httpClient.execute(request, createStatusResponseHandler());
        return true;
    }

    public void cancelQuery()
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
