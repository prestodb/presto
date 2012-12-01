/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.PlanFragmentSource;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Preconditions;
import io.airlift.http.client.AsyncHttpClient;
import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;

import javax.annotation.concurrent.ThreadSafe;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static io.airlift.json.JsonCodec.jsonCodec;

@ThreadSafe
public class HttpTaskClient
        implements QueryDriverProvider, QueryTask
{
    private final String taskId;
    private final HttpClient httpClient;
    private final ExecutorService executor;
    private final URI location;
    private final List<TupleInfo> tupleInfos;
    private final JsonCodec<QueryTaskInfo> queryTaskInfoCodec;
    private final String outputId;

    public HttpTaskClient(String taskId,
            URI location,
            String outputId,
            List<TupleInfo> tupleInfos, HttpClient httpClient,
            ExecutorService executor,
            JsonCodec<QueryTaskInfo> queryTaskInfoCodec)
    {
        this.taskId = taskId;
        this.httpClient = httpClient;
        this.executor = executor;
        this.location = location;
        this.tupleInfos = tupleInfos;
        this.queryTaskInfoCodec = queryTaskInfoCodec;
        this.outputId = outputId;
    }

    public HttpTaskClient(PlanFragment planFragment,
            Map<String, List<PlanFragmentSource>> fragmentSources,
            HttpClient httpClient,
            ExecutorService executor,
            JsonCodec<QueryFragmentRequest> queryFragmentRequestCodec,
            JsonCodec<QueryTaskInfo> queryTaskInfoCodec,
            URI serverURI,
            String outputId)
    {
        Preconditions.checkNotNull(planFragment, "planFragment is null");
        Preconditions.checkNotNull(fragmentSources, "fragmentSources is null");
        checkNotNull(httpClient, "httpClient is null");
        checkNotNull(executor, "executor is null");

        this.httpClient = httpClient;
        this.executor = executor;

        Request request = preparePost()
                .setUri(serverURI)
                .setHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
                .setBodyGenerator(jsonBodyGenerator(queryFragmentRequestCodec, new QueryFragmentRequest(planFragment, fragmentSources)))
                .build();

        JsonResponse<QueryTaskInfo> response = httpClient.execute(request, createFullJsonResponseHandler(jsonCodec(QueryTaskInfo.class)));
        Preconditions.checkState(response.getStatusCode() == 201,
                "Expected response code from %s to be 201, but was %d: %s",
                request.getUri(),
                response.getStatusCode(),
                response.getStatusMessage());
        String location = response.getHeader("Location");
        Preconditions.checkState(location != null);

        this.location = URI.create(location);

        QueryTaskInfo queryTaskInfo = response.getValue();
        this.taskId = queryTaskInfo.getTaskId();
        this.tupleInfos = queryTaskInfo.getTupleInfos();
        this.queryTaskInfoCodec = queryTaskInfoCodec;
        this.outputId = outputId;
    }

    @Override
    public String getTaskId()
    {
        return taskId;
    }

    public URI getLocation()
    {
        return location;
    }

    @Override
    public QueryTaskInfo getQueryTaskInfo()
    {
        URI statusUri = uriBuilderFrom(location).build();
        JsonResponse<QueryTaskInfo> response = httpClient.execute(prepareGet().setUri(statusUri).build(), createFullJsonResponseHandler(queryTaskInfoCodec));

        if (response.getStatusCode() == Status.GONE.getStatusCode()) {
            // query has failed, been deleted, or something, and is no longer being tracked by the server
            return null;
        }

        Preconditions.checkState(response.getStatusCode() == Status.OK.getStatusCode(),
                "Expected response code to be 201, but was %d: %s",
                response.getStatusCode(),
                response.getStatusMessage());

        QueryTaskInfo queryTaskInfo = response.getValue();
        return queryTaskInfo;
    }

    @Override
    public int getChannelCount()
    {
        return tupleInfos.size();
    }

    @Override
    public List<TupleInfo> getTupleInfos()
    {
        return tupleInfos;
    }

    @Override
    public QueryDriver create(QueryState queryState)
    {
        HttpQuery httpQuery = new HttpQuery(uriBuilderFrom(location).appendPath("results").appendPath(outputId).build(), queryState, new AsyncHttpClient(httpClient, executor));
        return httpQuery;
    }

    @Override
    public void cancel()
    {
        try {
            Request request = prepareDelete().setUri(location).build();
            httpClient.execute(request, createStatusResponseHandler());
        }
        catch (RuntimeException ignored) {
        }
    }
}
