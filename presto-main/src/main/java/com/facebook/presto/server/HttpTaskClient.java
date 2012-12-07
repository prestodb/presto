/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import io.airlift.http.client.AsyncHttpClient;
import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;

import javax.annotation.concurrent.ThreadSafe;
import javax.ws.rs.core.Response.Status;
import java.net.URI;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;

@ThreadSafe
public class HttpTaskClient
        implements QueryDriverProvider
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

    public String getTaskId()
    {
        return taskId;
    }

    public URI getLocation()
    {
        return location;
    }

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
    public QueryDriver create(PageBuffer outputBuffer)
    {
        HttpQuery httpQuery = new HttpQuery(uriBuilderFrom(location).appendPath("results").appendPath(outputId).build(), outputBuffer, new AsyncHttpClient(httpClient, executor));
        return httpQuery;
    }

    public void cancel()
    {
        try {
            Request request = prepareDelete().setUri(location).build();
            httpClient.execute(request, createStatusResponseHandler());
        }
        catch (RuntimeException ignored) {
        }
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("taskId", taskId)
                .add("location", location)
                .add("outputId", outputId)
                .toString();
    }
}
