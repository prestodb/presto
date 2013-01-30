/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.execution.PageBuffer;
import com.facebook.presto.execution.TaskInfo;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import io.airlift.http.client.AsyncHttpClient;
import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;

import javax.annotation.concurrent.ThreadSafe;
import javax.ws.rs.core.Response.Status;
import java.net.URI;

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
    private final AsyncHttpClient httpClient;
    private final URI location;
    private final JsonCodec<TaskInfo> taskInfoCodec;
    private final String outputId;
    private final URI resultsLocation;

    public HttpTaskClient(String taskId,
            URI location,
            String outputId,
            AsyncHttpClient httpClient,
            JsonCodec<TaskInfo> taskInfoCodec)
    {
        this.taskId = taskId;
        this.httpClient = httpClient;
        this.location = location;
        this.taskInfoCodec = taskInfoCodec;
        this.outputId = outputId;
        this.resultsLocation = uriBuilderFrom(location).appendPath("results").appendPath(outputId).build();
    }

    public String getTaskId()
    {
        return taskId;
    }

    public URI getLocation()
    {
        return location;
    }

    public TaskInfo getTaskInfo()
    {
        URI statusUri = uriBuilderFrom(location).build();
        JsonResponse<TaskInfo> response = httpClient.execute(prepareGet().setUri(statusUri).build(), createFullJsonResponseHandler(taskInfoCodec));

        if (response.getStatusCode() == Status.GONE.getStatusCode()) {
            // query has failed, been deleted, or something, and is no longer being tracked by the server
            return null;
        }

        Preconditions.checkState(response.getStatusCode() == Status.OK.getStatusCode(),
                "Expected response code to be 201, but was %s: %s",
                response.getStatusCode(),
                response.getStatusMessage());

        TaskInfo taskInfo = response.getValue();
        return taskInfo;
    }

    @Override
    public QueryDriver create(PageBuffer outputBuffer)
    {
        HttpQuery httpQuery = new HttpQuery(resultsLocation, outputBuffer, httpClient);
        return httpQuery;
    }

    public void cancel()
    {
        try {
            Request request = prepareDelete().setUri(resultsLocation).build();
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
