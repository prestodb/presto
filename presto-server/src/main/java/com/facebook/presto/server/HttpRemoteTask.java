/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.execution.BufferInfo;
import com.facebook.presto.execution.ExecutionStats;
import com.facebook.presto.execution.FailureInfo;
import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskState;
import com.facebook.presto.server.FullJsonResponseHandler.JsonResponse;
import com.facebook.presto.split.Split;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.JsonBodyGenerator;
import io.airlift.http.client.Request;
import io.airlift.http.client.StatusResponseHandler.StatusResponse;
import io.airlift.json.JsonCodec;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.server.FullJsonResponseHandler.createFullJsonResponseHandler;
import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.transform;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.Request.Builder.preparePut;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static io.airlift.json.JsonCodec.jsonCodec;

public class HttpRemoteTask
        implements RemoteTask
{
    private final Session session;
    private final AtomicReference<TaskInfo> taskInfo = new AtomicReference<>();
    private final PlanFragment planFragment;
    private final Map<PlanNodeId, Set<Split>> fixedSources;

    private final HttpClient httpClient;
    private final JsonCodec<TaskInfo> taskInfoCodec;
    private final JsonCodec<QueryFragmentRequest> queryFragmentRequestCodec;
    private final JsonCodec<Split> splitCodec;

    public HttpRemoteTask(Session session,
            String queryId,
            String stageId,
            String taskId,
            URI location,
            PlanFragment planFragment,
            Map<PlanNodeId, Set<Split>> fixedSources,
            List<String> initialOutputIds,
            HttpClient httpClient,
            JsonCodec<TaskInfo> taskInfoCodec,
            JsonCodec<QueryFragmentRequest> queryFragmentRequestCodec,
            JsonCodec<Split> splitCodec)
    {
        Preconditions.checkNotNull(session, "session is null");
        Preconditions.checkNotNull(queryId, "queryId is null");
        Preconditions.checkNotNull(stageId, "stageId is null");
        Preconditions.checkNotNull(taskId, "taskId is null");
        Preconditions.checkNotNull(planFragment, "planFragment is null");
        Preconditions.checkNotNull(fixedSources, "fixedSources is null");
        Preconditions.checkNotNull(initialOutputIds, "initialOutputIds is null");
        Preconditions.checkNotNull(httpClient, "httpClient is null");
        Preconditions.checkNotNull(taskInfoCodec, "taskInfoCodec is null");
        Preconditions.checkNotNull(queryFragmentRequestCodec, "queryFragmentRequestCodec is null");
        Preconditions.checkNotNull(splitCodec, "splitCodec is null");

        this.session = session;
        this.planFragment = planFragment;
        this.fixedSources = fixedSources;
        this.httpClient = httpClient;
        this.taskInfoCodec = taskInfoCodec;
        this.queryFragmentRequestCodec = queryFragmentRequestCodec;
        this.splitCodec = splitCodec;

        List<BufferInfo> bufferStates = ImmutableList.copyOf(transform(initialOutputIds, new Function<String, BufferInfo>()
        {
            @Override
            public BufferInfo apply(String outputId)
            {
                return new BufferInfo(outputId, false, 0);
            }
        }));

        ExecutionStats stats = new ExecutionStats();
        taskInfo.set(new TaskInfo(queryId,
                stageId,
                taskId,
                TaskState.PLANNED,
                location,
                bufferStates,
                stats,
                ImmutableList.<FailureInfo>of()));
    }

    @Override
    public String getTaskId()
    {
        return taskInfo.get().getTaskId();
    }

    @Override
    public TaskInfo getTaskInfo()
    {
        return taskInfo.get();
    }

    @Override
    public void start()
    {
        TaskInfo taskInfo = this.taskInfo.get();
        QueryFragmentRequest queryFragmentRequest = new QueryFragmentRequest(session,
                taskInfo.getQueryId(),
                taskInfo.getStageId(),
                planFragment,
                fixedSources,
                ImmutableList.copyOf(transform(taskInfo.getOutputBuffers(), BufferInfo.bufferIdGetter())));

        Request request = preparePut()
                .setUri(taskInfo.getSelf())
                .setHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
                .setBodyGenerator(jsonBodyGenerator(queryFragmentRequestCodec, queryFragmentRequest))
                .build();

        JsonResponse<TaskInfo> response = httpClient.execute(request, createFullJsonResponseHandler(jsonCodec(TaskInfo.class)));
        checkState(response.getStatusCode() == 201,
                "Expected response code from %s to be 201, but was %s: %s",
                request.getUri(),
                response.getStatusCode(),
                response.getStatusMessage());
        String location = response.getHeader("Location");
        checkState(location != null);

        this.taskInfo.set(response.getValue());
    }

    @Override
    public void addResultQueue(String outputName)
    {
        TaskInfo taskInfo = this.taskInfo.get();
        URI sourceUri = uriBuilderFrom(taskInfo.getSelf()).appendPath("results").appendPath(outputName).build();
        Request request = preparePut()
                .setUri(sourceUri)
                .build();
        StatusResponse response = httpClient.execute(request, createStatusResponseHandler());
        if (response.getStatusCode() != 204) {
            throw new RuntimeException(String.format("Error adding result queue '%s' to task %s", outputName, taskInfo.getTaskId()));
        }
    }

    @Override
    public void noMoreResultQueues()
    {
        TaskInfo taskInfo = this.taskInfo.get();
        URI statusUri = uriBuilderFrom(taskInfo.getSelf()).appendPath("results").appendPath("complete").build();
        Request request = preparePut()
                .setUri(statusUri)
                .setHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
                .setBodyGenerator(createStaticBodyGenerator("true", UTF_8))
                .build();
        StatusResponse response = httpClient.execute(request, createStatusResponseHandler());
        if (response.getStatusCode() / 100  != 2) {
            throw new RuntimeException(String.format("Error setting no more results queues on task %s", taskInfo.getTaskId()));
        }
    }

    @Override
    public void addSource(PlanNodeId sourceId, Split split)
    {
        TaskInfo taskInfo = this.taskInfo.get();
        URI sourceUri = uriBuilderFrom(taskInfo.getSelf()).appendPath("source").appendPath(sourceId.toString()).build();
        Request request = preparePost()
                .setUri(sourceUri)
                .setHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
                .setBodyGenerator(JsonBodyGenerator.jsonBodyGenerator(splitCodec, split))
                .build();
        StatusResponse response = httpClient.execute(request, createStatusResponseHandler());
        if (response.getStatusCode() != 204) {
            throw new RuntimeException(String.format("Error adding source '%s' to task %s", sourceId.toString(), taskInfo.getTaskId()));
        }
    }

    @Override
    public void noMoreSources(PlanNodeId sourceId)
    {
        TaskInfo taskInfo = this.taskInfo.get();
        URI statusUri = uriBuilderFrom(taskInfo.getSelf()).appendPath("source").appendPath(sourceId.toString()).appendPath("complete").build();
        Request request = preparePut()
                .setUri(statusUri)
                .setHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
                .setBodyGenerator(createStaticBodyGenerator("true", UTF_8))
                .build();
        StatusResponse response = httpClient.execute(request, createStatusResponseHandler());
        if (response.getStatusCode() / 100  != 2) {
            throw new RuntimeException(String.format("Error closing source '%s' on task %s", sourceId.toString(), taskInfo.getTaskId()));
        }
    }

    @Override
    public void updateState()
    {
        TaskInfo taskInfo = this.taskInfo.get();
        // don't update if the task hasn't been started yet or if it is already finished
        TaskState currentState = taskInfo.getState();
        if (currentState == TaskState.PLANNED || currentState.isDone()) {
            return;
        }
        URI statusUri = uriBuilderFrom(taskInfo.getSelf()).build();
        JsonResponse<TaskInfo> response = httpClient.execute(prepareGet().setUri(statusUri).build(), createFullJsonResponseHandler(taskInfoCodec));

        if (response.getStatusCode() == Status.GONE.getStatusCode()) {
            // query has failed, been deleted, or something, and is no longer being tracked by the server
            if (!currentState.isDone()) {
                this.taskInfo.set(new TaskInfo(taskInfo.getQueryId(),
                        taskInfo.getStageId(),
                        taskInfo.getTaskId(),
                        TaskState.CANCELED,
                        taskInfo.getSelf(),
                        taskInfo.getOutputBuffers(),
                        taskInfo.getStats(),
                        ImmutableList.<FailureInfo>of()));
            }
        }
        else {
            Preconditions.checkState(response.getStatusCode() == Status.OK.getStatusCode(),
                    "Expected response code to be 201, but was %s: %s",
                    response.getStatusCode(),
                    response.getStatusMessage());

            this.taskInfo.set(response.getValue());
        }
    }

    @Override
    public void cancel()
    {
        TaskInfo taskInfo = this.taskInfo.get();
        if (taskInfo.getSelf() == null) {
            this.taskInfo.set(new TaskInfo(taskInfo.getQueryId(),
                    taskInfo.getStageId(),
                    taskInfo.getTaskId(),
                    TaskState.CANCELED,
                    taskInfo.getSelf(),
                    taskInfo.getOutputBuffers(),
                    taskInfo.getStats(),
                    ImmutableList.<FailureInfo>of()));
            return;
        }
        try {
            Request request = prepareDelete().setUri(taskInfo.getSelf()).build();
            httpClient.execute(request, createStatusResponseHandler());
        }
        catch (RuntimeException ignored) {
        }
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .addValue(taskInfo.get())
                .toString();
    }
}
