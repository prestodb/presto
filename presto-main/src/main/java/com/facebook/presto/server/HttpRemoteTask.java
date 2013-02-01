/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.execution.ExchangePlanFragmentSource;
import com.facebook.presto.execution.ExecutionStats;
import com.facebook.presto.execution.FailureInfo;
import com.facebook.presto.execution.PageBuffer.BufferState;
import com.facebook.presto.execution.PageBufferInfo;
import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskState;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.PlanFragmentSource;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.transform;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePut;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static io.airlift.json.JsonCodec.jsonCodec;

public class HttpRemoteTask
        implements RemoteTask
{
    private final Session session;
    private final AtomicReference<TaskInfo> taskInfo = new AtomicReference<>();
    private final PlanFragment planFragment;
    private final List<PlanFragmentSource> splits;
    private final Map<PlanNodeId, ExchangePlanFragmentSource> exchangeSources;

    private final HttpClient httpClient;
    private final JsonCodec<TaskInfo> taskInfoCodec;
    private final JsonCodec<QueryFragmentRequest> queryFragmentRequestCodec;

    public HttpRemoteTask(Session session,
            String queryId,
            String stageId,
            String taskId,
            URI location,
            PlanFragment planFragment,
            List<PlanFragmentSource> splits,
            Map<PlanNodeId, ExchangePlanFragmentSource> exchangeSources,
            List<String> outputIds,
            HttpClient httpClient,
            JsonCodec<TaskInfo> taskInfoCodec,
            JsonCodec<QueryFragmentRequest> queryFragmentRequestCodec)
    {
        Preconditions.checkNotNull(session, "session is null");
        Preconditions.checkNotNull(queryId, "queryId is null");
        Preconditions.checkNotNull(stageId, "stageId is null");
        Preconditions.checkNotNull(taskId, "taskId is null");
        Preconditions.checkNotNull(planFragment, "planFragment is null");
        Preconditions.checkNotNull(splits, "splits is null");
        Preconditions.checkNotNull(exchangeSources, "exchangeSources is null");
        Preconditions.checkNotNull(outputIds, "outputIds is null");
        Preconditions.checkArgument(!outputIds.isEmpty(), "outputIds is empty");
        Preconditions.checkNotNull(httpClient, "httpClient is null");
        Preconditions.checkNotNull(taskInfoCodec, "taskInfoCodec is null");
        Preconditions.checkNotNull(queryFragmentRequestCodec, "queryFragmentRequestCodec is null");

        this.session = session;
        this.planFragment = planFragment;
        this.splits = splits;
        this.exchangeSources = exchangeSources;
        this.httpClient = httpClient;
        this.taskInfoCodec = taskInfoCodec;
        this.queryFragmentRequestCodec = queryFragmentRequestCodec;

        List<PageBufferInfo> bufferStates = ImmutableList.copyOf(transform(outputIds, new Function<String, PageBufferInfo>()
        {
            @Override
            public PageBufferInfo apply(String outputId)
            {
                return new PageBufferInfo(outputId, BufferState.CREATED, 0);
            }
        }));

        ExecutionStats stats = new ExecutionStats();
        stats.addSplits(splits.size());
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
                splits,
                exchangeSources,
                ImmutableList.copyOf(transform(taskInfo.getOutputBuffers(), PageBufferInfo.bufferIdGetter())));

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
