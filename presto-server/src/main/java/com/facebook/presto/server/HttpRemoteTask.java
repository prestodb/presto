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
import com.facebook.presto.metadata.Node;
import com.facebook.presto.server.FullJsonResponseHandler.JsonResponse;
import com.facebook.presto.split.RemoteSplit;
import com.facebook.presto.split.Split;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.JsonBodyGenerator;
import io.airlift.http.client.Request;
import io.airlift.http.client.StatusResponseHandler.StatusResponse;
import io.airlift.json.JsonCodec;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;
import java.net.URI;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
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
    private final String nodeId;
    private final AtomicReference<TaskInfo> taskInfo = new AtomicReference<>();
    private final PlanFragment planFragment;

    @GuardedBy("this")
    private final SetMultimap<PlanNodeId, URI> exchangeLocations = HashMultimap.create();
    @GuardedBy("this")
    private final Set<String> outputIds = new TreeSet<>();

    private final HttpClient httpClient;
    private final JsonCodec<TaskInfo> taskInfoCodec;
    private final JsonCodec<QueryFragmentRequest> queryFragmentRequestCodec;
    private final JsonCodec<Split> splitCodec;
    private final List<TupleInfo> tupleInfos;

    public HttpRemoteTask(Session session,
            String queryId,
            String stageId,
            String taskId,
            Node node,
            URI location,
            PlanFragment planFragment,
            Multimap<PlanNodeId, URI> initialExchangeLocations,
            Set<String> initialOutputIds,
            HttpClient httpClient,
            JsonCodec<TaskInfo> taskInfoCodec,
            JsonCodec<QueryFragmentRequest> queryFragmentRequestCodec,
            JsonCodec<Split> splitCodec)
    {
        Preconditions.checkNotNull(session, "session is null");
        Preconditions.checkNotNull(queryId, "queryId is null");
        Preconditions.checkNotNull(stageId, "stageId is null");
        Preconditions.checkNotNull(taskId, "taskId is null");
        Preconditions.checkNotNull(location, "location is null");
        Preconditions.checkNotNull(planFragment, "planFragment1 is null");
        Preconditions.checkNotNull(initialExchangeLocations, "initialExchangeLocations is null");
        Preconditions.checkNotNull(initialOutputIds, "initialOutputIds is null");
        Preconditions.checkNotNull(httpClient, "httpClient is null");
        Preconditions.checkNotNull(taskInfoCodec, "taskInfoCodec is null");
        Preconditions.checkNotNull(queryFragmentRequestCodec, "queryFragmentRequestCodec is null");
        Preconditions.checkNotNull(splitCodec, "splitCodec is null");

        this.session = session;
        this.nodeId = node.getNodeIdentifier();
        this.planFragment = planFragment;
        this.exchangeLocations.putAll(initialExchangeLocations);
        this.outputIds.addAll(initialOutputIds);
        this.httpClient = httpClient;
        this.taskInfoCodec = taskInfoCodec;
        this.queryFragmentRequestCodec = queryFragmentRequestCodec;
        this.splitCodec = splitCodec;
        tupleInfos = planFragment.getTupleInfos();

        List<BufferInfo> bufferStates = ImmutableList.copyOf(transform(initialOutputIds, new Function<String, BufferInfo>()
        {
            @Override
            public BufferInfo apply(String outputId)
            {
                return new BufferInfo(outputId, false, 0);
            }
        }));

        taskInfo.set(new TaskInfo(queryId,
                stageId,
                taskId,
                TaskState.PLANNED,
                location,
                bufferStates,
                new ExecutionStats(),
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
    public void addSplit(Split split)
    {
        addSource(planFragment.getPartitionedSource(), split);
    }

    @Override
    public void noMoreSplits()
    {
        if (planFragment.getPartitionedSource() != null) {
            noMoreSources(planFragment.getPartitionedSource());
        }
    }

    @Override
    public void addExchangeLocations(Multimap<PlanNodeId, URI> exchangeLocations, boolean noMore)
    {
        // determine which locations are new
        SetMultimap<PlanNodeId, URI> newExchangeLocations = HashMultimap.create(exchangeLocations);
        synchronized (this) {
            newExchangeLocations.entries().removeAll(this.exchangeLocations.entries());
            this.exchangeLocations.putAll(exchangeLocations);
        }

        // add the new locations
        for (Entry<PlanNodeId, URI> entry : newExchangeLocations.entries()) {
            addSource(entry.getKey(), createRemoteSplitFor(entry.getValue()));
        }

        // if this is the final set, set no more
        if (noMore) {
            Set<PlanNodeId> allExchangeSourceIds;
            synchronized (this) {
                allExchangeSourceIds = exchangeLocations.keySet();
            }
            for (PlanNodeId planNodeId : allExchangeSourceIds) {
                noMoreSources(planNodeId);
            }
        }
    }

    @Override
    public void addOutputBuffers(Set<String> outputBuffers, boolean noMore)
    {
        // determine which buffers are new
        HashSet<String> newOutputBuffers = new HashSet<>(outputBuffers);
        synchronized (this) {
            newOutputBuffers.removeAll(this.outputIds);
            this.outputIds.addAll(outputBuffers);
        }

        // add the new buffers
        for (String newOutputBuffer : newOutputBuffers) {
            addResultQueue(newOutputBuffer);
        }

        // if this is the final set, set no more
        if (noMore) {
            noMoreResultQueues();
        }
    }

    @Override
    public void start(@Nullable Split initialSplit)
    {
        TaskInfo taskInfo = this.taskInfo.get();
        QueryFragmentRequest queryFragmentRequest = new QueryFragmentRequest(session,
                taskInfo.getQueryId(),
                taskInfo.getStageId(),
                planFragment,
                initialSources(initialSplit),
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

    private synchronized Map<PlanNodeId, Set<Split>> initialSources(@Nullable Split initialSplit)
    {
        ImmutableMap.Builder<PlanNodeId, Set<Split>> sources = ImmutableMap.builder();
        if (initialSplit != null) {
            sources.put(planFragment.getPartitionedSource(), ImmutableSet.of(initialSplit));
        }
        for (Entry<PlanNodeId, Collection<URI>> entry : exchangeLocations.asMap().entrySet()) {
            ImmutableSet.Builder<Split> splits = ImmutableSet.builder();
            for (URI location : entry.getValue()) {
                splits.add(createRemoteSplitFor(location));
            }
            sources.put(entry.getKey(), splits.build());
        }
        return sources.build();
    }

    private void addResultQueue(String outputName)
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

    private void noMoreResultQueues()
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

    private void addSource(PlanNodeId sourceId, Split split)
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
            throw new RuntimeException(String.format("Error adding split to source '%s' in task %s", sourceId, taskInfo.getTaskId()));
        }
    }

    private void noMoreSources(PlanNodeId sourceId)
    {
        Preconditions.checkNotNull(sourceId, "sourceId is null");
        URI taskLocation = taskInfo.get().getSelf();
        Preconditions.checkNotNull(taskLocation, "taskLocation is null");
        URI statusUri = uriBuilderFrom(taskLocation).appendPath("source").appendPath(sourceId.toString()).appendPath("complete").build();
        Request request = preparePut()
                .setUri(statusUri)
                .setHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
                .setBodyGenerator(createStaticBodyGenerator("true", UTF_8))
                .build();
        StatusResponse response = httpClient.execute(request, createStatusResponseHandler());
        if (response.getStatusCode() / 100  != 2) {
            throw new RuntimeException(String.format("Error closing source '%s' on task %s", sourceId, this.taskInfo.get().getTaskId()));
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

    private RemoteSplit createRemoteSplitFor(URI taskLocation)
    {
        URI splitLocation = uriBuilderFrom(taskLocation).appendPath("results").appendPath(nodeId).build();
        return new RemoteSplit(splitLocation, tupleInfos);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .addValue(taskInfo.get())
                .toString();
    }
}
