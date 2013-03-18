/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.execution.BufferInfo;
import com.facebook.presto.execution.ExecutionStats;
import com.facebook.presto.execution.FailureInfo;
import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.execution.SharedBuffer.QueueState;
import com.facebook.presto.execution.SharedBufferInfo;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskState;
import com.facebook.presto.metadata.Node;
import com.facebook.presto.split.RemoteSplit;
import com.facebook.presto.split.Split;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.plan.PlanNode;
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
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.http.client.AsyncHttpClient;
import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.JsonBodyGenerator;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;

import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.util.FutureUtils.chainedCallback;
import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.transform;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.Request.Builder.preparePut;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.json.JsonCodec.jsonCodec;

public class HttpRemoteTask
        implements RemoteTask
{
    private static final Logger log = Logger.get(HttpRemoteTask.class);

    private final Session session;
    private final String nodeId;
    private final AtomicReference<TaskInfo> taskInfo = new AtomicReference<>();
    private final PlanFragment planFragment;

    @GuardedBy("this")
    private boolean noMoreSplits;
    @GuardedBy("this")
    private final SetMultimap<PlanNodeId, URI> exchangeLocations = HashMultimap.create();
    @GuardedBy("this")
    private boolean noMoreExchangeLocations;
    @GuardedBy("this")
    private final Set<String> outputIds = new TreeSet<>();
    @GuardedBy("this")
    private boolean noMoreOutputIds;

    private final AsyncHttpClient httpClient;
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
            AsyncHttpClient httpClient,
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
                0,
                TaskState.PLANNED,
                location,
                new SharedBufferInfo(QueueState.OPEN, bufferStates),
                ImmutableSet.<PlanNodeId>of(),
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
        synchronized (this) {
            Preconditions.checkState(!noMoreSplits, "noMoreSplits has already been set");
        }
        addSource(planFragment.getPartitionedSource(), split);
    }

    @Override
    public void noMoreSplits()
    {
        synchronized (this) {
            Preconditions.checkState(!noMoreSplits, "noMoreSplits has already been set");
            noMoreSplits = true;
        }
        if (planFragment.getPartitionedSource() != null) {
            noMoreSources(planFragment.getPartitionedSource());
        }
    }

    @Override
    public void addExchangeLocations(Multimap<PlanNodeId, URI> additionalLocations, boolean noMore)
    {
        // determine which locations are new
        SetMultimap<PlanNodeId, URI> newExchangeLocations;
        synchronized (this) {
            Preconditions.checkState(!noMoreExchangeLocations || exchangeLocations.entries().containsAll(additionalLocations.entries()),
                    "Locations can not be added after noMoreExchangeLocations has been set");

            noMoreExchangeLocations = noMore;

            newExchangeLocations = HashMultimap.create(additionalLocations);
            newExchangeLocations.entries().removeAll(exchangeLocations.entries());

            this.exchangeLocations.putAll(additionalLocations);
        }

        // add the new locations
        for (Entry<PlanNodeId, URI> entry : newExchangeLocations.entries()) {
            addSource(entry.getKey(), createRemoteSplitFor(entry.getValue()));
        }

        // if this is the final set, set no more
        if (noMore) {
            // assume all unpartitioned sources are exchange sources
            for (PlanNode planNode : planFragment.getSources()) {
                PlanNodeId planNodeId = planNode.getId();
                if (!planNodeId.equals(planFragment.getPartitionedSource())) {
                    noMoreSources(planNodeId);
                }
            }
        }
    }

    @Override
    public void addOutputBuffers(Set<String> outputBuffers, boolean noMore)
    {
        // determine which buffers are new
        Set<String> newOutputBuffers;
        synchronized (this) {
            Preconditions.checkState(!noMoreOutputIds || outputBuffers.containsAll(outputBuffers), "Buffers can not be added after noMoreOutputIds has been set");
            noMoreOutputIds = noMore;

            newOutputBuffers = ImmutableSet.copyOf(Sets.difference(outputBuffers, outputIds));

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
                ImmutableList.copyOf(transform(taskInfo.getOutputBuffers().getBuffers(), BufferInfo.bufferIdGetter())));

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

        updateTaskInfo(response.getValue());
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
        // if task is already complete, ignore this call
        TaskInfo taskInfo = this.taskInfo.get();
        if (taskInfo.getState().isDone()) {
            return;
        }

        // send http request
        URI sourceUri = uriBuilderFrom(taskInfo.getSelf()).appendPath("results").appendPath(outputName).build();
        Request request = preparePut()
                .setUri(sourceUri)
                .build();
        JsonResponse<TaskInfo> response = httpClient.execute(request, createFullJsonResponseHandler(taskInfoCodec));

        updateTaskInfo(response);

        // expect a 200 or 404
        if (response.getStatusCode() != 200 && response.getStatusCode() != 404) {
            throw new RuntimeException(String.format("Error adding result queue '%s' to task %s", outputName, taskInfo.getTaskId()));
        }
    }

    private void noMoreResultQueues()
    {
        // if task is already complete, ignore this call
        TaskInfo taskInfo = this.taskInfo.get();
        if (taskInfo.getState().isDone()) {
            return;
        }

        URI statusUri = uriBuilderFrom(taskInfo.getSelf()).appendPath("results").appendPath("complete").build();
        Request request = preparePut()
                .setUri(statusUri)
                .setHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
                .setBodyGenerator(createStaticBodyGenerator("true", UTF_8))
                .build();
        JsonResponse<TaskInfo> response = httpClient.execute(request, createFullJsonResponseHandler(taskInfoCodec));

        updateTaskInfo(response);

        // expect a 200 or 404
        if (response.getStatusCode() != 200 && response.getStatusCode() != 404) {
            throw new RuntimeException(String.format("Error setting no more results queues on task %s", taskInfo.getTaskId()));
        }
    }

    private void addSource(PlanNodeId sourceId, Split split)
    {
        // if task is already complete, ignore this call
        TaskInfo taskInfo = this.taskInfo.get();
        if (taskInfo.getState().isDone()) {
            return;
        }

        URI sourceUri = uriBuilderFrom(taskInfo.getSelf()).appendPath("source").appendPath(sourceId.toString()).build();
        Request request = preparePost()
                .setUri(sourceUri)
                .setHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
                .setBodyGenerator(JsonBodyGenerator.jsonBodyGenerator(splitCodec, split))
                .build();
        JsonResponse<TaskInfo> response = httpClient.execute(request, createFullJsonResponseHandler(taskInfoCodec));

        updateTaskInfo(response);

        // expect a 200 or 404
        if (response.getStatusCode() != 200 && response.getStatusCode() != 404) {
            throw new RuntimeException(String.format("Error adding split to source '%s' in task %s", sourceId, taskInfo.getTaskId()));
        }
    }

    private void noMoreSources(PlanNodeId sourceId)
    {
        Preconditions.checkNotNull(sourceId, "sourceId is null");

        // if task is already complete, ignore this call
        TaskInfo taskInfo = this.taskInfo.get();
        if (taskInfo.getState().isDone()) {
            return;
        }

        // send http request
        URI statusUri = uriBuilderFrom(taskInfo.getSelf()).appendPath("source").appendPath(sourceId.toString()).appendPath("complete").build();
        Request request = preparePut()
                .setUri(statusUri)
                .setHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
                .setBodyGenerator(createStaticBodyGenerator("true", UTF_8))
                .build();
        JsonResponse<TaskInfo> response = httpClient.execute(request, createFullJsonResponseHandler(taskInfoCodec));

        updateTaskInfo(response);

        // expect a 200 or 404
        if (response.getStatusCode() != 200 && response.getStatusCode() != 404) {
            throw new RuntimeException(String.format("Error closing source '%s' on task %s", sourceId, this.taskInfo.get().getTaskId()));
        }
    }

    private void updateTaskInfo(JsonResponse<TaskInfo> response)
    {
        // update task info
        if (response.hasValue()) {
            try {
                updateTaskInfo(response.getValue());
            }
            catch (Exception ignored) {
                // if we got bad json, back just ignore it... updating the task info is an optional part of this task
            }
        }
    }

    private void updateTaskInfo(TaskInfo newValue)
    {
        while (true) {
            TaskInfo currentValue = taskInfo.get();
            if (currentValue.getState().isDone()) {
                // never update if the task has reached a terminal state
                return;
            }
            if (newValue.getVersion() < currentValue.getVersion()) {
                // don't update to an older version (same version is ok)
                return;
            }
            if (taskInfo.compareAndSet(currentValue, newValue)) {
                return;
            }
        }
    }

    @Override
    public ListenableFuture<?> updateState()
    {
        final TaskInfo taskInfo = this.taskInfo.get();
        // don't update if the task hasn't been started yet or if it is already finished
        if ((taskInfo.getState() == TaskState.PLANNED) || taskInfo.getState().isDone()) {
            return Futures.immediateFuture(null);
        }
        URI statusUri = uriBuilderFrom(taskInfo.getSelf()).build();

        Request request = prepareGet().setUri(statusUri).build();
        ListenableFuture<JsonResponse<TaskInfo>> future = httpClient.executeAsync(request, createFullJsonResponseHandler(taskInfoCodec));
        return chainedCallback(future, new FutureCallback<JsonResponse<TaskInfo>>()
        {
            @Override
            public void onSuccess(JsonResponse<TaskInfo> response)
            {
                if (response.getStatusCode() == Status.GONE.getStatusCode()) {
                    // query has failed, been deleted, or something, and is no longer being tracked by the server
                    updateTaskInfo(new TaskInfo(taskInfo.getQueryId(),
                            taskInfo.getStageId(),
                            taskInfo.getTaskId(),
                            Long.MAX_VALUE,
                            TaskState.CANCELED,
                            taskInfo.getSelf(),
                            taskInfo.getOutputBuffers(),
                            taskInfo.getNoMoreSplits(),
                            taskInfo.getStats(),
                            ImmutableList.<FailureInfo>of()));
                }
                else {
                    checkState(response.getStatusCode() == Status.OK.getStatusCode(),
                            "Expected response code to be 201, but was %s: %s",
                            response.getStatusCode(),
                            response.getStatusMessage());
                    updateTaskInfo(response.getValue());
                }
            }

            @Override
            public void onFailure(Throwable t)
            {
                log.debug(t, "Error updating task info");
            }
        });
    }

    @Override
    public void cancel()
    {
        final TaskInfo taskInfo = this.taskInfo.get();
        final TaskInfo canceledTask = new TaskInfo(taskInfo.getQueryId(),
                taskInfo.getStageId(),
                taskInfo.getTaskId(),
                Long.MAX_VALUE,
                TaskState.CANCELED,
                taskInfo.getSelf(),
                taskInfo.getOutputBuffers(),
                taskInfo.getNoMoreSplits(),
                taskInfo.getStats(),
                ImmutableList.<FailureInfo>of());

        if (taskInfo.getSelf() == null) {
            updateTaskInfo(canceledTask);
            return;
        }

        Request request = prepareDelete().setUri(taskInfo.getSelf()).build();
        Futures.addCallback(httpClient.executeAsync(request, createFullJsonResponseHandler(taskInfoCodec)), new FutureCallback<JsonResponse<TaskInfo>>()
        {
            @Override
            public void onSuccess(JsonResponse<TaskInfo> response)
            {
                if (response.hasValue() && response.getValue().getState().isDone()) {
                    updateTaskInfo(response);
                }
                else {
                    updateTaskInfo(canceledTask);
                }
            }

            @Override
            public void onFailure(Throwable t)
            {
                log.debug(t, "Failed to cancel task %s", taskInfo.getTaskId());
                updateTaskInfo(canceledTask);
            }
        });
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
