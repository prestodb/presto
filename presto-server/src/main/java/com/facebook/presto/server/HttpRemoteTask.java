/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.OutputBuffers;
import com.facebook.presto.ScheduledSplit;
import com.facebook.presto.TaskSource;
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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;
import com.google.common.net.HttpHeaders;
import com.google.common.net.MediaType;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.RateLimiter;
import io.airlift.http.client.AsyncHttpClient;
import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import javax.annotation.concurrent.GuardedBy;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.transform;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.Request.Builder.preparePost;

public class HttpRemoteTask
        implements RemoteTask
{
    private static final Logger log = Logger.get(HttpRemoteTask.class);

    private final Session session;
    private final String nodeId;
    private final PlanFragment planFragment;

    private final AtomicLong nextSplitId = new AtomicLong();

    @GuardedBy("this")
    private TaskInfo taskInfo;
    @GuardedBy("this")
    private boolean canceled;
    @GuardedBy("this")
    private CurrentRequest currentRequest;
    @GuardedBy("this")
    private final SetMultimap<PlanNodeId, ScheduledSplit> pendingSplits = HashMultimap.create();
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
    private final JsonCodec<TaskUpdateRequest> taskUpdateRequestCodec;
    private final List<TupleInfo> tupleInfos;

    private final RateLimiter requestRateLimiter = RateLimiter.create(10);

    public HttpRemoteTask(Session session,
            String queryId,
            String stageId,
            String taskId,
            Node node,
            URI location,
            PlanFragment planFragment,
            Split initialSplit,
            Multimap<PlanNodeId, URI> initialExchangeLocations,
            Set<String> initialOutputIds,
            AsyncHttpClient httpClient,
            JsonCodec<TaskInfo> taskInfoCodec,
            JsonCodec<TaskUpdateRequest> taskUpdateRequestCodec)
    {
        Preconditions.checkNotNull(session, "session is null");
        Preconditions.checkNotNull(queryId, "queryId is null");
        Preconditions.checkNotNull(stageId, "stageId is null");
        Preconditions.checkNotNull(taskId, "taskId is null");
        Preconditions.checkNotNull(location, "location is null");
        Preconditions.checkNotNull(planFragment, "planFragment1 is null");
        Preconditions.checkNotNull(initialOutputIds, "initialOutputIds is null");
        Preconditions.checkNotNull(httpClient, "httpClient is null");
        Preconditions.checkNotNull(taskInfoCodec, "taskInfoCodec is null");
        Preconditions.checkNotNull(taskUpdateRequestCodec, "taskUpdateRequestCodec is null");

        this.session = session;
        this.nodeId = node.getNodeIdentifier();
        this.planFragment = planFragment;
        this.outputIds.addAll(initialOutputIds);
        this.httpClient = httpClient;
        this.taskInfoCodec = taskInfoCodec;
        this.taskUpdateRequestCodec = taskUpdateRequestCodec;
        tupleInfos = planFragment.getTupleInfos();


        for (Entry<PlanNodeId, URI> entry : initialExchangeLocations.entries()) {
            ScheduledSplit scheduledSplit = new ScheduledSplit(nextSplitId.getAndIncrement(), createRemoteSplitFor(entry.getValue()));
            pendingSplits.put(entry.getKey(), scheduledSplit);
        }
        this.exchangeLocations.putAll(initialExchangeLocations);

        List<BufferInfo> bufferStates = ImmutableList.copyOf(transform(initialOutputIds, new Function<String, BufferInfo>()
        {
            @Override
            public BufferInfo apply(String outputId)
            {
                return new BufferInfo(outputId, false, 0, 0);
            }
        }));

        if (initialSplit != null) {
            Preconditions.checkState(planFragment.isPartitioned(), "Plan is not partitioned");
            pendingSplits.put(planFragment.getPartitionedSource(), new ScheduledSplit(nextSplitId.getAndIncrement(), initialSplit));
        }

        taskInfo = new TaskInfo(queryId,
                stageId,
                taskId,
                TaskInfo.MIN_VERSION,
                TaskState.PLANNED,
                location,
                new SharedBufferInfo(QueueState.OPEN, 0, bufferStates),
                ImmutableSet.<PlanNodeId>of(),
                new ExecutionStats(),
                ImmutableList.<FailureInfo>of());
    }

    @Override
    public synchronized String getTaskId()
    {
        return taskInfo.getTaskId();
    }

    @Override
    public synchronized TaskInfo getTaskInfo()
    {
        return taskInfo;
    }

    @Override
    public void addSplit(Split split)
    {
        synchronized (this) {
            Preconditions.checkNotNull(split, "split is null");
            Preconditions.checkState(!noMoreSplits, "noMoreSplits has already been set");
            Preconditions.checkState(planFragment.isPartitioned(), "Plan is not partitioned");

            // only add pending split if not canceled
            if (!canceled) {
                pendingSplits.put(planFragment.getPartitionedSource(), new ScheduledSplit(nextSplitId.getAndIncrement(), split));
            }
        }
        updateState(false);
    }

    @Override
    public void noMoreSplits()
    {
        synchronized (this) {
            Preconditions.checkState(!noMoreSplits, "noMoreSplits has already been set");
            noMoreSplits = true;
        }
        updateState(false);
    }

    @Override
    public void addExchangeLocations(Multimap<PlanNodeId, URI> additionalLocations, boolean noMore)
    {
        // determine which locations are new
        synchronized (this) {
            Preconditions.checkState(!noMoreExchangeLocations || exchangeLocations.entries().containsAll(additionalLocations.entries()),
                    "Locations can not be added after noMoreExchangeLocations has been set");

            SetMultimap<PlanNodeId, URI> newExchangeLocations = HashMultimap.create(additionalLocations);
            newExchangeLocations.entries().removeAll(exchangeLocations.entries());

            // only add pending split if not canceled
            if (!canceled) {
                for (Entry<PlanNodeId, URI> entry : newExchangeLocations.entries()) {
                    ScheduledSplit scheduledSplit = new ScheduledSplit(nextSplitId.getAndIncrement(), createRemoteSplitFor(entry.getValue()));
                    pendingSplits.put(entry.getKey(), scheduledSplit);
                }
            }
            noMoreExchangeLocations = noMore;

            this.exchangeLocations.putAll(additionalLocations);
        }
        updateState(false);
    }

    @Override
    public void addOutputBuffers(Set<String> outputBuffers, boolean noMore)
    {
        synchronized (this) {
            Preconditions.checkState(!noMoreOutputIds, "Buffers can not be added after noMoreOutputIds has been set");
            noMoreOutputIds = noMore;
            this.outputIds.addAll(outputBuffers);
        }
        updateState(false);
    }

    private synchronized void updateTaskInfo(TaskInfo newValue)
    {
        if (taskInfo.getState().isDone()) {
            // never update if the task has reached a terminal state
            return;
        }
        if (newValue.getVersion() < taskInfo.getVersion()) {
            // don't update to an older version (same version is ok)
            return;
        }

        taskInfo = newValue;
        if (newValue.getState().isDone()) {
            // splits can be huge so clear the list
            pendingSplits.clear();
        }
    }

    @Override
    public ListenableFuture<?> updateState(boolean forceRefresh)
    {
        Request request;
        CurrentRequest currentRequest;
        synchronized (this) {
            // don't update if the task hasn't been started yet or if it is already finished
            if (taskInfo.getState().isDone()) {
                return Futures.immediateFuture(null);
            }

            if (!forceRefresh) {
                if (this.currentRequest != null) {
                    // if current request is old, cancel it so we can begin a new request
                    this.currentRequest.cancelIfOlderThan(new Duration(2, TimeUnit.SECONDS));

                    if (!this.currentRequest.isDone()) {
                        // request is still running, but when it finishes, it should update again
                        this.currentRequest.requestAnotherUpdate();

                        // todo return existing pending request future?
                        return Futures.immediateFuture(null);
                    }

                    this.currentRequest = null;
                }

                // don't update too fast
                if (!requestRateLimiter.tryAcquire()) {
                    // todo return existing pending request future?
                    return Futures.immediateFuture(null);
                }
            }

            List<TaskSource> sources = getSources();
            currentRequest = new CurrentRequest(sources);
            if (canceled) {
                request = prepareDelete().setUri(taskInfo.getSelf()).build();
            }
            else {
                TaskUpdateRequest updateRequest = new TaskUpdateRequest(session,
                        taskInfo.getQueryId(),
                        taskInfo.getStageId(),
                        planFragment,
                        sources,
                        new OutputBuffers(outputIds, noMoreOutputIds));

                request = preparePost()
                        .setUri(uriBuilderFrom(taskInfo.getSelf()).build())
                        .setHeader(HttpHeaders.CONTENT_TYPE, MediaType.JSON_UTF_8.toString())
                        .setBodyGenerator(jsonBodyGenerator(taskUpdateRequestCodec, updateRequest))
                        .build();
            }

            this.currentRequest = currentRequest;
        }

        ListenableFuture<JsonResponse<TaskInfo>> future = httpClient.executeAsync(request, createFullJsonResponseHandler(taskInfoCodec));
        currentRequest.setRequestFuture(future);
        return currentRequest;
    }

    @Override
    public synchronized int getQueuedSplits()
    {
        int pendingSplitCount = 0;
        if (planFragment.isPartitioned()) {
            pendingSplitCount = pendingSplits.get(planFragment.getPartitionedSource()).size();
        }
        return pendingSplitCount + taskInfo.getStats().getQueuedSplits();
    }

    private synchronized List<TaskSource> getSources()
    {
        ImmutableList.Builder<TaskSource> sources = ImmutableList.builder();
        if (planFragment.isPartitioned()) {
            Set<ScheduledSplit> splits = pendingSplits.get(planFragment.getPartitionedSource());
            if (!splits.isEmpty() || noMoreSplits) {
                sources.add(new TaskSource(planFragment.getPartitionedSource(), splits, noMoreSplits));
            }
        }
        for (PlanNode planNode : planFragment.getSources()) {
            PlanNodeId planNodeId = planNode.getId();
            if (!planNodeId.equals(planFragment.getPartitionedSource())) {
                Set<ScheduledSplit> splits = pendingSplits.get(planNodeId);
                if (!splits.isEmpty() || noMoreExchangeLocations) {
                    sources.add(new TaskSource(planNodeId, splits, noMoreExchangeLocations));
                }
            }
        }
        return sources.build();
    }

    private synchronized void acknowledgeSources(List<TaskSource> sources)
    {
        for (TaskSource source : sources) {
            PlanNodeId planNodeId = source.getPlanNodeId();
            for (ScheduledSplit split : source.getSplits()) {
                pendingSplits.remove(planNodeId, split);
            }
        }
    }

    @Override
    public void cancel()
    {
        synchronized (this) {
            pendingSplits.clear();

            if (!canceled && currentRequest != null) {
                currentRequest.cancel(true);
                canceled = true;
            }
        }
        updateState(false);
    }

    private RemoteSplit createRemoteSplitFor(URI taskLocation)
    {
        URI splitLocation = uriBuilderFrom(taskLocation).appendPath("results").appendPath(nodeId).build();
        return new RemoteSplit(splitLocation, tupleInfos);
    }

    @Override
    public synchronized String toString()
    {
        return Objects.toStringHelper(this)
                .addValue(taskInfo)
                .toString();
    }


    private class CurrentRequest
            extends AbstractFuture<Void>
            implements FutureCallback<JsonResponse<TaskInfo>>
    {
        private final long startTime = System.nanoTime();

        private final List<TaskSource> sources;

        @GuardedBy("this")
        private Future<?> requestFuture;

        @GuardedBy("this")
        private boolean anotherUpdateRequested;

        private CurrentRequest(List<TaskSource> sources)
        {
            this.sources = ImmutableList.copyOf(sources);
        }

        public synchronized void requestAnotherUpdate()
        {
            this.anotherUpdateRequested = true;
        }

        public synchronized void setRequestFuture(ListenableFuture<JsonResponse<TaskInfo>> requestFuture)
        {
            Preconditions.checkNotNull(requestFuture, "requestFuture is null");
            Preconditions.checkState(this.requestFuture == null, "requestFuture already set");

            this.requestFuture = requestFuture;
            if (isDone()) {
                requestFuture.cancel(true);
            } else {
                Futures.addCallback(requestFuture, this);
            }
        }

        public boolean cancel(boolean mayInterruptIfRunning)
        {
            synchronized (this) {
                if (requestFuture != null) {
                    requestFuture.cancel(true);
                }
            }
            return super.cancel(true);
        }

        public void cancelIfOlderThan(Duration maxRequestTime)
        {
            if (isDone() || Duration.nanosSince(startTime).compareTo(maxRequestTime) < 0) {
                return;
            }
            cancel(true);
        }

        @Override
        public void onSuccess(JsonResponse<TaskInfo> response)
        {
            try {
                if (response.getStatusCode() != HttpStatus.GONE.code()) {
                    checkState(response.getStatusCode() == HttpStatus.OK.code(),
                            "Expected response code to be %s, but was %s: %s",
                            HttpStatus.OK.code(),
                            response.getStatusCode(),
                            response.getStatusMessage());

                    // update task info
                    if (response.hasValue()) {
                        try {
                            updateTaskInfo(response.getValue());
                        }
                        catch (Exception ignored) {
                            // if we got bad json back, update again
                            updateState(false);
                            return;
                        }
                    }

                    // remove acknowledged splits, which frees memory
                    acknowledgeSources(sources);

                    updateIfNecessary();
                }
                else {
                    updateTaskInfo(new TaskInfo(taskInfo.getQueryId(),
                            taskInfo.getStageId(),
                            taskInfo.getTaskId(),
                            TaskInfo.MAX_VERSION,
                            TaskState.CANCELED,
                            taskInfo.getSelf(),
                            taskInfo.getOutputBuffers(),
                            taskInfo.getNoMoreSplits(),
                            taskInfo.getStats(),
                            ImmutableList.<FailureInfo>of()));
                }
            } catch (Throwable t) {
                setException(t);
            }
            finally {
                set(null);
            }
        }

        private void updateIfNecessary()
        {
            synchronized (this) {
                if (!anotherUpdateRequested) {
                    return;
                }
            }

            // is this is no longer the current request, don't do anything
            synchronized (HttpRemoteTask.this) {
                if (currentRequest != this) {
                    return;
                }
            }

            updateState(false);
        }

        @Override
        public synchronized void onFailure(Throwable t)
        {
            setException(t);

            if (!(t instanceof CancellationException)) {
                TaskInfo taskInfo = getTaskInfo();
                if (isSocketError(t)) {
                    log.warn("%s task %s: %s: %s", "Error updating", taskInfo.getTaskId(), t.getMessage(), taskInfo.getSelf());
                }
                else {
                    log.warn(t, "%s task %s: %s", "Error updating", taskInfo.getTaskId(), taskInfo.getSelf());
                }
            }

            updateState(false);
        }

        private boolean isSocketError(Throwable t)
        {
            while (t != null) {
                if ((t instanceof SocketException) || (t instanceof SocketTimeoutException)) {
                    return true;
                }
                t = t.getCause();
            }
            return false;
        }
    }
}
