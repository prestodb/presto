/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.OutputBuffers;
import com.facebook.presto.ScheduledSplit;
import com.facebook.presto.TaskSource;
import com.facebook.presto.client.FailureInfo;
import com.facebook.presto.client.PrestoHeaders;
import com.facebook.presto.execution.BufferInfo;
import com.facebook.presto.execution.ExecutionStats.ExecutionStatsSnapshot;
import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.execution.SharedBuffer.QueueState;
import com.facebook.presto.execution.SharedBufferInfo;
import com.facebook.presto.execution.StateMachine;
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskState;
import com.facebook.presto.metadata.Node;
import com.facebook.presto.operator.OperatorStats.SplitExecutionStats;
import com.facebook.presto.spi.Split;
import com.facebook.presto.split.RemoteSplit;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.planner.OutputReceiver;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;
import com.google.common.net.HttpHeaders;
import com.google.common.net.MediaType;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.RateLimiter;
import io.airlift.http.client.AsyncHttpClient;
import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.http.client.StatusResponseHandler.StatusResponse;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import javax.annotation.concurrent.GuardedBy;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.presto.util.Failures.toFailure;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.transform;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;

public class HttpRemoteTask
        implements RemoteTask
{
    private static final Logger log = Logger.get(HttpRemoteTask.class);

    private final Session session;
    private final String nodeId;
    private final PlanFragment planFragment;
    private final int maxConsecutiveErrorCount;
    private final Duration minErrorDuration;

    private final AtomicLong nextSplitId = new AtomicLong();

    private final StateMachine<TaskInfo> taskInfo;

    @GuardedBy("this")
    private Future<?> currentRequest;
    @GuardedBy("this")
    private long currentRequestStartNanos;

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

    @GuardedBy("this")
    private ContinuousTaskInfoFetcher continuousTaskInfoFetcher;

    private final AsyncHttpClient httpClient;
    private final Executor executor;
    private final JsonCodec<TaskInfo> taskInfoCodec;
    private final JsonCodec<TaskUpdateRequest> taskUpdateRequestCodec;
    private final List<TupleInfo> tupleInfos;
    private final Map<PlanNodeId, OutputReceiver> outputReceivers;

    private final RateLimiter errorRequestRateLimiter = RateLimiter.create(0.1);

    private final AtomicLong lastSuccessfulRequest = new AtomicLong(System.nanoTime());
    private final AtomicLong errorCount = new AtomicLong();
    private final Queue<Throwable> errorsSinceLastSuccess = new ConcurrentLinkedQueue<>();

    private final AtomicBoolean needsUpdate = new AtomicBoolean(true);

    public HttpRemoteTask(Session session,
            TaskId taskId,
            Node node,
            URI location,
            PlanFragment planFragment,
            Split initialSplit,
            Map<PlanNodeId, OutputReceiver> outputReceivers,
            Multimap<PlanNodeId, URI> initialExchangeLocations,
            Set<String> initialOutputIds,
            AsyncHttpClient httpClient,
            Executor executor,
            int maxConsecutiveErrorCount,
            Duration minErrorDuration,
            JsonCodec<TaskInfo> taskInfoCodec,
            JsonCodec<TaskUpdateRequest> taskUpdateRequestCodec)
    {
        checkNotNull(session, "session is null");
        checkNotNull(taskId, "taskId is null");
        checkNotNull(location, "location is null");
        checkNotNull(planFragment, "planFragment1 is null");
        checkNotNull(outputReceivers, "outputReceivers is null");
        checkNotNull(initialOutputIds, "initialOutputIds is null");
        checkNotNull(httpClient, "httpClient is null");
        checkNotNull(executor, "executor is null");
        checkNotNull(taskInfoCodec, "taskInfoCodec is null");
        checkNotNull(taskUpdateRequestCodec, "taskUpdateRequestCodec is null");

        this.session = session;
        this.nodeId = node.getNodeIdentifier();
        this.planFragment = planFragment;
        this.outputReceivers = ImmutableMap.copyOf(outputReceivers);
        this.outputIds.addAll(initialOutputIds);
        this.httpClient = httpClient;
        this.executor = executor;
        this.taskInfoCodec = taskInfoCodec;
        this.taskUpdateRequestCodec = taskUpdateRequestCodec;
        this.tupleInfos = planFragment.getTupleInfos();
        this.maxConsecutiveErrorCount = maxConsecutiveErrorCount;
        this.minErrorDuration = minErrorDuration;

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
            checkState(planFragment.isPartitioned(), "Plan is not partitioned");
            pendingSplits.put(planFragment.getPartitionedSource(), new ScheduledSplit(nextSplitId.getAndIncrement(), initialSplit));
        }

        taskInfo = new StateMachine<>("task " + taskId, executor, new TaskInfo(
                taskId,
                TaskInfo.MIN_VERSION,
                TaskState.PLANNED,
                location,
                new SharedBufferInfo(QueueState.OPEN, 0, bufferStates),
                ImmutableSet.<PlanNodeId>of(),
                new ExecutionStatsSnapshot(),
                ImmutableList.<SplitExecutionStats>of(),
                ImmutableList.<FailureInfo>of(),
                ImmutableMap.<PlanNodeId, Set<?>>of()));
    }

    @Override
    public TaskInfo getTaskInfo()
    {
        return taskInfo.get();
    }

    @Override
    public void start()
    {
        // to start we just need to trigger an update
        scheduleUpdate();
    }

    @Override
    public synchronized void addSplit(Split split)
    {
        checkNotNull(split, "split is null");
        checkState(!noMoreSplits, "noMoreSplits has already been set");
        checkState(planFragment.isPartitioned(), "Plan is not partitioned");

        // only add pending split if not done
        if (!getTaskInfo().getState().isDone()) {
            pendingSplits.put(planFragment.getPartitionedSource(), new ScheduledSplit(nextSplitId.getAndIncrement(), split));
            needsUpdate.set(true);
        }

        scheduleUpdate();
    }

    @Override
    public synchronized void noMoreSplits()
    {
        Preconditions.checkState(!noMoreSplits, "noMoreSplits has already been set");
        noMoreSplits = true;
        needsUpdate.set(true);

        scheduleUpdate();
    }

    @Override
    public synchronized void addExchangeLocations(Multimap<PlanNodeId, URI> additionalLocations, boolean noMore)
    {
        if (getTaskInfo().getState().isDone()) {
            return;
        }

        if (noMoreExchangeLocations == noMore && exchangeLocations.entries().containsAll(additionalLocations.entries())) {
            // duplicate request
            return;
        }
        Preconditions.checkState(!noMoreExchangeLocations, "Locations can not be added after noMoreExchangeLocations has been set");

        // determine which locations are new
        SetMultimap<PlanNodeId, URI> newExchangeLocations = HashMultimap.create(additionalLocations);
        newExchangeLocations.entries().removeAll(exchangeLocations.entries());

        // only add pending split if not done
        for (Entry<PlanNodeId, URI> entry : newExchangeLocations.entries()) {
            ScheduledSplit scheduledSplit = new ScheduledSplit(nextSplitId.getAndIncrement(), createRemoteSplitFor(entry.getValue()));
            pendingSplits.put(entry.getKey(), scheduledSplit);
        }
        exchangeLocations.putAll(additionalLocations);
        noMoreExchangeLocations = noMore;
        needsUpdate.set(true);

        scheduleUpdate();
    }

    @Override
    public synchronized void addOutputBuffers(Set<String> outputBuffers, boolean noMore)
    {
        if (getTaskInfo().getState().isDone()) {
            return;
        }

        if (noMoreOutputIds == noMore && this.outputIds.containsAll(outputBuffers)) {
            // duplicate request
            return;
        }
        Preconditions.checkState(!noMoreOutputIds, "New buffers can not be added after noMoreOutputIds has been set");

        outputIds.addAll(outputBuffers);
        noMoreOutputIds = noMore;
        needsUpdate.set(true);

        scheduleUpdate();
    }

    @Override
    public synchronized int getQueuedSplits()
    {
        int pendingSplitCount = 0;
        if (planFragment.isPartitioned()) {
            pendingSplitCount = pendingSplits.get(planFragment.getPartitionedSource()).size();
        }
        return pendingSplitCount + taskInfo.get().getStats().getQueuedSplits();
    }

    @Override
    public void addStateChangeListener(StateChangeListener<TaskInfo> stateChangeListener)
    {
        taskInfo.addStateChangeListener(stateChangeListener);
    }

    private synchronized void updateTaskInfo(final TaskInfo newValue)
    {
        for (Entry<PlanNodeId, Set<?>> entry : newValue.getOutputs().entrySet()) {
            OutputReceiver outputReceiver = outputReceivers.get(entry.getKey());
            checkState(outputReceiver != null, "Got Result for node %s which is not an output receiver!", entry.getKey());
            for (Object result : entry.getValue()) {
                outputReceiver.updateOutput(result);
            }
        }

        if (newValue.getState().isDone()) {
            // splits can be huge so clear the list
            pendingSplits.clear();
        }

        // change to new value if old value is not changed and new value has a newer version
        taskInfo.setIf(newValue, new Predicate<TaskInfo>()
        {
            public boolean apply(TaskInfo oldValue)
            {
                if (oldValue.getState().isDone()) {
                    // never update if the task has reached a terminal state
                    return false;
                }
                if (newValue.getVersion() < oldValue.getVersion()) {
                    // don't update to an older version (same version is ok)
                    return false;
                }
                return true;
            }
        });
    }

    private synchronized void scheduleUpdate()
    {
        // don't update if the task hasn't been started yet or if it is already finished
        if (!needsUpdate.get() || taskInfo.get().getState().isDone()) {
            return;
        }

        // if we have an old request outstanding, cancel it
        if (currentRequest != null && Duration.nanosSince(currentRequestStartNanos).compareTo(new Duration(2, TimeUnit.SECONDS)) >= 0) {
            needsUpdate.set(true);
            currentRequest.cancel(true);
            currentRequest = null;
            currentRequestStartNanos = 0;
        }

        // if there is a request already running, wait for it to complete
        if (this.currentRequest != null && !this.currentRequest.isDone()) {
            return;
        }

        // don't update too fast in the face of errors
        if (errorCount.get() > 0) {
            errorRequestRateLimiter.acquire();
        }

        List<TaskSource> sources = getSources();
        TaskUpdateRequest updateRequest = new TaskUpdateRequest(session,
                planFragment,
                sources,
                new OutputBuffers(outputIds, noMoreOutputIds));

        Request request = preparePost()
                .setUri(uriBuilderFrom(taskInfo.get().getSelf()).build())
                .setHeader(HttpHeaders.CONTENT_TYPE, MediaType.JSON_UTF_8.toString())
                .setBodyGenerator(jsonBodyGenerator(taskUpdateRequestCodec, updateRequest))
                .build();

        ListenableFuture<JsonResponse<TaskInfo>> future = httpClient.executeAsync(request, createFullJsonResponseHandler(taskInfoCodec));
        currentRequest = future;
        currentRequestStartNanos = System.nanoTime();

        Futures.addCallback(future, new SimpleHttpResponseHandler<>(new UpdateResponseHandler(sources), request.getUri()), executor);

        needsUpdate.set(false);
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

    @Override
    public synchronized void cancel()
    {
        // clear pending splits to free memory
        pendingSplits.clear();

        // cancel pending request
        if (currentRequest != null) {
            currentRequest.cancel(true);
            currentRequest = null;
            currentRequestStartNanos = 0;
        }

        // mark task as canceled (if not already done)
        TaskInfo taskInfo = getTaskInfo();
        updateTaskInfo(new TaskInfo(taskInfo.getTaskId(),
                TaskInfo.MAX_VERSION,
                TaskState.CANCELED,
                taskInfo.getSelf(),
                taskInfo.getOutputBuffers(),
                taskInfo.getNoMoreSplits(),
                taskInfo.getStats(),
                ImmutableList.<SplitExecutionStats>of(),
                ImmutableList.<FailureInfo>of(),
                ImmutableMap.<PlanNodeId, Set<?>>of()));

        // fire delete to task and ignore response
        if (taskInfo.getSelf() != null) {
            final long start = System.nanoTime();
            final Request request = prepareDelete().setUri(taskInfo.getSelf()).build();
            Futures.addCallback(httpClient.executeAsync(request, createStatusResponseHandler()), new FutureCallback<StatusResponse>()
            {
                @Override
                public void onSuccess(StatusResponse result)
                {
                    // assume any response is good enough
                }

                @Override
                public void onFailure(Throwable t)
                {
                    // reschedule
                    if (Duration.nanosSince(start).compareTo(new Duration(2, TimeUnit.MINUTES)) < 0) {
                        Futures.addCallback(httpClient.executeAsync(request, createStatusResponseHandler()), this, executor);
                    }
                    else {
                        log.error(t, "Unable to cancel task at %s", request.getUri());
                    }
                }
            }, executor);
        }
    }

    private synchronized void requestSucceeded(TaskInfo newValue, List<TaskSource> sources)
    {
        updateTaskInfo(newValue);
        lastSuccessfulRequest.set(System.nanoTime());
        errorCount.set(0);
        errorsSinceLastSuccess.clear();

        // remove acknowledged splits, which frees memory
        for (TaskSource source : sources) {
            PlanNodeId planNodeId = source.getPlanNodeId();
            for (ScheduledSplit split : source.getSplits()) {
                pendingSplits.remove(planNodeId, split);
            }
        }

        if (continuousTaskInfoFetcher == null) {
            continuousTaskInfoFetcher = new ContinuousTaskInfoFetcher();
            continuousTaskInfoFetcher.start();
        }
    }

    private synchronized void requestFailed(Throwable reason)
    {
        // cancellation is not a failure
        if (reason instanceof CancellationException) {
            return;
        }

        // log failure message
        TaskInfo taskInfo = getTaskInfo();
        if (isSocketError(reason)) {
            // don't print a stack for a socket error
            log.warn("Error updating task %s: %s: %s", taskInfo.getTaskId(), reason.getMessage(), taskInfo.getSelf());
        }
        else {
            log.warn(reason, "Error updating task %s: %s", taskInfo.getTaskId(), taskInfo.getSelf());
        }

        // remember the first 10 errors
        if (errorsSinceLastSuccess.size() < 10) {
            errorsSinceLastSuccess.add(reason);
        }

        // fail the task, if we have more than X failures in a row and more than Y seconds have passed since the last request
        long errorCount = this.errorCount.incrementAndGet();
        Duration timeSinceLastSuccess = Duration.nanosSince(lastSuccessfulRequest.get());
        if (errorCount > maxConsecutiveErrorCount && timeSinceLastSuccess.compareTo(minErrorDuration) > 0) {
            // it is weird to mark the task failed locally and then cancel the remote task, but there is no way to tell a remote task that it is failed
            RuntimeException exception = new RuntimeException(String.format("Too many requests to %s failed: %s failures: Time since last success %s",
                    taskInfo.getSelf(),
                    errorCount,
                    timeSinceLastSuccess));
            for (Throwable error : errorsSinceLastSuccess) {
                exception.addSuppressed(error);
            }
            failTask(exception);
            cancel();
        }
    }

    /**
     * Move the task directly to the failed state
     */
    private void failTask(Throwable cause)
    {
        TaskInfo taskInfo = getTaskInfo();
        if (!taskInfo.getState().isDone()) {
            log.debug(cause, "Remote task failed: %s", taskInfo.getSelf());
        }
        updateTaskInfo(new TaskInfo(taskInfo.getTaskId(),
                TaskInfo.MAX_VERSION,
                TaskState.FAILED,
                taskInfo.getSelf(),
                taskInfo.getOutputBuffers(),
                taskInfo.getNoMoreSplits(),
                taskInfo.getStats(),
                ImmutableList.<SplitExecutionStats>of(),
                ImmutableList.of(toFailure(cause)),
                taskInfo.getOutputs()));
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
                .addValue(getTaskInfo())
                .toString();
    }

    private class UpdateResponseHandler
            implements SimpleHttpResponseCallback<TaskInfo>
    {
        private final List<TaskSource> sources;

        private UpdateResponseHandler(List<TaskSource> sources)
        {
            this.sources = ImmutableList.copyOf(checkNotNull(sources, "sources is null"));
        }

        @Override
        public void success(TaskInfo value)
        {
            try {
                requestSucceeded(value, sources);
            }
            finally {
                scheduleUpdate();
            }
        }

        @Override
        public void failed(Throwable cause)
        {
            try {
                // on failure assume we need to update again
                needsUpdate.set(true);

                requestFailed(cause);
            }
            finally {
                scheduleUpdate();
            }
        }

        @Override
        public void fatal(Throwable cause)
        {
            failTask(cause);
        }
    }

    /**
     * Continuous update loop for task info.  Wait for a short period for task state to change, and
     * if it does not, return the current state of the task.  This will cause stats to be updated at
     * a regular interval, and state changes will be immediately recorded.
     */
    private class ContinuousTaskInfoFetcher
            implements SimpleHttpResponseCallback<TaskInfo>
    {
        @GuardedBy("this")
        private boolean running;

        @GuardedBy("this")
        private ListenableFuture<JsonResponse<TaskInfo>> future;

        public synchronized void start()
        {
            if (running) {
                // already running
                return;
            }
            running = true;
            scheduleNextRequest();
        }

        public synchronized void stop()
        {
            running = false;
            if (future != null) {
                future.cancel(true);
                future = null;
            }
        }

        private synchronized void scheduleNextRequest()
        {
            // stopped or done?
            TaskInfo taskInfo = HttpRemoteTask.this.taskInfo.get();
            if (!running || taskInfo.getState().isDone()) {
                return;
            }

            // outstanding request?
            if (future != null && !future.isDone()) {
                // this should never happen
                log.error("Can not reschedule update because an update is already running");
                return;
            }

            Request request = prepareGet()
                    .setUri(taskInfo.getSelf())
                    .setHeader(HttpHeaders.CONTENT_TYPE, MediaType.JSON_UTF_8.toString())
                    .setHeader(PrestoHeaders.PRESTO_CURRENT_STATE, taskInfo.getState().toString())
                    .setHeader(PrestoHeaders.PRESTO_MAX_WAIT, "200ms")
                    .build();

            future = httpClient.executeAsync(request, createFullJsonResponseHandler(taskInfoCodec));
            Futures.addCallback(future, new SimpleHttpResponseHandler<>(this, request.getUri()), executor);
        }

        @Override
        public void success(TaskInfo value)
        {
            synchronized (this) {
                future = null;
            }

            try {
                requestSucceeded(value, ImmutableList.<TaskSource>of());
            }
            finally {
                scheduleNextRequest();
            }
        }

        @Override
        public void failed(Throwable cause)
        {
            synchronized (this) {
                future = null;
            }

            try {
                requestFailed(cause);
            }
            finally {
                scheduleNextRequest();
            }
        }

        @Override
        public void fatal(Throwable cause)
        {
            synchronized (this) {
                future = null;
            }

            failTask(cause);
        }
    }

    public static class SimpleHttpResponseHandler<T>
            implements FutureCallback<JsonResponse<T>>
    {
        private final SimpleHttpResponseCallback<T> callback;

        private final URI uri;

        public SimpleHttpResponseHandler(SimpleHttpResponseCallback<T> callback, URI uri)
        {
            this.callback = callback;
            this.uri = uri;
        }

        @Override
        public void onSuccess(JsonResponse<T> response)
        {
            try {
                if (response.getStatusCode() == HttpStatus.OK.code() && response.hasValue()) {
                    callback.success(response.getValue());
                }
                else if (response.getStatusCode() == HttpStatus.SERVICE_UNAVAILABLE.code()) {
                    callback.failed(new RuntimeException("Server at %s returned SERVICE_UNAVAILABLE"));
                }
                else {
                    // Something is broken in the server or the client, so fail the task immediately (includes 500 errors)
                    Exception cause = response.getException();
                    if (cause == null) {
                        cause = new RuntimeException(String.format("Expected response code from %s to be %s, but was %s: %s",
                                uri,
                                HttpStatus.OK.code(),
                                response.getStatusCode(),
                                response.getStatusMessage()));
                    }
                    callback.fatal(cause);
                }
            }
            catch (Throwable t) {
                // this should never happen
                callback.failed(t);
            }
        }

        @Override
        public void onFailure(Throwable t)
        {
            callback.failed(t);
        }
    }

    public interface SimpleHttpResponseCallback<T>
    {
        void success(T value);

        void failed(Throwable cause);

        void fatal(Throwable cause);
    }

    private static boolean isSocketError(Throwable t)
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
