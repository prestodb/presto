/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.server;

import com.facebook.presto.OutputBuffers;
import com.facebook.presto.ScheduledSplit;
import com.facebook.presto.Session;
import com.facebook.presto.TaskSource;
import com.facebook.presto.client.PrestoHeaders;
import com.facebook.presto.execution.BufferInfo;
import com.facebook.presto.execution.NodeTaskMap.PartitionedSplitCountTracker;
import com.facebook.presto.execution.PageBufferInfo;
import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.execution.SharedBuffer.BufferState;
import com.facebook.presto.execution.SharedBufferInfo;
import com.facebook.presto.execution.StateMachine;
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskState;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.operator.TaskStats;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.ObjectArrays;
import com.google.common.collect.SetMultimap;
import com.google.common.net.HttpHeaders;
import com.google.common.net.MediaType;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import io.airlift.concurrent.SetThreadName;
import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.Request;
import io.airlift.http.client.StatusResponseHandler.StatusResponse;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.EOFException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static com.facebook.presto.spi.StandardErrorCode.REMOTE_TASK_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.REMOTE_TASK_MISMATCH;
import static com.facebook.presto.spi.StandardErrorCode.TOO_MANY_REQUESTS_FAILED;
import static com.facebook.presto.util.Failures.REMOTE_TASK_MISMATCH_ERROR;
import static com.facebook.presto.util.Failures.WORKER_NODE_ERROR;
import static com.facebook.presto.util.Failures.toFailure;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class HttpRemoteTask
        implements RemoteTask
{
    private static final Logger log = Logger.get(HttpRemoteTask.class);
    private static final Duration MAX_CLEANUP_RETRY_TIME = new Duration(2, TimeUnit.MINUTES);

    private final TaskId taskId;
    private final int partition;

    private final Session session;
    private final String nodeId;
    private final PlanFragment planFragment;

    private final AtomicLong nextSplitId = new AtomicLong();

    private final StateMachine<TaskInfo> taskInfo;

    @GuardedBy("this")
    private Future<?> currentRequest;
    @GuardedBy("this")
    private long currentRequestStartNanos;

    @GuardedBy("this")
    private final SetMultimap<PlanNodeId, ScheduledSplit> pendingSplits = HashMultimap.create();
    @GuardedBy("this")
    private volatile int pendingSourceSplitCount;
    @GuardedBy("this")
    private final Set<PlanNodeId> noMoreSplits = new HashSet<>();
    @GuardedBy("this")
    private final AtomicReference<OutputBuffers> outputBuffers = new AtomicReference<>();

    private final boolean summarizeTaskInfo;
    private final Duration requestTimeout;
    private final ContinuousTaskInfoFetcher continuousTaskInfoFetcher;

    private final HttpClient httpClient;
    private final Executor executor;
    private final ScheduledExecutorService errorScheduledExecutor;
    private final JsonCodec<TaskInfo> taskInfoCodec;
    private final JsonCodec<TaskUpdateRequest> taskUpdateRequestCodec;

    private final RequestErrorTracker updateErrorTracker;
    private final RequestErrorTracker getErrorTracker;

    private final AtomicBoolean needsUpdate = new AtomicBoolean(true);
    private final AtomicBoolean sendPlan = new AtomicBoolean(true);

    private final PartitionedSplitCountTracker partitionedSplitCountTracker;

    public HttpRemoteTask(Session session,
            TaskId taskId,
            String nodeId,
            int partition,
            URI location,
            PlanFragment planFragment,
            Multimap<PlanNodeId, Split> initialSplits,
            OutputBuffers outputBuffers,
            HttpClient httpClient,
            Executor executor,
            ScheduledExecutorService errorScheduledExecutor,
            Duration minErrorDuration,
            Duration refreshMaxWait,
            boolean summarizeTaskInfo,
            JsonCodec<TaskInfo> taskInfoCodec,
            JsonCodec<TaskUpdateRequest> taskUpdateRequestCodec,
            PartitionedSplitCountTracker partitionedSplitCountTracker)
    {
        requireNonNull(session, "session is null");
        requireNonNull(taskId, "taskId is null");
        requireNonNull(nodeId, "nodeId is null");
        requireNonNull(location, "location is null");
        checkArgument(partition >= 0, "partition is negative");
        requireNonNull(planFragment, "planFragment1 is null");
        requireNonNull(outputBuffers, "outputBuffers is null");
        requireNonNull(httpClient, "httpClient is null");
        requireNonNull(executor, "executor is null");
        requireNonNull(taskInfoCodec, "taskInfoCodec is null");
        requireNonNull(taskUpdateRequestCodec, "taskUpdateRequestCodec is null");
        requireNonNull(partitionedSplitCountTracker, "partitionedSplitCountTracker is null");

        try (SetThreadName ignored = new SetThreadName("HttpRemoteTask-%s", taskId)) {
            this.taskId = taskId;
            this.session = session;
            this.nodeId = nodeId;
            this.partition = partition;
            this.planFragment = planFragment;
            this.outputBuffers.set(outputBuffers);
            this.httpClient = httpClient;
            this.executor = executor;
            this.errorScheduledExecutor = errorScheduledExecutor;
            this.summarizeTaskInfo = summarizeTaskInfo;
            this.taskInfoCodec = taskInfoCodec;
            this.taskUpdateRequestCodec = taskUpdateRequestCodec;
            this.updateErrorTracker = new RequestErrorTracker(taskId, location, minErrorDuration, errorScheduledExecutor, "updating task");
            this.getErrorTracker = new RequestErrorTracker(taskId, location, minErrorDuration, errorScheduledExecutor, "getting info for task");
            this.partitionedSplitCountTracker = requireNonNull(partitionedSplitCountTracker, "partitionedSplitCountTracker is null");

            for (Entry<PlanNodeId, Split> entry : requireNonNull(initialSplits, "initialSplits is null").entries()) {
                ScheduledSplit scheduledSplit = new ScheduledSplit(nextSplitId.getAndIncrement(), entry.getValue());
                pendingSplits.put(entry.getKey(), scheduledSplit);
            }
            if (initialSplits.containsKey(planFragment.getPartitionedSource())) {
                pendingSourceSplitCount = initialSplits.get(planFragment.getPartitionedSource()).size();
            }

            List<BufferInfo> bufferStates = outputBuffers.getBuffers()
                    .keySet().stream()
                    .map(outputId -> new BufferInfo(outputId, false, 0, 0, PageBufferInfo.empty()))
                    .collect(toImmutableList());

            TaskStats taskStats = new TaskStats(DateTime.now(), null);

            taskInfo = new StateMachine<>("task " + taskId, executor, new TaskInfo(
                    taskId,
                    "",
                    TaskInfo.MIN_VERSION,
                    TaskState.PLANNED,
                    location,
                    DateTime.now(),
                    new SharedBufferInfo(BufferState.OPEN, true, true, 0, 0, 0, 0, bufferStates),
                    ImmutableSet.of(),
                    taskStats,
                    ImmutableList.of(),
                    true));

            long timeout = minErrorDuration.toMillis() / 3;
            requestTimeout = new Duration(timeout + refreshMaxWait.toMillis(), MILLISECONDS);
            continuousTaskInfoFetcher = new ContinuousTaskInfoFetcher(refreshMaxWait);

            partitionedSplitCountTracker.setPartitionedSplitCount(getPartitionedSplitCount());
        }
    }

    @Override
    public TaskId getTaskId()
    {
        return taskId;
    }

    @Override
    public String getNodeId()
    {
        return nodeId;
    }

    @Override
    public int getPartition()
    {
        return partition;
    }

    @Override
    public TaskInfo getTaskInfo()
    {
        return taskInfo.get();
    }

    @Override
    public void start()
    {
        try (SetThreadName ignored = new SetThreadName("HttpRemoteTask-%s", taskId)) {
            // to start we just need to trigger an update
            scheduleUpdate();

            // begin the info fetcher
            continuousTaskInfoFetcher.start();
        }
    }

    @Override
    public synchronized void addSplits(Multimap<PlanNodeId, Split> splitsBySource)
    {
        try (SetThreadName ignored = new SetThreadName("HttpRemoteTask-%s", taskId)) {
            requireNonNull(splitsBySource, "splitsBySource is null");

            // only add pending split if not done
            if (getTaskInfo().getState().isDone()) {
                return;
            }

            for (Entry<PlanNodeId, Collection<Split>> entry : splitsBySource.asMap().entrySet()) {
                PlanNodeId sourceId = entry.getKey();
                Collection<Split> splits = entry.getValue();

                checkState(!noMoreSplits.contains(sourceId), "noMoreSplits has already been set for %s", sourceId);
                checkState(!noMoreSplits.contains(sourceId), "noMoreSplits has already been set for %s", sourceId);
                int added = 0;
                for (Split split : splits) {
                    if (pendingSplits.put(sourceId, new ScheduledSplit(nextSplitId.getAndIncrement(), split))) {
                        added++;
                    }
                }
                if (sourceId.equals(planFragment.getPartitionedSource())) {
                    pendingSourceSplitCount += added;
                    partitionedSplitCountTracker.setPartitionedSplitCount(getPartitionedSplitCount());
                }
                needsUpdate.set(true);
            }

            scheduleUpdate();
        }
    }

    @Override
    public synchronized void noMoreSplits(PlanNodeId sourceId)
    {
        if (noMoreSplits.add(sourceId)) {
            needsUpdate.set(true);
            scheduleUpdate();
        }
    }

    @Override
    public synchronized void setOutputBuffers(OutputBuffers newOutputBuffers)
    {
        if (getTaskInfo().getState().isDone()) {
            return;
        }

        if (newOutputBuffers.getVersion() > outputBuffers.get().getVersion()) {
            outputBuffers.set(newOutputBuffers);
            needsUpdate.set(true);
            scheduleUpdate();
        }
    }

    @Override
    public int getPartitionedSplitCount()
    {
        TaskInfo taskInfo = this.taskInfo.get();
        if (taskInfo.getState().isDone()) {
            return 0;
        }
        return getPendingSourceSplitCount() + taskInfo.getStats().getQueuedPartitionedDrivers() + taskInfo.getStats().getRunningPartitionedDrivers();
    }

    @Override
    public int getQueuedPartitionedSplitCount()
    {
        TaskInfo taskInfo = this.taskInfo.get();
        if (taskInfo.getState().isDone()) {
            return 0;
        }
        return getPendingSourceSplitCount() + taskInfo.getStats().getQueuedPartitionedDrivers();
    }

    @SuppressWarnings("FieldAccessNotGuarded")
    private int getPendingSourceSplitCount()
    {
        return pendingSourceSplitCount;
    }

    @Override
    public void addStateChangeListener(StateChangeListener<TaskInfo> stateChangeListener)
    {
        try (SetThreadName ignored = new SetThreadName("HttpRemoteTask-%s", taskId)) {
            taskInfo.addStateChangeListener(stateChangeListener);
        }
    }

    @Override
    public CompletableFuture<TaskInfo> getStateChange(TaskInfo taskInfo)
    {
        return this.taskInfo.getStateChange(taskInfo);
    }

    private synchronized void updateTaskInfo(TaskInfo newValue)
    {
        updateTaskInfo(newValue, ImmutableList.of());
    }

    private synchronized void updateTaskInfo(TaskInfo newValue, List<TaskSource> sources)
    {
        if (newValue.getState().isDone()) {
            // splits can be huge so clear the list
            pendingSplits.clear();
            pendingSourceSplitCount = 0;
        }

        // change to new value if old value is not changed and new value has a newer version
        AtomicBoolean taskMismatch = new AtomicBoolean();
        taskInfo.setIf(newValue, oldValue -> {
            // did the task instance id change
            if (!isNullOrEmpty(oldValue.getTaskInstanceId()) && !oldValue.getTaskInstanceId().equals(newValue.getTaskInstanceId())) {
                taskMismatch.set(true);
                return false;
            }

            if (oldValue.getState().isDone()) {
                // never update if the task has reached a terminal state
                return false;
            }
            // don't update to an older version (same version is ok)
            return newValue.getVersion() >= oldValue.getVersion();
        });

        if (taskMismatch.get()) {
            failTask(new PrestoException(REMOTE_TASK_MISMATCH, REMOTE_TASK_MISMATCH_ERROR));
            abort();
        }

        // remove acknowledged splits, which frees memory
        for (TaskSource source : sources) {
            PlanNodeId planNodeId = source.getPlanNodeId();
            int removed = 0;
            for (ScheduledSplit split : source.getSplits()) {
                if (pendingSplits.remove(planNodeId, split)) {
                    removed++;
                }
            }
            if (planNodeId.equals(planFragment.getPartitionedSource())) {
                pendingSourceSplitCount -= removed;
            }
        }

        partitionedSplitCountTracker.setPartitionedSplitCount(getPartitionedSplitCount());
    }

    private void scheduleUpdate()
    {
        executor.execute(this::sendUpdate);
    }

    private synchronized void sendUpdate()
    {
        // don't update if the task hasn't been started yet or if it is already finished
        if (!needsUpdate.get() || taskInfo.get().getState().isDone()) {
            return;
        }

        // if we have an old request outstanding, cancel it
        if (currentRequest != null && Duration.nanosSince(currentRequestStartNanos).compareTo(requestTimeout) >= 0) {
            needsUpdate.set(true);
            currentRequest.cancel(true);
            currentRequest = null;
            currentRequestStartNanos = 0;
        }

        // if there is a request already running, wait for it to complete
        if (this.currentRequest != null && !this.currentRequest.isDone()) {
            return;
        }

        // if throttled due to error, asynchronously wait for timeout and try again
        ListenableFuture<?> errorRateLimit = updateErrorTracker.acquireRequestPermit();
        if (!errorRateLimit.isDone()) {
            errorRateLimit.addListener(this::sendUpdate, executor);
            return;
        }

        List<TaskSource> sources = getSources();

        Optional<PlanFragment> fragment = Optional.empty();
        if (sendPlan.get()) {
            fragment = Optional.of(planFragment);
        }
        TaskUpdateRequest updateRequest = new TaskUpdateRequest(session.toSessionRepresentation(),
                fragment,
                sources,
                outputBuffers.get());

        HttpUriBuilder uriBuilder = uriBuilderFrom(taskInfo.get().getSelf());
        if (summarizeTaskInfo) {
            uriBuilder.addParameter("summarize");
        }
        Request request = preparePost()
                .setUri(uriBuilder.build())
                .setHeader(HttpHeaders.CONTENT_TYPE, MediaType.JSON_UTF_8.toString())
                .setBodyGenerator(jsonBodyGenerator(taskUpdateRequestCodec, updateRequest))
                .build();

        updateErrorTracker.startRequest();

        ListenableFuture<JsonResponse<TaskInfo>> future = httpClient.executeAsync(request, createFullJsonResponseHandler(taskInfoCodec));
        currentRequest = future;
        currentRequestStartNanos = System.nanoTime();

        // The needsUpdate flag needs to be set to false BEFORE adding the Future callback since callback might change the flag value
        // and does so without grabbing the instance lock.
        needsUpdate.set(false);

        Futures.addCallback(future, new SimpleHttpResponseHandler<>(new UpdateResponseHandler(sources), request.getUri()), executor);
    }

    private synchronized List<TaskSource> getSources()
    {
        return Stream.concat(Stream.of(planFragment.getPartitionedSourceNode()), planFragment.getRemoteSourceNodes().stream())
                .filter(Objects::nonNull)
                .map(PlanNode::getId)
                .map(this::getSource)
                .filter(Objects::nonNull)
                .collect(toImmutableList());
    }

    private synchronized TaskSource getSource(PlanNodeId planNodeId)
    {
        Set<ScheduledSplit> splits = pendingSplits.get(planNodeId);
        boolean noMoreSplits = this.noMoreSplits.contains(planNodeId);
        TaskSource element = null;
        if (!splits.isEmpty() || noMoreSplits) {
            element = new TaskSource(planNodeId, splits, noMoreSplits);
        }
        return element;
    }

    @Override
    public synchronized void cancel()
    {
        try (SetThreadName ignored = new SetThreadName("HttpRemoteTask-%s", taskId)) {
            if (getTaskInfo().getState().isDone()) {
                return;
            }
            checkState(continuousTaskInfoFetcher.isRunning(), "Cannot cancel task when it is not running");

            URI uri = getTaskInfo().getSelf();
            if (uri == null) {
                return;
            }

            // send cancel to task and ignore response
            HttpUriBuilder uriBuilder = uriBuilderFrom(uri).addParameter("abort", "false");
            if (summarizeTaskInfo) {
                uriBuilder.addParameter("summarize");
            }
            Request request = prepareDelete()
                    .setUri(uriBuilder.build())
                    .build();
            scheduleAsyncCleanupRequest(new Backoff(MAX_CLEANUP_RETRY_TIME), request, "cancel");
        }
    }

    @Override
    public synchronized void abort()
    {
        try (SetThreadName ignored = new SetThreadName("HttpRemoteTask-%s", taskId)) {
            // clear pending splits to free memory
            pendingSplits.clear();
            pendingSourceSplitCount = 0;
            partitionedSplitCountTracker.setPartitionedSplitCount(getPartitionedSplitCount());

            // cancel pending request
            if (currentRequest != null) {
                currentRequest.cancel(true);
                currentRequest = null;
                currentRequestStartNanos = 0;
            }

            // mark task as canceled (if not already done)
            TaskInfo taskInfo = getTaskInfo();
            URI uri = taskInfo.getSelf();

            updateTaskInfo(new TaskInfo(taskInfo.getTaskId(),
                    taskInfo.getTaskInstanceId(),
                    TaskInfo.MAX_VERSION,
                    TaskState.ABORTED,
                    uri,
                    taskInfo.getLastHeartbeat(),
                    taskInfo.getOutputBuffers(),
                    taskInfo.getNoMoreSplits(),
                    taskInfo.getStats(),
                    ImmutableList.of(),
                    taskInfo.isNeedsPlan()));

            // send abort to task and ignore response
            HttpUriBuilder uriBuilder = uriBuilderFrom(uri);
            if (summarizeTaskInfo) {
                uriBuilder.addParameter("summarize");
            }
            Request request = prepareDelete()
                    .setUri(uriBuilder.build())
                    .build();
            scheduleAsyncCleanupRequest(new Backoff(MAX_CLEANUP_RETRY_TIME), request, "abort");
        }
    }

    private void scheduleAsyncCleanupRequest(Backoff cleanupBackoff, Request request, String action)
    {
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
                if (t instanceof RejectedExecutionException) {
                    // client has been shutdown
                    return;
                }

                // record failure
                if (cleanupBackoff.failure()) {
                    logError(t, "Unable to %s task at %s", action, request.getUri());
                    return;
                }

                // reschedule
                long delayNanos = cleanupBackoff.getBackoffDelayNanos();
                if (delayNanos == 0) {
                    scheduleAsyncCleanupRequest(cleanupBackoff, request, action);
                }
                else {
                    errorScheduledExecutor.schedule(() -> scheduleAsyncCleanupRequest(cleanupBackoff, request, action), delayNanos, NANOSECONDS);
                }
            }
        }, executor);
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
                taskInfo.getTaskInstanceId(),
                TaskInfo.MAX_VERSION,
                TaskState.FAILED,
                taskInfo.getSelf(),
                taskInfo.getLastHeartbeat(),
                taskInfo.getOutputBuffers(),
                taskInfo.getNoMoreSplits(),
                taskInfo.getStats(),
                ImmutableList.of(toFailure(cause)),
                taskInfo.isNeedsPlan()));
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(getTaskInfo())
                .toString();
    }

    private class UpdateResponseHandler
            implements SimpleHttpResponseCallback<TaskInfo>
    {
        private final List<TaskSource> sources;

        private UpdateResponseHandler(List<TaskSource> sources)
        {
            this.sources = ImmutableList.copyOf(requireNonNull(sources, "sources is null"));
        }

        @Override
        public void success(TaskInfo value)
        {
            try (SetThreadName ignored = new SetThreadName("UpdateResponseHandler-%s", taskId)) {
                try {
                    synchronized (HttpRemoteTask.this) {
                        currentRequest = null;
                        sendPlan.set(value.isNeedsPlan());
                    }
                    updateTaskInfo(value, sources);
                    updateErrorTracker.requestSucceeded();
                }
                finally {
                    sendUpdate();
                }
            }
        }

        @Override
        public void failed(Throwable cause)
        {
            try (SetThreadName ignored = new SetThreadName("UpdateResponseHandler-%s", taskId)) {
                try {
                    synchronized (HttpRemoteTask.this) {
                        currentRequest = null;
                    }

                    // on failure assume we need to update again
                    needsUpdate.set(true);

                    // if task not already done, record error
                    TaskInfo taskInfo = getTaskInfo();
                    if (!taskInfo.getState().isDone()) {
                        updateErrorTracker.requestFailed(cause);
                    }
                }
                catch (Error e) {
                    failTask(e);
                    abort();
                    throw e;
                }
                catch (RuntimeException e) {
                    failTask(e);
                    abort();
                }
                finally {
                    sendUpdate();
                }
            }
        }

        @Override
        public void fatal(Throwable cause)
        {
            try (SetThreadName ignored = new SetThreadName("UpdateResponseHandler-%s", taskId)) {
                failTask(cause);
            }
        }
    }

    /**
     * Continuous update loop for task info.  Wait for a short period for task state to change, and
     * if it does not, return the current state of the task.  This will cause stats to be updated at a
     * regular interval, and state changes will be immediately recorded.
     */
    private class ContinuousTaskInfoFetcher
            implements SimpleHttpResponseCallback<TaskInfo>
    {
        private final Duration refreshMaxWait;

        @GuardedBy("this")
        private boolean running;

        @GuardedBy("this")
        private ListenableFuture<JsonResponse<TaskInfo>> future;

        public ContinuousTaskInfoFetcher(Duration refreshMaxWait)
        {
            this.refreshMaxWait = refreshMaxWait;
        }

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

            // if throttled due to error, asynchronously wait for timeout and try again
            ListenableFuture<?> errorRateLimit = getErrorTracker.acquireRequestPermit();
            if (!errorRateLimit.isDone()) {
                errorRateLimit.addListener(this::scheduleNextRequest, executor);
                return;
            }

            HttpUriBuilder uriBuilder = uriBuilderFrom(taskInfo.getSelf());
            if (summarizeTaskInfo) {
                uriBuilder.addParameter("summarize");
            }
            Request request = prepareGet()
                    .setUri(uriBuilder.build())
                    .setHeader(HttpHeaders.CONTENT_TYPE, MediaType.JSON_UTF_8.toString())
                    .setHeader(PrestoHeaders.PRESTO_CURRENT_STATE, taskInfo.getState().toString())
                    .setHeader(PrestoHeaders.PRESTO_MAX_WAIT, refreshMaxWait.toString())
                    .build();

            getErrorTracker.startRequest();

            future = httpClient.executeAsync(request, createFullJsonResponseHandler(taskInfoCodec));
            Futures.addCallback(future, new SimpleHttpResponseHandler<>(this, request.getUri()), executor);
        }

        @Override
        public void success(TaskInfo value)
        {
            try (SetThreadName ignored = new SetThreadName("ContinuousTaskInfoFetcher-%s", taskId)) {
                synchronized (this) {
                    future = null;
                }

                try {
                    updateTaskInfo(value, ImmutableList.of());
                    getErrorTracker.requestSucceeded();
                }
                finally {
                    scheduleNextRequest();
                }
            }
        }

        @Override
        public void failed(Throwable cause)
        {
            try (SetThreadName ignored = new SetThreadName("ContinuousTaskInfoFetcher-%s", taskId)) {
                synchronized (this) {
                    future = null;
                }

                try {
                    // if task not already done, record error
                    TaskInfo taskInfo = getTaskInfo();
                    if (!taskInfo.getState().isDone()) {
                        getErrorTracker.requestFailed(cause);
                    }
                }
                catch (Error e) {
                    failTask(e);
                    abort();
                    throw e;
                }
                catch (RuntimeException e) {
                    failTask(e);
                    abort();
                }
                finally {
                    // there is no back off here so we can get a lot of error messages when a server spins
                    // down, but it typically goes away quickly because the queries get canceled
                    scheduleNextRequest();
                }
            }
        }

        @Override
        public void fatal(Throwable cause)
        {
            try (SetThreadName ignored = new SetThreadName("ContinuousTaskInfoFetcher-%s", taskId)) {
                synchronized (this) {
                    future = null;
                }

                failTask(cause);
            }
        }

        public synchronized boolean isRunning()
        {
            return running;
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
                    callback.failed(new ServiceUnavailableException(uri));
                }
                else {
                    // Something is broken in the server or the client, so fail the task immediately (includes 500 errors)
                    Exception cause = response.getException();
                    if (cause == null) {
                        if (response.getStatusCode() == HttpStatus.OK.code()) {
                            cause = new PrestoException(REMOTE_TASK_ERROR, format("Expected response from %s is empty", uri));
                        }
                        else {
                            cause = new PrestoException(REMOTE_TASK_ERROR, format("Expected response code from %s to be %s, but was %s: %s%n%s",
                                    uri,
                                    HttpStatus.OK.code(),
                                    response.getStatusCode(),
                                    response.getStatusMessage(),
                                    response.getResponseBody()));
                        }
                    }
                    callback.fatal(cause);
                }
            }
            catch (Throwable t) {
                // this should never happen
                callback.fatal(t);
            }
        }

        @Override
        public void onFailure(Throwable t)
        {
            callback.failed(t);
        }
    }

    @ThreadSafe
    private static class RequestErrorTracker
    {
        private final TaskId taskId;
        private final URI taskUri;
        private final ScheduledExecutorService scheduledExecutor;
        private final String jobDescription;
        private final Backoff backoff;

        private final Queue<Throwable> errorsSinceLastSuccess = new ConcurrentLinkedQueue<>();

        public RequestErrorTracker(TaskId taskId, URI taskUri, Duration minErrorDuration, ScheduledExecutorService scheduledExecutor, String jobDescription)
        {
            this.taskId = taskId;
            this.taskUri = taskUri;
            this.scheduledExecutor = scheduledExecutor;
            this.backoff = new Backoff(minErrorDuration);
            this.jobDescription = jobDescription;
        }

        public ListenableFuture<?> acquireRequestPermit()
        {
            long delayNanos = backoff.getBackoffDelayNanos();

            if (delayNanos == 0) {
                return Futures.immediateFuture(null);
            }

            ListenableFutureTask<Object> futureTask = ListenableFutureTask.create(() -> null);
            scheduledExecutor.schedule(futureTask, delayNanos, NANOSECONDS);
            return futureTask;
        }

        public void startRequest()
        {
            // before scheduling a new request clear the error timer
            // we consider a request to be "new" if there are no current failures
            if (backoff.getFailureCount() == 0) {
                requestSucceeded();
            }
        }

        public void requestSucceeded()
        {
            backoff.success();
            errorsSinceLastSuccess.clear();
        }

        public void requestFailed(Throwable reason)
                throws PrestoException
        {
            // cancellation is not a failure
            if (reason instanceof CancellationException) {
                return;
            }

            if (reason instanceof RejectedExecutionException) {
                throw new PrestoException(REMOTE_TASK_ERROR, reason);
            }

            // log failure message
            if (isExpectedError(reason)) {
                // don't print a stack for a known errors
                log.warn("Error " + jobDescription + " %s: %s: %s", taskId, reason.getMessage(), taskUri);
            }
            else {
                log.warn(reason, "Error " + jobDescription + " %s: %s", taskId, taskUri);
            }

            // remember the first 10 errors
            if (errorsSinceLastSuccess.size() < 10) {
                errorsSinceLastSuccess.add(reason);
            }

            // fail the task, if we have more than X failures in a row and more than Y seconds have passed since the last request
            if (backoff.failure()) {
                // it is weird to mark the task failed locally and then cancel the remote task, but there is no way to tell a remote task that it is failed
                PrestoException exception = new PrestoException(TOO_MANY_REQUESTS_FAILED,
                        format("%s (%s %s - %s failures, time since last success %s)",
                                WORKER_NODE_ERROR,
                                jobDescription,
                                taskUri,
                                backoff.getFailureCount(),
                                backoff.getTimeSinceLastSuccess().convertTo(SECONDS)));
                errorsSinceLastSuccess.forEach(exception::addSuppressed);
                throw exception;
            }
        }
    }

    public interface SimpleHttpResponseCallback<T>
    {
        void success(T value);

        void failed(Throwable cause);

        void fatal(Throwable cause);
    }

    private static void logError(Throwable t, String format, Object... args)
    {
        if (isExpectedError(t)) {
            log.error(format + ": %s", ObjectArrays.concat(args, t));
        }
        else {
            log.error(t, format, args);
        }
    }

    private static boolean isExpectedError(Throwable t)
    {
        while (t != null) {
            if ((t instanceof SocketException) ||
                    (t instanceof SocketTimeoutException) ||
                    (t instanceof EOFException) ||
                    (t instanceof TimeoutException) ||
                    (t instanceof ServiceUnavailableException)) {
                return true;
            }
            t = t.getCause();
        }
        return false;
    }

    private static class ServiceUnavailableException
            extends RuntimeException
    {
        public ServiceUnavailableException(URI uri)
        {
            super("Server returned SERVICE_UNAVAILABLE: " + uri);
        }
    }
}
