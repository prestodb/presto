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
package com.facebook.presto.server.remotetask;

import com.facebook.airlift.concurrent.SetThreadName;
import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.HttpUriBuilder;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.http.client.ResponseHandler;
import com.facebook.airlift.http.client.StatusResponseHandler.StatusResponse;
import com.facebook.airlift.log.Logger;
import com.facebook.drift.transport.netty.codec.Protocol;
import com.facebook.presto.Session;
import com.facebook.presto.execution.FutureStateChange;
import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.execution.NodeTaskMap.PartitionedSplitCountTracker;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.execution.ScheduledSplit;
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.execution.TaskState;
import com.facebook.presto.execution.TaskStatus;
import com.facebook.presto.execution.buffer.BufferInfo;
import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.execution.buffer.PageBufferInfo;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.MetadataUpdates;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.operator.TaskStats;
import com.facebook.presto.server.RequestErrorTracker;
import com.facebook.presto.server.SimpleHttpResponseCallback;
import com.facebook.presto.server.SimpleHttpResponseHandler;
import com.facebook.presto.server.TaskUpdateRequest;
import com.facebook.presto.server.codec.Codec;
import com.facebook.presto.server.smile.BaseResponse;
import com.facebook.presto.server.smile.SmileCodec;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.sql.planner.PlanFragment;
import com.google.common.base.Ticker;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.ObjectArrays;
import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static com.facebook.airlift.http.client.HttpStatus.NO_CONTENT;
import static com.facebook.airlift.http.client.HttpStatus.OK;
import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static com.facebook.airlift.http.client.Request.Builder.prepareDelete;
import static com.facebook.airlift.http.client.Request.Builder.preparePost;
import static com.facebook.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static com.facebook.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static com.facebook.presto.execution.TaskInfo.createInitialTask;
import static com.facebook.presto.execution.TaskState.ABORTED;
import static com.facebook.presto.execution.TaskState.FAILED;
import static com.facebook.presto.execution.TaskStatus.failWith;
import static com.facebook.presto.server.RequestErrorTracker.isExpectedError;
import static com.facebook.presto.server.RequestErrorTracker.taskRequestErrorTracker;
import static com.facebook.presto.server.RequestHelpers.setContentTypeHeaders;
import static com.facebook.presto.server.smile.AdaptingJsonResponseHandler.createAdaptingJsonResponseHandler;
import static com.facebook.presto.server.smile.FullSmileResponseHandler.createFullSmileResponseHandler;
import static com.facebook.presto.server.smile.JsonCodecWrapper.unwrapJsonCodec;
import static com.facebook.presto.spi.StandardErrorCode.EXCEEDED_TASK_UPDATE_SIZE_LIMIT;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.REMOTE_TASK_ERROR;
import static com.facebook.presto.util.Failures.toFailure;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.util.concurrent.Futures.addCallback;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class HttpRemoteTask
        implements RemoteTask
{
    private static final Logger log = Logger.get(HttpRemoteTask.class);
    private static final double UPDATE_WITHOUT_PLAN_STATS_SAMPLE_RATE = 0.01;

    private final TaskId taskId;
    private final URI taskLocation;
    private final URI remoteTaskLocation;

    private final Session session;
    private final String nodeId;
    private final PlanFragment planFragment;

    private final Set<PlanNodeId> tableScanPlanNodeIds;
    private final Set<PlanNodeId> remoteSourcePlanNodeIds;

    private final AtomicLong nextSplitId = new AtomicLong();

    private final Duration maxErrorDuration;
    private final RemoteTaskStats stats;
    private final TaskInfoFetcher taskInfoFetcher;
    private final ContinuousTaskStatusFetcher taskStatusFetcher;

    @GuardedBy("this")
    private Future<?> currentRequest;
    @GuardedBy("this")
    private long currentRequestStartNanos;

    @GuardedBy("this")
    private final SetMultimap<PlanNodeId, ScheduledSplit> pendingSplits = HashMultimap.create();
    @GuardedBy("this")
    private volatile int pendingSourceSplitCount;
    @GuardedBy("this")
    private final SetMultimap<PlanNodeId, Lifespan> pendingNoMoreSplitsForLifespan = HashMultimap.create();
    @GuardedBy("this")
    // The keys of this map represent all plan nodes that have "no more splits".
    // The boolean value of each entry represents whether the "no more splits" notification is pending delivery to workers.
    private final Map<PlanNodeId, Boolean> noMoreSplits = new HashMap<>();
    @GuardedBy("this")
    private final AtomicReference<OutputBuffers> outputBuffers = new AtomicReference<>();
    private final FutureStateChange<?> whenSplitQueueHasSpace = new FutureStateChange<>();
    @GuardedBy("this")
    private boolean splitQueueHasSpace = true;
    @GuardedBy("this")
    private OptionalInt whenSplitQueueHasSpaceThreshold = OptionalInt.empty();

    private final boolean summarizeTaskInfo;

    private final HttpClient httpClient;
    private final Executor executor;
    private final ScheduledExecutorService errorScheduledExecutor;

    private final Codec<TaskInfo> taskInfoCodec;
    private final Codec<TaskUpdateRequest> taskUpdateRequestCodec;
    private final Codec<PlanFragment> planFragmentCodec;

    private final RequestErrorTracker updateErrorTracker;

    private final AtomicBoolean needsUpdate = new AtomicBoolean(true);
    private final AtomicBoolean sendPlan = new AtomicBoolean(true);

    private final PartitionedSplitCountTracker partitionedSplitCountTracker;

    private final AtomicBoolean aborting = new AtomicBoolean(false);

    private final boolean binaryTransportEnabled;
    private final boolean thriftTransportEnabled;
    private final Protocol thriftProtocol;
    private final int maxTaskUpdateSizeInBytes;

    private final TableWriteInfo tableWriteInfo;

    public HttpRemoteTask(
            Session session,
            TaskId taskId,
            String nodeId,
            URI location,
            URI remoteLocation,
            PlanFragment planFragment,
            Multimap<PlanNodeId, Split> initialSplits,
            OutputBuffers outputBuffers,
            HttpClient httpClient,
            Executor executor,
            ScheduledExecutorService updateScheduledExecutor,
            ScheduledExecutorService errorScheduledExecutor,
            Duration maxErrorDuration,
            Duration taskStatusRefreshMaxWait,
            Duration taskInfoRefreshMaxWait,
            Duration taskInfoUpdateInterval,
            boolean summarizeTaskInfo,
            Codec<TaskStatus> taskStatusCodec,
            Codec<TaskInfo> taskInfoCodec,
            Codec<TaskUpdateRequest> taskUpdateRequestCodec,
            Codec<PlanFragment> planFragmentCodec,
            Codec<MetadataUpdates> metadataUpdatesCodec,
            PartitionedSplitCountTracker partitionedSplitCountTracker,
            RemoteTaskStats stats,
            boolean binaryTransportEnabled,
            boolean thriftTransportEnabled,
            Protocol thriftProtocol,
            TableWriteInfo tableWriteInfo,
            int maxTaskUpdateSizeInBytes,
            MetadataManager metadataManager,
            QueryManager queryManager)
    {
        requireNonNull(session, "session is null");
        requireNonNull(taskId, "taskId is null");
        requireNonNull(nodeId, "nodeId is null");
        requireNonNull(location, "location is null");
        requireNonNull(remoteLocation, "remoteLocation is null");
        requireNonNull(planFragment, "planFragment is null");
        requireNonNull(outputBuffers, "outputBuffers is null");
        requireNonNull(httpClient, "httpClient is null");
        requireNonNull(executor, "executor is null");
        requireNonNull(taskStatusCodec, "taskStatusCodec is null");
        requireNonNull(taskInfoCodec, "taskInfoCodec is null");
        requireNonNull(taskUpdateRequestCodec, "taskUpdateRequestCodec is null");
        requireNonNull(planFragmentCodec, "planFragmentCodec is null");
        requireNonNull(partitionedSplitCountTracker, "partitionedSplitCountTracker is null");
        requireNonNull(maxErrorDuration, "maxErrorDuration is null");
        requireNonNull(stats, "stats is null");
        requireNonNull(taskInfoRefreshMaxWait, "taskInfoRefreshMaxWait is null");
        requireNonNull(tableWriteInfo, "tableWriteInfo is null");
        requireNonNull(metadataManager, "metadataManager is null");
        requireNonNull(queryManager, "queryManager is null");
        requireNonNull(thriftProtocol, "thriftProtocol is null");

        try (SetThreadName ignored = new SetThreadName("HttpRemoteTask-%s", taskId)) {
            this.taskId = taskId;
            this.taskLocation = location;
            this.remoteTaskLocation = remoteLocation;
            this.session = session;
            this.nodeId = nodeId;
            this.planFragment = planFragment;
            this.outputBuffers.set(outputBuffers);
            this.httpClient = httpClient;
            this.executor = executor;
            this.errorScheduledExecutor = errorScheduledExecutor;
            this.summarizeTaskInfo = summarizeTaskInfo;
            this.taskInfoCodec = taskInfoCodec;
            this.taskUpdateRequestCodec = taskUpdateRequestCodec;
            this.planFragmentCodec = planFragmentCodec;
            this.updateErrorTracker = taskRequestErrorTracker(taskId, location, maxErrorDuration, errorScheduledExecutor, "updating task");
            this.partitionedSplitCountTracker = requireNonNull(partitionedSplitCountTracker, "partitionedSplitCountTracker is null");
            this.maxErrorDuration = maxErrorDuration;
            this.stats = stats;
            this.binaryTransportEnabled = binaryTransportEnabled;
            this.thriftTransportEnabled = thriftTransportEnabled;
            this.thriftProtocol = thriftProtocol;
            this.tableWriteInfo = tableWriteInfo;
            this.maxTaskUpdateSizeInBytes = maxTaskUpdateSizeInBytes;

            this.tableScanPlanNodeIds = ImmutableSet.copyOf(planFragment.getTableScanSchedulingOrder());
            this.remoteSourcePlanNodeIds = planFragment.getRemoteSourceNodes().stream()
                    .map(PlanNode::getId)
                    .collect(toImmutableSet());

            for (Entry<PlanNodeId, Split> entry : requireNonNull(initialSplits, "initialSplits is null").entries()) {
                ScheduledSplit scheduledSplit = new ScheduledSplit(nextSplitId.getAndIncrement(), entry.getKey(), entry.getValue());
                pendingSplits.put(entry.getKey(), scheduledSplit);
            }
            pendingSourceSplitCount = planFragment.getTableScanSchedulingOrder().stream()
                    .filter(initialSplits::containsKey)
                    .mapToInt(partitionedSource -> initialSplits.get(partitionedSource).size())
                    .sum();

            List<BufferInfo> bufferStates = outputBuffers.getBuffers()
                    .keySet().stream()
                    .map(outputId -> new BufferInfo(outputId, false, 0, 0, PageBufferInfo.empty()))
                    .collect(toImmutableList());

            TaskInfo initialTask = createInitialTask(taskId, location, bufferStates, new TaskStats(DateTime.now(), null));

            this.taskStatusFetcher = new ContinuousTaskStatusFetcher(
                    this::failTask,
                    taskId,
                    initialTask.getTaskStatus(),
                    taskStatusRefreshMaxWait,
                    taskStatusCodec,
                    executor,
                    httpClient,
                    maxErrorDuration,
                    errorScheduledExecutor,
                    stats,
                    binaryTransportEnabled,
                    thriftTransportEnabled,
                    thriftProtocol);

            this.taskInfoFetcher = new TaskInfoFetcher(
                    this::failTask,
                    initialTask,
                    httpClient,
                    taskInfoUpdateInterval,
                    taskInfoRefreshMaxWait,
                    taskInfoCodec,
                    metadataUpdatesCodec,
                    maxErrorDuration,
                    summarizeTaskInfo,
                    executor,
                    updateScheduledExecutor,
                    errorScheduledExecutor,
                    stats,
                    binaryTransportEnabled,
                    session,
                    metadataManager,
                    queryManager);

            taskStatusFetcher.addStateChangeListener(newStatus -> {
                TaskState state = newStatus.getState();
                if (state.isDone()) {
                    cleanUpTask();
                }
                else {
                    partitionedSplitCountTracker.setPartitionedSplitCount(getPartitionedSplitCount());
                    updateSplitQueueSpace();
                }
            });

            partitionedSplitCountTracker.setPartitionedSplitCount(getPartitionedSplitCount());
            updateSplitQueueSpace();
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
    public TaskInfo getTaskInfo()
    {
        return taskInfoFetcher.getTaskInfo();
    }

    @Override
    public TaskStatus getTaskStatus()
    {
        return taskStatusFetcher.getTaskStatus();
    }

    @Override
    public URI getRemoteTaskLocation()
    {
        return remoteTaskLocation;
    }

    @Override
    public void start()
    {
        try (SetThreadName ignored = new SetThreadName("HttpRemoteTask-%s", taskId)) {
            // to start we just need to trigger an update
            scheduleUpdate();

            taskStatusFetcher.start();
            taskInfoFetcher.start();
        }
    }

    @Override
    public synchronized void addSplits(Multimap<PlanNodeId, Split> splitsBySource)
    {
        requireNonNull(splitsBySource, "splitsBySource is null");

        // only add pending split if not done
        if (getTaskStatus().getState().isDone()) {
            return;
        }

        boolean needsUpdate = false;
        for (Entry<PlanNodeId, Collection<Split>> entry : splitsBySource.asMap().entrySet()) {
            PlanNodeId sourceId = entry.getKey();
            Collection<Split> splits = entry.getValue();

            checkState(!noMoreSplits.containsKey(sourceId), "noMoreSplits has already been set for %s", sourceId);
            int added = 0;
            for (Split split : splits) {
                if (pendingSplits.put(sourceId, new ScheduledSplit(nextSplitId.getAndIncrement(), sourceId, split))) {
                    added++;
                }
            }
            if (tableScanPlanNodeIds.contains(sourceId)) {
                pendingSourceSplitCount += added;
                partitionedSplitCountTracker.setPartitionedSplitCount(getPartitionedSplitCount());
            }
            needsUpdate = true;
        }
        updateSplitQueueSpace();

        if (needsUpdate) {
            this.needsUpdate.set(true);
            scheduleUpdate();
        }
    }

    @Override
    public synchronized void noMoreSplits(PlanNodeId sourceId)
    {
        if (noMoreSplits.containsKey(sourceId)) {
            return;
        }

        noMoreSplits.put(sourceId, true);
        needsUpdate.set(true);
        scheduleUpdate();
    }

    @Override
    public synchronized void noMoreSplits(PlanNodeId sourceId, Lifespan lifespan)
    {
        if (pendingNoMoreSplitsForLifespan.put(sourceId, lifespan)) {
            needsUpdate.set(true);
            scheduleUpdate();
        }
    }

    @Override
    public synchronized void setOutputBuffers(OutputBuffers newOutputBuffers)
    {
        if (getTaskStatus().getState().isDone()) {
            return;
        }

        if (newOutputBuffers.getVersion() > outputBuffers.get().getVersion()) {
            outputBuffers.set(newOutputBuffers);
            needsUpdate.set(true);
            scheduleUpdate();
        }
    }

    @Override
    public ListenableFuture<?> removeRemoteSource(TaskId remoteSourceTaskId)
    {
        URI remoteSourceUri = uriBuilderFrom(taskLocation)
                .appendPath("remote-source")
                .appendPath(remoteSourceTaskId.toString())
                .build();

        Request request = prepareDelete()
                .setUri(remoteSourceUri)
                .build();
        RequestErrorTracker errorTracker = taskRequestErrorTracker(
                taskId,
                remoteSourceUri,
                maxErrorDuration,
                errorScheduledExecutor,
                "Remove exchange remote source");

        SettableFuture<?> future = SettableFuture.create();
        doRemoveRemoteSource(errorTracker, request, future);
        return future;
    }

    /// This method may call itself recursively when retrying for failures
    private void doRemoveRemoteSource(RequestErrorTracker errorTracker, Request request, SettableFuture<?> future)
    {
        errorTracker.startRequest();

        FutureCallback<StatusResponse> callback = new FutureCallback<StatusResponse>()
        {
            @Override
            public void onSuccess(@Nullable StatusResponse response)
            {
                if (response == null) {
                    throw new PrestoException(GENERIC_INTERNAL_ERROR, "Request failed with null response");
                }
                if (response.getStatusCode() != OK.code() && response.getStatusCode() != NO_CONTENT.code()) {
                    throw new PrestoException(GENERIC_INTERNAL_ERROR, "Request failed with HTTP status " + response.getStatusCode());
                }
                future.set(null);
            }

            @Override
            public void onFailure(Throwable failedReason)
            {
                if (failedReason instanceof RejectedExecutionException && httpClient.isClosed()) {
                    log.error("Unable to destroy exchange source at %s. HTTP client is closed", request.getUri());
                    future.setException(failedReason);
                    return;
                }
                // record failure
                try {
                    errorTracker.requestFailed(failedReason);
                }
                catch (PrestoException e) {
                    future.setException(e);
                    return;
                }
                // if throttled due to error, asynchronously wait for timeout and try again
                ListenableFuture<?> errorRateLimit = errorTracker.acquireRequestPermit();
                if (errorRateLimit.isDone()) {
                    doRemoveRemoteSource(errorTracker, request, future);
                }
                else {
                    errorRateLimit.addListener(() -> doRemoveRemoteSource(errorTracker, request, future), errorScheduledExecutor);
                }
            }
        };

        addCallback(httpClient.executeAsync(request, createStatusResponseHandler()), callback, directExecutor());
    }

    @Override
    public int getPartitionedSplitCount()
    {
        TaskStatus taskStatus = getTaskStatus();
        if (taskStatus.getState().isDone()) {
            return 0;
        }
        return getPendingSourceSplitCount() + taskStatus.getQueuedPartitionedDrivers() + taskStatus.getRunningPartitionedDrivers();
    }

    @Override
    public int getQueuedPartitionedSplitCount()
    {
        TaskStatus taskStatus = getTaskStatus();
        if (taskStatus.getState().isDone()) {
            return 0;
        }
        return getPendingSourceSplitCount() + taskStatus.getQueuedPartitionedDrivers();
    }

    @SuppressWarnings("FieldAccessNotGuarded")
    private int getPendingSourceSplitCount()
    {
        return pendingSourceSplitCount;
    }

    @Override
    public void addStateChangeListener(StateChangeListener<TaskStatus> stateChangeListener)
    {
        try (SetThreadName ignored = new SetThreadName("HttpRemoteTask-%s", taskId)) {
            taskStatusFetcher.addStateChangeListener(stateChangeListener);
        }
    }

    @Override
    public void addFinalTaskInfoListener(StateChangeListener<TaskInfo> stateChangeListener)
    {
        taskInfoFetcher.addFinalTaskInfoListener(stateChangeListener);
    }

    @Override
    public synchronized ListenableFuture<?> whenSplitQueueHasSpace(int threshold)
    {
        if (whenSplitQueueHasSpaceThreshold.isPresent()) {
            checkArgument(threshold == whenSplitQueueHasSpaceThreshold.getAsInt(), "Multiple split queue space notification thresholds not supported");
        }
        else {
            whenSplitQueueHasSpaceThreshold = OptionalInt.of(threshold);
            updateSplitQueueSpace();
        }
        if (splitQueueHasSpace) {
            return immediateFuture(null);
        }
        return whenSplitQueueHasSpace.createNewListener();
    }

    private synchronized void updateSplitQueueSpace()
    {
        if (!whenSplitQueueHasSpaceThreshold.isPresent()) {
            return;
        }
        splitQueueHasSpace = getQueuedPartitionedSplitCount() < whenSplitQueueHasSpaceThreshold.getAsInt();
        if (splitQueueHasSpace) {
            whenSplitQueueHasSpace.complete(null, executor);
        }
    }

    private synchronized void processTaskUpdate(TaskInfo newValue, List<TaskSource> sources)
    {
        updateTaskInfo(newValue);

        // remove acknowledged splits, which frees memory
        for (TaskSource source : sources) {
            PlanNodeId planNodeId = source.getPlanNodeId();
            int removed = 0;
            for (ScheduledSplit split : source.getSplits()) {
                if (pendingSplits.remove(planNodeId, split)) {
                    removed++;
                }
            }
            if (source.isNoMoreSplits()) {
                noMoreSplits.put(planNodeId, false);
            }
            for (Lifespan lifespan : source.getNoMoreSplitsForLifespan()) {
                pendingNoMoreSplitsForLifespan.remove(planNodeId, lifespan);
            }
            if (tableScanPlanNodeIds.contains(planNodeId)) {
                pendingSourceSplitCount -= removed;
            }
        }
        updateSplitQueueSpace();

        partitionedSplitCountTracker.setPartitionedSplitCount(getPartitionedSplitCount());
    }

    private void updateTaskInfo(TaskInfo taskInfo)
    {
        taskStatusFetcher.updateTaskStatus(taskInfo.getTaskStatus());
        taskInfoFetcher.updateTaskInfo(taskInfo);
    }

    private void scheduleUpdate()
    {
        executor.execute(this::sendUpdate);
    }

    private synchronized void sendUpdate()
    {
        TaskStatus taskStatus = getTaskStatus();
        // don't update if the task hasn't been started yet or if it is already finished
        if (!needsUpdate.get() || taskStatus.getState().isDone()) {
            return;
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

        Optional<byte[]> fragment = sendPlan.get() ? Optional.of(planFragment.toBytes(planFragmentCodec)) : Optional.empty();
        Optional<TableWriteInfo> writeInfo = sendPlan.get() ? Optional.of(tableWriteInfo) : Optional.empty();
        TaskUpdateRequest updateRequest = new TaskUpdateRequest(
                session.toSessionRepresentation(),
                session.getIdentity().getExtraCredentials(),
                fragment,
                sources,
                outputBuffers.get(),
                writeInfo);
        byte[] taskUpdateRequestJson = taskUpdateRequestCodec.toBytes(updateRequest);

        if (taskUpdateRequestJson.length > maxTaskUpdateSizeInBytes) {
            failTask(new PrestoException(EXCEEDED_TASK_UPDATE_SIZE_LIMIT, format("TaskUpdate size of %d Bytes has exceeded the limit of %d Bytes", taskUpdateRequestJson.length, maxTaskUpdateSizeInBytes)));
        }

        if (fragment.isPresent()) {
            stats.updateWithPlanSize(taskUpdateRequestJson.length);
        }
        else {
            if (ThreadLocalRandom.current().nextDouble() < UPDATE_WITHOUT_PLAN_STATS_SAMPLE_RATE) {
                // This is to keep track of the task update size even when the plan fragment is NOT present
                stats.updateWithoutPlanSize(taskUpdateRequestJson.length);
            }
        }

        HttpUriBuilder uriBuilder = getHttpUriBuilder(taskStatus);
        Request request = setContentTypeHeaders(binaryTransportEnabled, preparePost())
                .setUri(uriBuilder.build())
                .setBodyGenerator(createStaticBodyGenerator(taskUpdateRequestJson))
                .build();

        ResponseHandler responseHandler;
        if (binaryTransportEnabled) {
            responseHandler = createFullSmileResponseHandler((SmileCodec<TaskInfo>) taskInfoCodec);
        }
        else {
            responseHandler = createAdaptingJsonResponseHandler(unwrapJsonCodec(taskInfoCodec));
        }

        updateErrorTracker.startRequest();

        ListenableFuture<BaseResponse<TaskInfo>> future = httpClient.executeAsync(request, responseHandler);
        currentRequest = future;
        currentRequestStartNanos = System.nanoTime();

        // The needsUpdate flag needs to be set to false BEFORE adding the Future callback since callback might change the flag value
        // and does so without grabbing the instance lock.
        needsUpdate.set(false);

        Futures.addCallback(
                future,
                new SimpleHttpResponseHandler<>(new UpdateResponseHandler(sources), request.getUri(), stats.getHttpResponseStats(), REMOTE_TASK_ERROR),
                executor);
    }

    private synchronized List<TaskSource> getSources()
    {
        return Stream.concat(tableScanPlanNodeIds.stream(), remoteSourcePlanNodeIds.stream())
                .map(this::getSource)
                .filter(Objects::nonNull)
                .collect(toImmutableList());
    }

    private synchronized TaskSource getSource(PlanNodeId planNodeId)
    {
        Set<ScheduledSplit> splits = pendingSplits.get(planNodeId);
        boolean pendingNoMoreSplits = Boolean.TRUE.equals(this.noMoreSplits.get(planNodeId));
        boolean noMoreSplits = this.noMoreSplits.containsKey(planNodeId);
        Set<Lifespan> noMoreSplitsForLifespan = pendingNoMoreSplitsForLifespan.get(planNodeId);

        TaskSource element = null;
        if (!splits.isEmpty() || !noMoreSplitsForLifespan.isEmpty() || pendingNoMoreSplits) {
            element = new TaskSource(planNodeId, splits, noMoreSplitsForLifespan, noMoreSplits);
        }
        return element;
    }

    @Override
    public synchronized void cancel()
    {
        try (SetThreadName ignored = new SetThreadName("HttpRemoteTask-%s", taskId)) {
            TaskStatus taskStatus = getTaskStatus();
            if (taskStatus.getState().isDone()) {
                return;
            }

            // send cancel to task and ignore response
            HttpUriBuilder uriBuilder = getHttpUriBuilder(taskStatus).addParameter("abort", "false");
            Request request = setContentTypeHeaders(binaryTransportEnabled, prepareDelete())
                    .setUri(uriBuilder.build())
                    .build();
            scheduleAsyncCleanupRequest(createCleanupBackoff(), request, "cancel");
        }
    }

    private synchronized void cleanUpTask()
    {
        checkState(getTaskStatus().getState().isDone(), "attempt to clean up a task that is not done yet");

        // clear pending splits to free memory
        pendingSplits.clear();
        pendingSourceSplitCount = 0;
        partitionedSplitCountTracker.setPartitionedSplitCount(getPartitionedSplitCount());
        splitQueueHasSpace = true;
        whenSplitQueueHasSpace.complete(null, executor);

        // cancel pending request
        if (currentRequest != null) {
            // do not terminate if the request is already running to avoid closing pooled connections
            currentRequest.cancel(false);
            currentRequest = null;
            currentRequestStartNanos = 0;
        }

        taskStatusFetcher.stop();

        // The remote task is likely to get a delete from the PageBufferClient first.
        // We send an additional delete anyway to get the final TaskInfo
        HttpUriBuilder uriBuilder = getHttpUriBuilder(getTaskStatus());
        Request request = setContentTypeHeaders(binaryTransportEnabled, prepareDelete())
                .setUri(uriBuilder.build())
                .build();

        scheduleAsyncCleanupRequest(createCleanupBackoff(), request, "cleanup");
    }

    @Override
    public synchronized void abort()
    {
        if (getTaskStatus().getState().isDone()) {
            return;
        }

        abort(failWith(getTaskStatus(), ABORTED, ImmutableList.of()));
    }

    private synchronized void abort(TaskStatus status)
    {
        checkState(status.getState().isDone(), "cannot abort task with an incomplete status");

        try (SetThreadName ignored = new SetThreadName("HttpRemoteTask-%s", taskId)) {
            taskStatusFetcher.updateTaskStatus(status);

            // send abort to task
            HttpUriBuilder uriBuilder = getHttpUriBuilder(getTaskStatus());
            Request request = setContentTypeHeaders(binaryTransportEnabled, prepareDelete())
                    .setUri(uriBuilder.build())
                    .build();
            scheduleAsyncCleanupRequest(createCleanupBackoff(), request, "abort");
        }
    }

    private void scheduleAsyncCleanupRequest(Backoff cleanupBackoff, Request request, String action)
    {
        if (!aborting.compareAndSet(false, true)) {
            // Do not initiate another round of cleanup requests if one had been initiated.
            // Otherwise, we can get into an asynchronous recursion here. For example, when aborting a task after REMOTE_TASK_MISMATCH.
            return;
        }
        doScheduleAsyncCleanupRequest(cleanupBackoff, request, action);
    }

    private void doScheduleAsyncCleanupRequest(Backoff cleanupBackoff, Request request, String action)
    {
        ResponseHandler responseHandler;
        if (binaryTransportEnabled) {
            responseHandler = createFullSmileResponseHandler((SmileCodec<TaskInfo>) taskInfoCodec);
        }
        else {
            responseHandler = createAdaptingJsonResponseHandler(unwrapJsonCodec(taskInfoCodec));
        }

        Futures.addCallback(httpClient.executeAsync(request, responseHandler), new FutureCallback<BaseResponse<TaskInfo>>()
        {
            @Override
            public void onSuccess(BaseResponse<TaskInfo> result)
            {
                try {
                    updateTaskInfo(result.getValue());
                }
                finally {
                    if (!getTaskInfo().getTaskStatus().getState().isDone()) {
                        cleanUpLocally();
                    }
                }
            }

            @Override
            public void onFailure(Throwable t)
            {
                if (t instanceof RejectedExecutionException && httpClient.isClosed()) {
                    logError(t, "Unable to %s task at %s. HTTP client is closed.", action, request.getUri());
                    cleanUpLocally();
                    return;
                }

                // record failure
                if (cleanupBackoff.failure()) {
                    logError(t, "Unable to %s task at %s. Back off depleted.", action, request.getUri());
                    cleanUpLocally();
                    return;
                }

                // reschedule
                long delayNanos = cleanupBackoff.getBackoffDelayNanos();
                if (delayNanos == 0) {
                    doScheduleAsyncCleanupRequest(cleanupBackoff, request, action);
                }
                else {
                    errorScheduledExecutor.schedule(() -> doScheduleAsyncCleanupRequest(cleanupBackoff, request, action), delayNanos, NANOSECONDS);
                }
            }

            private void cleanUpLocally()
            {
                // Update the taskInfo with the new taskStatus.

                // Generally, we send a cleanup request to the worker, and update the TaskInfo on
                // the coordinator based on what we fetched from the worker. If we somehow cannot
                // get the cleanup request to the worker, the TaskInfo that we fetch for the worker
                // likely will not say the task is done however many times we try. In this case,
                // we have to set the local query info directly so that we stop trying to fetch
                // updated TaskInfo from the worker. This way, the task on the worker eventually
                // expires due to lack of activity.

                // This is required because the query state machine depends on TaskInfo (instead of task status)
                // to transition its own state.
                // TODO: Update the query state machine and stage state machine to depend on TaskStatus instead

                // Since this TaskInfo is updated in the client the "complete" flag will not be set,
                // indicating that the stats may not reflect the final stats on the worker.
                updateTaskInfo(getTaskInfo().withTaskStatus(getTaskStatus()));
            }
        }, executor);
    }

    /**
     * Move the task directly to the failed state if there was a failure in this task
     */
    private void failTask(Throwable cause)
    {
        TaskStatus taskStatus = getTaskStatus();
        if (!taskStatus.getState().isDone()) {
            log.debug(cause, "Remote task %s failed with %s", taskStatus.getSelf(), cause);
        }

        TaskStatus failedTaskStatus = failWith(getTaskStatus(), FAILED, ImmutableList.of(toFailure(cause)));
        // Transition task to failed state without waiting for the final task info returned by the abort request.
        // The abort request is very likely not to succeed, leaving the task and the stage in the limbo state for
        // the entire duration of abort retries. If the task is failed, it is not that important to actually
        // record the final statistics and the final information about a failed task.
        taskInfoFetcher.updateTaskInfo(getTaskInfo().withTaskStatus(failedTaskStatus));

        // Initiate abort request
        abort(failedTaskStatus);
    }

    private HttpUriBuilder getHttpUriBuilder(TaskStatus taskStatus)
    {
        HttpUriBuilder uriBuilder = uriBuilderFrom(taskStatus.getSelf());
        if (summarizeTaskInfo) {
            uriBuilder.addParameter("summarize");
        }
        return uriBuilder;
    }

    private static Backoff createCleanupBackoff()
    {
        return new Backoff(10, new Duration(10, TimeUnit.MINUTES), Ticker.systemTicker(), ImmutableList.<Duration>builder()
                .add(new Duration(0, MILLISECONDS))
                .add(new Duration(100, MILLISECONDS))
                .add(new Duration(500, MILLISECONDS))
                .add(new Duration(1, SECONDS))
                .add(new Duration(10, SECONDS))
                .build());
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
                    long currentRequestStartNanos;
                    synchronized (HttpRemoteTask.this) {
                        currentRequest = null;
                        sendPlan.set(value.isNeedsPlan());
                        currentRequestStartNanos = HttpRemoteTask.this.currentRequestStartNanos;
                    }
                    updateStats(currentRequestStartNanos);
                    processTaskUpdate(value, sources);
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
                    long currentRequestStartNanos;
                    synchronized (HttpRemoteTask.this) {
                        currentRequest = null;
                        currentRequestStartNanos = HttpRemoteTask.this.currentRequestStartNanos;
                    }
                    updateStats(currentRequestStartNanos);

                    // on failure assume we need to update again
                    needsUpdate.set(true);

                    // if task not already done, record error
                    TaskStatus taskStatus = getTaskStatus();
                    if (!taskStatus.getState().isDone()) {
                        updateErrorTracker.requestFailed(cause);
                    }
                }
                catch (Error e) {
                    failTask(e);
                    throw e;
                }
                catch (RuntimeException e) {
                    failTask(e);
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

        private void updateStats(long currentRequestStartNanos)
        {
            Duration requestRoundTrip = Duration.nanosSince(currentRequestStartNanos);
            stats.updateRoundTripMillis(requestRoundTrip.toMillis());
        }
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
}
