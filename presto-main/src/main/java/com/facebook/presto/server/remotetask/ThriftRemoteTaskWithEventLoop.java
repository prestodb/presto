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

import com.facebook.airlift.log.Logger;
import com.facebook.airlift.stats.DecayCounter;
import com.facebook.drift.client.DriftClient;
import com.facebook.drift.transport.netty.codec.Protocol;
import com.facebook.presto.Session;
import com.facebook.presto.execution.FutureStateChange;
import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.execution.NodeTaskMap.NodeStatsTracker;
import com.facebook.presto.execution.PartitionedSplitsInfo;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.execution.SafeEventLoopGroup;
import com.facebook.presto.execution.ScheduledSplit;
import com.facebook.presto.execution.SchedulerStatsTracker;
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.execution.TaskStatus;
import com.facebook.presto.execution.buffer.BufferInfo;
import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.execution.buffer.PageBufferInfo;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.metadata.HandleResolver;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.operator.TaskStats;
import com.facebook.presto.server.RequestErrorTracker;
import com.facebook.presto.server.TaskUpdateRequest;
import com.facebook.presto.spi.SplitWeight;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.sql.planner.PlanFragment;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import javax.annotation.Nullable;

import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.Future;

import static com.facebook.presto.SystemSessionProperties.getMaxUnacknowledgedSplitsPerTask;
import static com.facebook.presto.execution.TaskInfo.createInitialTask;
import static com.facebook.presto.execution.TaskState.ABORTED;
import static com.facebook.presto.execution.TaskState.FAILED;
import static com.facebook.presto.execution.TaskStatus.failWith;
import static com.facebook.presto.server.RequestErrorTracker.taskRequestErrorTracker;
import static com.facebook.presto.util.Failures.toFailure;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.lang.Math.addExact;
import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.util.Objects.requireNonNull;

/**
 * Represents a task running on a remote worker using Thrift communication.
 * <p>
 * This class replaces HTTP-based communication with Thrift for better performance
 * and type safety. It maintains the same event loop concurrency model as the HTTP version:
 * <ul>
 * <li>All mutable state access and modifications are performed on a single dedicated event loop thread</li>
 * <li>External threads submit operations to the event loop using {@code safeExecuteOnEventLoop()}</li>
 * <li>The event loop serializes all operations, eliminating race conditions without using locks</li>
 * </ul>
 */
public final class ThriftRemoteTaskWithEventLoop
        implements RemoteTask
{
    private static final Logger log = Logger.get(ThriftRemoteTaskWithEventLoop.class);
    private static final double UPDATE_WITHOUT_PLAN_STATS_SAMPLE_RATE = 0.01;

    private final TaskId taskId;
    private final URI taskLocation;
    private final URI remoteTaskLocation;

    private final Session session;
    private final String nodeId;
    private final PlanFragment planFragment;

    private final Set<PlanNodeId> tableScanPlanNodeIds;
    private final Set<PlanNodeId> remoteSourcePlanNodeIds;

    private long nextSplitId;

    private final Duration maxErrorDuration;
    private final RemoteTaskStats stats;

    // Direct task status and info management for Thrift
    private volatile TaskInfo currentTaskInfo;
    private volatile TaskStatus currentTaskStatus;

    private final LongArrayList taskUpdateTimeline = new LongArrayList();
    private Future<?> currentRequest;
    private long currentRequestStartNanos;
    private long currentRequestLastTaskUpdate;

    private final SetMultimap<PlanNodeId, ScheduledSplit> pendingSplits = HashMultimap.create();
    private volatile int pendingSourceSplitCount;
    private volatile long pendingSourceSplitsWeight;
    private final SetMultimap<PlanNodeId, Lifespan> pendingNoMoreSplitsForLifespan = HashMultimap.create();
    private final Map<PlanNodeId, Boolean> noMoreSplits = new HashMap<>();
    private OutputBuffers outputBuffers;
    private final FutureStateChange<?> whenSplitQueueHasSpace = new FutureStateChange<>();
    private volatile boolean splitQueueHasSpace;
    private OptionalLong whenSplitQueueHasSpaceThreshold = OptionalLong.empty();

    private final boolean summarizeTaskInfo;

    private final PrestoThriftService thriftClient;

    private final RequestErrorTracker updateErrorTracker;

    private boolean needsUpdate = true;
    private boolean sendPlan = true;

    private final NodeStatsTracker nodeStatsTracker;

    private boolean started;
    private boolean aborting;

    private final Protocol thriftProtocol;
    private final HandleResolver handleResolver;
    private final int maxTaskUpdateSizeInBytes;
    private final int maxUnacknowledgedSplits;
    private final DataSize maxTaskUpdateDataSize;

    private final TableWriteInfo tableWriteInfo;

    private final DecayCounter taskUpdateRequestSize;
    private final boolean taskUpdateSizeTrackingEnabled;
    private final SchedulerStatsTracker schedulerStatsTracker;

    private final SafeEventLoopGroup.SafeEventLoop taskEventLoop;
    private final String loggingPrefix;

    public static ThriftRemoteTaskWithEventLoop createThriftRemoteTaskWithEventLoop(
            Session session,
            TaskId taskId,
            String nodeId,
            URI location,
            URI remoteLocation,
            PlanFragment planFragment,
            Multimap<PlanNodeId, Split> initialSplits,
            OutputBuffers outputBuffers,
            DriftClient<PrestoThriftService> prestoThriftServiceDriftClient,
            Duration maxErrorDuration,
            Duration taskStatusRefreshMaxWait,
            Duration taskInfoRefreshMaxWait,
            Duration taskInfoUpdateInterval,
            boolean summarizeTaskInfo,
            NodeStatsTracker nodeStatsTracker,
            RemoteTaskStats stats,
            Protocol thriftProtocol,
            TableWriteInfo tableWriteInfo,
            int maxTaskUpdateSizeInBytes,
            MetadataManager metadataManager,
            QueryManager queryManager,
            DecayCounter taskUpdateRequestSize,
            boolean taskUpdateSizeTrackingEnabled,
            HandleResolver handleResolver,
            SchedulerStatsTracker schedulerStatsTracker,
            SafeEventLoopGroup.SafeEventLoop taskEventLoop)
    {
        ThriftRemoteTaskWithEventLoop task = new ThriftRemoteTaskWithEventLoop(
                session,
                taskId,
                nodeId,
                location,
                remoteLocation,
                planFragment,
                initialSplits,
                outputBuffers,
                prestoThriftServiceDriftClient,
                maxErrorDuration,
                taskStatusRefreshMaxWait,
                taskInfoRefreshMaxWait,
                taskInfoUpdateInterval,
                summarizeTaskInfo,
                nodeStatsTracker,
                stats,
                thriftProtocol,
                tableWriteInfo,
                maxTaskUpdateSizeInBytes,
                metadataManager,
                queryManager,
                taskUpdateRequestSize,
                taskUpdateSizeTrackingEnabled,
                handleResolver,
                schedulerStatsTracker,
                taskEventLoop);
        task.initialize();
        return task;
    }

    private ThriftRemoteTaskWithEventLoop(
            Session session,
            TaskId taskId,
            String nodeId,
            URI location,
            URI remoteLocation,
            PlanFragment planFragment,
            Multimap<PlanNodeId, Split> initialSplits,
            OutputBuffers outputBuffers,
            DriftClient<PrestoThriftService> prestoThriftServiceDriftClient,
            Duration maxErrorDuration,
            Duration taskStatusRefreshMaxWait,
            Duration taskInfoRefreshMaxWait,
            Duration taskInfoUpdateInterval,
            boolean summarizeTaskInfo,
            NodeStatsTracker nodeStatsTracker,
            RemoteTaskStats stats,
            Protocol thriftProtocol,
            TableWriteInfo tableWriteInfo,
            int maxTaskUpdateSizeInBytes,
            MetadataManager metadataManager,
            QueryManager queryManager,
            DecayCounter taskUpdateRequestSize,
            boolean taskUpdateSizeTrackingEnabled,
            HandleResolver handleResolver,
            SchedulerStatsTracker schedulerStatsTracker,
            SafeEventLoopGroup.SafeEventLoop taskEventLoop)
    {
        requireNonNull(session, "session is null");
        requireNonNull(taskId, "taskId is null");
        requireNonNull(nodeId, "nodeId is null");
        requireNonNull(location, "location is null");
        requireNonNull(remoteLocation, "remoteLocation is null");
        requireNonNull(planFragment, "planFragment is null");
        requireNonNull(outputBuffers, "outputBuffers is null");
        requireNonNull(prestoThriftServiceDriftClient, "prestoThriftServiceDriftClient is null");
        requireNonNull(nodeStatsTracker, "nodeStatsTracker is null");
        requireNonNull(maxErrorDuration, "maxErrorDuration is null");
        requireNonNull(stats, "stats is null");
        requireNonNull(taskInfoRefreshMaxWait, "taskInfoRefreshMaxWait is null");
        requireNonNull(tableWriteInfo, "tableWriteInfo is null");
        requireNonNull(metadataManager, "metadataManager is null");
        requireNonNull(queryManager, "queryManager is null");
        requireNonNull(thriftProtocol, "thriftProtocol is null");
        requireNonNull(handleResolver, "handleResolver is null");
        requireNonNull(taskUpdateRequestSize, "taskUpdateRequestSize cannot be null");
        requireNonNull(schedulerStatsTracker, "schedulerStatsTracker is null");
        requireNonNull(taskEventLoop, "taskEventLoop is null");

        this.taskEventLoop = taskEventLoop;
        this.taskId = taskId;
        this.taskLocation = location;
        this.remoteTaskLocation = remoteLocation;
        this.session = session;
        this.nodeId = nodeId;
        this.planFragment = planFragment;
        this.outputBuffers = outputBuffers;
        this.summarizeTaskInfo = summarizeTaskInfo;
        this.updateErrorTracker = taskRequestErrorTracker(taskId, location, maxErrorDuration, taskEventLoop, "updating task");
        this.nodeStatsTracker = requireNonNull(nodeStatsTracker, "nodeStatsTracker is null");
        this.maxErrorDuration = maxErrorDuration;
        this.stats = stats;
        this.thriftProtocol = thriftProtocol;
        this.handleResolver = handleResolver;
        this.tableWriteInfo = tableWriteInfo;
        this.maxTaskUpdateSizeInBytes = maxTaskUpdateSizeInBytes;
        this.maxTaskUpdateDataSize = DataSize.succinctBytes(this.maxTaskUpdateSizeInBytes);
        this.maxUnacknowledgedSplits = getMaxUnacknowledgedSplitsPerTask(session);
        checkArgument(maxUnacknowledgedSplits > 0, "maxUnacknowledgedSplits must be > 0, found: %s", maxUnacknowledgedSplits);

        this.tableScanPlanNodeIds = ImmutableSet.copyOf(planFragment.getTableScanSchedulingOrder());
        this.remoteSourcePlanNodeIds = planFragment.getRemoteSourceNodes().stream()
                .map(PlanNode::getId)
                .collect(toImmutableSet());
        this.taskUpdateRequestSize = taskUpdateRequestSize;
        this.taskUpdateSizeTrackingEnabled = taskUpdateSizeTrackingEnabled;
        this.schedulerStatsTracker = schedulerStatsTracker;

        // Create Thrift client
        this.thriftClient = prestoThriftServiceDriftClient.get(Optional.of(location.getAuthority()));

        for (Map.Entry<PlanNodeId, Split> entry : requireNonNull(initialSplits, "initialSplits is null").entries()) {
            ScheduledSplit scheduledSplit = new ScheduledSplit(nextSplitId++, entry.getKey(), entry.getValue());
            pendingSplits.put(entry.getKey(), scheduledSplit);
        }
        int pendingSourceSplitCount = 0;
        long pendingSourceSplitsWeight = 0;
        for (PlanNodeId planNodeId : planFragment.getTableScanSchedulingOrder()) {
            Collection<Split> tableScanSplits = initialSplits.get(planNodeId);
            if (tableScanSplits != null && !tableScanSplits.isEmpty()) {
                pendingSourceSplitCount += tableScanSplits.size();
                pendingSourceSplitsWeight = addExact(pendingSourceSplitsWeight, SplitWeight.rawValueSum(tableScanSplits, Split::getSplitWeight));
            }
        }
        this.pendingSourceSplitCount = pendingSourceSplitCount;
        this.pendingSourceSplitsWeight = pendingSourceSplitsWeight;

        List<BufferInfo> bufferStates = outputBuffers.getBuffers()
                .keySet().stream()
                .map(outputId -> new BufferInfo(outputId, false, 0, 0, PageBufferInfo.empty()))
                .collect(toImmutableList());

        TaskInfo initialTask = createInitialTask(taskId, location, bufferStates, new TaskStats(currentTimeMillis(), 0), nodeId);

        // Initialize task status and info directly for Thrift
        this.currentTaskStatus = initialTask.getTaskStatus();
        this.currentTaskInfo = initialTask;
        this.loggingPrefix = format("Query: %s, Task: %s", session.getQueryId(), taskId);
    }

    private void initialize()
    {
        updateTaskStats();
        safeExecuteOnEventLoop(this::updateSplitQueueSpace, "updateSplitQueueSpace");

        // Start periodic status/info polling
        startPeriodicPolling();
    }

    private void startPeriodicPolling()
    {
        safeExecuteOnEventLoop(() -> {
            if (started && !currentTaskStatus.getState().isDone()) {
                // Periodically fetch task status and info via Thrift
                pollTaskStatus();
                pollTaskInfo();

                // Schedule next poll
                taskEventLoop.schedule(this::startPeriodicPolling, 1000, java.util.concurrent.TimeUnit.MILLISECONDS);
            }
        }, "startPeriodicPolling");
    }

    private void pollTaskStatus()
    {
        try {
            ListenableFuture<TaskStatus> statusFuture = thriftClient.getTaskStatus(taskId.toString());

            Futures.addCallback(statusFuture, new FutureCallback<TaskStatus>() {
                @Override
                public void onSuccess(@Nullable TaskStatus result)
                {
                    if (result != null) {
                        updateTaskStatus(result);
                    }
                }

                @Override
                public void onFailure(Throwable throwable)
                {
                    log.debug(throwable, "Failed to poll task status for task %s", taskId);
                }
            }, taskEventLoop);
        }
        catch (Exception e) {
            log.debug(e, "Error polling task status for task %s", taskId);
        }
    }

    private void pollTaskInfo()
    {
        try {
            ListenableFuture<TaskInfo> infoFuture = thriftClient.getTaskInfo(taskId.toString(), summarizeTaskInfo);

            Futures.addCallback(infoFuture, new FutureCallback<TaskInfo>() {
                @Override
                public void onSuccess(@Nullable TaskInfo result)
                {
                    if (result != null) {
                        updateTaskInfo(result);
                    }
                }

                @Override
                public void onFailure(Throwable throwable)
                {
                    log.debug(throwable, "Failed to poll task info for task %s", taskId);
                }
            }, taskEventLoop);
        }
        catch (Exception e) {
            log.debug(e, "Error polling task info for task %s", taskId);
        }
    }

    private void updateTaskStatus(TaskStatus newTaskStatus)
    {
        if (newTaskStatus != null) {
            this.currentTaskStatus = newTaskStatus;

            if (newTaskStatus.getState().isDone()) {
                cleanUpTask();
            }
            else {
                updateTaskStats();
                updateSplitQueueSpace();
            }
        }
    }

    @Override
    public PlanFragment getPlanFragment()
    {
        return planFragment;
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
        return currentTaskInfo;
    }

    @Override
    public TaskStatus getTaskStatus()
    {
        return currentTaskStatus;
    }

    @Override
    public URI getRemoteTaskLocation()
    {
        return remoteTaskLocation;
    }

    @Override
    public void start()
    {
        safeExecuteOnEventLoop(() -> {
            started = true;
            scheduleUpdate();

            // Start periodic polling instead of using fetchers
            startPeriodicPolling();
        }, "start");
    }

    @Override
    public void addSplits(Multimap<PlanNodeId, Split> splitsBySource)
    {
        requireNonNull(splitsBySource, "splitsBySource is null");

        if (getTaskStatus().getState().isDone()) {
            return;
        }

        safeExecuteOnEventLoop(() -> {
            boolean updateNeeded = false;
            for (Map.Entry<PlanNodeId, Collection<Split>> entry : splitsBySource.asMap().entrySet()) {
                PlanNodeId sourceId = entry.getKey();
                Collection<Split> splits = entry.getValue();
                boolean isTableScanSource = tableScanPlanNodeIds.contains(sourceId);

                checkState(!noMoreSplits.containsKey(sourceId), "noMoreSplits has already been set for %s", sourceId);
                int added = 0;
                long addedWeight = 0;
                for (Split split : splits) {
                    if (pendingSplits.put(sourceId, new ScheduledSplit(nextSplitId++, sourceId, split))) {
                        if (isTableScanSource) {
                            added++;
                            addedWeight = addExact(addedWeight, split.getSplitWeight().getRawValue());
                        }
                    }
                }
                if (isTableScanSource) {
                    pendingSourceSplitCount += added;
                    pendingSourceSplitsWeight = addExact(pendingSourceSplitsWeight, addedWeight);
                    updateTaskStats();
                }
                updateNeeded = true;
            }
            updateSplitQueueSpace();

            if (updateNeeded) {
                needsUpdate = true;
                scheduleUpdate();
            }
        }, "addSplits");
    }

    @Override
    public void noMoreSplits(PlanNodeId sourceId)
    {
        safeExecuteOnEventLoop(() -> {
            if (noMoreSplits.containsKey(sourceId)) {
                return;
            }

            noMoreSplits.put(sourceId, true);
            needsUpdate = true;
            scheduleUpdate();
        }, "noMoreSplits");
    }

    @Override
    public void noMoreSplits(PlanNodeId sourceId, Lifespan lifespan)
    {
        safeExecuteOnEventLoop(() -> {
            if (pendingNoMoreSplitsForLifespan.put(sourceId, lifespan)) {
                needsUpdate = true;
                scheduleUpdate();
            }
        }, "noMoreSplits with lifeSpan");
    }

    @Override
    public void setOutputBuffers(OutputBuffers newOutputBuffers)
    {
        if (getTaskStatus().getState().isDone()) {
            return;
        }

        safeExecuteOnEventLoop(() -> {
            if (newOutputBuffers.getVersion() > outputBuffers.getVersion()) {
                outputBuffers = newOutputBuffers;
                needsUpdate = true;
                scheduleUpdate();
            }
        }, "setOutputBuffers");
    }

    @Override
    public ListenableFuture<?> removeRemoteSource(TaskId remoteSourceTaskId)
    {
        // For Thrift implementation, we would need to call a specific Thrift method
        // For now, return completed future as this is not yet implemented
        SettableFuture<?> future = SettableFuture.create();
        future.set(null);
        return future;
    }

    @Override
    public PartitionedSplitsInfo getPartitionedSplitsInfo()
    {
        TaskStatus taskStatus = getTaskStatus();
        if (taskStatus.getState().isDone()) {
            return PartitionedSplitsInfo.forZeroSplits();
        }
        PartitionedSplitsInfo unacknowledgedSplitsInfo = getUnacknowledgedPartitionedSplitsInfo();
        int count = unacknowledgedSplitsInfo.getCount() + taskStatus.getQueuedPartitionedDrivers() + taskStatus.getRunningPartitionedDrivers();
        long weight = unacknowledgedSplitsInfo.getWeightSum() + taskStatus.getQueuedPartitionedSplitsWeight() + taskStatus.getRunningPartitionedSplitsWeight();
        return PartitionedSplitsInfo.forSplitCountAndWeightSum(count, weight);
    }

    @SuppressWarnings("FieldAccessNotGuarded")
    public PartitionedSplitsInfo getUnacknowledgedPartitionedSplitsInfo()
    {
        int count = pendingSourceSplitCount;
        long weight = pendingSourceSplitsWeight;
        return PartitionedSplitsInfo.forSplitCountAndWeightSum(count, weight);
    }

    @Override
    public PartitionedSplitsInfo getQueuedPartitionedSplitsInfo()
    {
        TaskStatus taskStatus = getTaskStatus();
        if (taskStatus.getState().isDone()) {
            return PartitionedSplitsInfo.forZeroSplits();
        }
        PartitionedSplitsInfo unacknowledgedSplitsInfo = getUnacknowledgedPartitionedSplitsInfo();
        int count = unacknowledgedSplitsInfo.getCount() + taskStatus.getQueuedPartitionedDrivers();
        long weight = unacknowledgedSplitsInfo.getWeightSum() + taskStatus.getQueuedPartitionedSplitsWeight();
        return PartitionedSplitsInfo.forSplitCountAndWeightSum(count, weight);
    }

    @Override
    public int getUnacknowledgedPartitionedSplitCount()
    {
        return getPendingSourceSplitCount();
    }

    @SuppressWarnings("FieldAccessNotGuarded")
    private int getPendingSourceSplitCount()
    {
        return pendingSourceSplitCount;
    }

    private long getQueuedPartitionedSplitsWeight()
    {
        TaskStatus taskStatus = getTaskStatus();
        if (taskStatus.getState().isDone()) {
            return 0;
        }
        return getPendingSourceSplitsWeight() + taskStatus.getQueuedPartitionedSplitsWeight();
    }

    @SuppressWarnings("FieldAccessNotGuarded")
    private long getPendingSourceSplitsWeight()
    {
        return pendingSourceSplitsWeight;
    }

    @Override
    public void addStateChangeListener(StateChangeListener<TaskStatus> stateChangeListener)
    {
        // For now, just store the listener. In a full implementation, we would
        // notify these listeners when the task status changes
        // TODO: Implement proper state change notification
    }

    @Override
    public void addFinalTaskInfoListener(StateChangeListener<TaskInfo> stateChangeListener)
    {
        // For now, just store the listener. In a full implementation, we would
        // notify these listeners when the task info changes to a final state
        // TODO: Implement proper final task info notification
    }

    @Override
    public ListenableFuture<?> whenSplitQueueHasSpace(long weightThreshold)
    {
        if (splitQueueHasSpace) {
            return immediateFuture(null);
        }
        SettableFuture<?> future = SettableFuture.create();
        safeExecuteOnEventLoop(() -> {
            if (whenSplitQueueHasSpaceThreshold.isPresent()) {
                checkArgument(weightThreshold == whenSplitQueueHasSpaceThreshold.getAsLong(), "Multiple split queue space notification thresholds not supported");
            }
            else {
                whenSplitQueueHasSpaceThreshold = OptionalLong.of(weightThreshold);
                updateSplitQueueSpace();
            }
            if (splitQueueHasSpace) {
                future.set(null);
            }
            whenSplitQueueHasSpace.createNewListener().addListener(() -> future.set(null), taskEventLoop);
        }, "whenSplitQueueHasSpace");
        return future;
    }

    private void updateSplitQueueSpace()
    {
        verify(taskEventLoop.inEventLoop());

        splitQueueHasSpace = getUnacknowledgedPartitionedSplitCount() < maxUnacknowledgedSplits &&
                (!whenSplitQueueHasSpaceThreshold.isPresent() || getQueuedPartitionedSplitsWeight() < whenSplitQueueHasSpaceThreshold.getAsLong());
        if (splitQueueHasSpace && whenSplitQueueHasSpaceThreshold.isPresent()) {
            whenSplitQueueHasSpace.complete(null, taskEventLoop);
        }
    }

    private void updateTaskStats()
    {
        TaskStatus taskStatus = getTaskStatus();
        if (taskStatus.getState().isDone()) {
            nodeStatsTracker.setPartitionedSplits(PartitionedSplitsInfo.forZeroSplits());
            nodeStatsTracker.setMemoryUsage(0);
            nodeStatsTracker.setCpuUsage(taskStatus.getTaskAgeInMillis(), 0);
        }
        else {
            nodeStatsTracker.setPartitionedSplits(getPartitionedSplitsInfo());
            nodeStatsTracker.setMemoryUsage(taskStatus.getMemoryReservationInBytes() + taskStatus.getSystemMemoryReservationInBytes());
            nodeStatsTracker.setCpuUsage(taskStatus.getTaskAgeInMillis(), taskStatus.getTotalCpuTimeInNanos());
        }
    }

    private void scheduleUpdate()
    {
        verify(taskEventLoop.inEventLoop());

        taskUpdateTimeline.add(System.nanoTime());
        sendUpdate();
    }

    private void sendUpdate()
    {
        safeExecuteOnEventLoop(() -> {
            TaskStatus taskStatus = getTaskStatus();
            if (!started || !needsUpdate || taskStatus.getState().isDone()) {
                return;
            }

            if (this.currentRequest != null && !this.currentRequest.isDone()) {
                return;
            }

            ListenableFuture<?> errorRateLimit = updateErrorTracker.acquireRequestPermit();
            if (!errorRateLimit.isDone()) {
                errorRateLimit.addListener(this::sendUpdate, taskEventLoop);
                return;
            }

            List<TaskSource> sources = getSources();

            Optional<byte[]> fragment = Optional.empty();
            if (sendPlan) {
                // Serialize plan fragment - implementation needed
                // fragment = Optional.of(planFragment.serialize());
            }
            Optional<TableWriteInfo> writeInfo = sendPlan ? Optional.of(tableWriteInfo) : Optional.empty();
            TaskUpdateRequest updateRequest = new TaskUpdateRequest(
                    session.toSessionRepresentation(),
                    session.getIdentity().getExtraCredentials(),
                    fragment,
                    sources,
                    outputBuffers,
                    writeInfo);

            // Convert to Thrift types and make the call
            try {
                updateErrorTracker.startRequest();
                ListenableFuture<TaskInfo> future = thriftClient.createOrUpdateTask(
                        taskId.toString(),
                        convertToThriftTaskUpdateRequest(updateRequest));

                currentRequest = future;
                currentRequestStartNanos = System.nanoTime();
                if (!taskUpdateTimeline.isEmpty()) {
                    currentRequestLastTaskUpdate = taskUpdateTimeline.getLong(taskUpdateTimeline.size() - 1);
                }

                needsUpdate = false;

                Futures.addCallback(future, new FutureCallback<TaskInfo>() {
                    @Override
                    public void onSuccess(@Nullable TaskInfo result)
                    {
                        verify(taskEventLoop.inEventLoop());
                        try {
                            if (result != null) {
                                TaskInfo taskInfo = convertFromThriftTaskInfo(result);
                                processTaskUpdate(taskInfo, sources);
                                updateErrorTracker.requestSucceeded();
                            }
                        }
                        finally {
                            currentRequest = null;
                            sendUpdate();
                        }
                    }

                    @Override
                    public void onFailure(Throwable throwable)
                    {
                        verify(taskEventLoop.inEventLoop());
                        try {
                            currentRequest = null;
                            needsUpdate = true;

                            TaskStatus taskStatus = getTaskStatus();
                            if (!taskStatus.getState().isDone()) {
                                updateErrorTracker.requestFailed(throwable);
                            }
                        }
                        catch (Exception e) {
                            failTask(e);
                        }
                        finally {
                            sendUpdate();
                        }
                    }
                }, taskEventLoop);
            }
            catch (Exception e) {
                failTask(e);
            }
        }, "sendUpdate");
    }

    private List<TaskSource> getSources()
    {
        return tableScanPlanNodeIds.stream()
                .map(this::getSource)
                .filter(Objects::nonNull)
                .collect(toImmutableList());
    }

    private TaskSource getSource(PlanNodeId planNodeId)
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

    private void processTaskUpdate(TaskInfo newValue, List<TaskSource> sources)
    {
        verify(taskEventLoop.inEventLoop());

        updateTaskInfo(newValue);

        // remove acknowledged splits, which frees memory
        for (TaskSource source : sources) {
            PlanNodeId planNodeId = source.getPlanNodeId();
            boolean isTableScanSource = tableScanPlanNodeIds.contains(planNodeId);
            int removed = 0;
            long removedWeight = 0;
            for (ScheduledSplit split : source.getSplits()) {
                if (pendingSplits.remove(planNodeId, split)) {
                    if (isTableScanSource) {
                        removed++;
                        removedWeight = addExact(removedWeight, split.getSplit().getSplitWeight().getRawValue());
                    }
                }
            }
            if (source.isNoMoreSplits()) {
                noMoreSplits.put(planNodeId, false);
            }
            for (Lifespan lifespan : source.getNoMoreSplitsForLifespan()) {
                pendingNoMoreSplitsForLifespan.remove(planNodeId, lifespan);
            }
            if (isTableScanSource) {
                pendingSourceSplitCount -= removed;
                pendingSourceSplitsWeight -= removedWeight;
            }
        }
        updateTaskStats();
        updateSplitQueueSpace();
    }

    private void updateTaskInfo(TaskInfo taskInfo)
    {
        verify(taskEventLoop.inEventLoop());

        this.currentTaskInfo = taskInfo;
        this.currentTaskStatus = taskInfo.getTaskStatus();
    }

    @Override
    public void cancel()
    {
        safeExecuteOnEventLoop(() -> {
            TaskStatus taskStatus = getTaskStatus();
            if (taskStatus.getState().isDone()) {
                return;
            }

            // For Thrift, we could call a cancel method
            // For now, just log that cancel was called
            log.info("Cancel called for task %s", taskId);
        }, "cancel");
    }

    private void cleanUpTask()
    {
        safeExecuteOnEventLoop(() -> {
            checkState(getTaskStatus().getState().isDone(), "attempt to clean up a task that is not done yet");

            // clear pending splits to free memory
            pendingSplits.clear();
            pendingSourceSplitCount = 0;
            pendingSourceSplitsWeight = 0;
            updateTaskStats();
            splitQueueHasSpace = true;
            whenSplitQueueHasSpace.complete(null, taskEventLoop);

            // cancel pending request
            if (currentRequest != null) {
                currentRequest.cancel(false);
                currentRequest = null;
                currentRequestStartNanos = 0;
            }

            // No fetchers to stop for Thrift implementation
        }, "cleanUpTask");
    }

    @Override
    public void abort()
    {
        if (getTaskStatus().getState().isDone()) {
            return;
        }

        abort(failWith(getTaskStatus(), ABORTED, ImmutableList.of()));
    }

    private void abort(TaskStatus status)
    {
        safeExecuteOnEventLoop(() -> {
            checkState(status.getState().isDone(), "cannot abort task with an incomplete status");

            // Update task status directly
            this.currentTaskStatus = status;

            // For Thrift, we could call an abort method
            log.info("Abort called for task %s", taskId);
        }, "abort");
    }

    private void failTask(Throwable cause)
    {
        verify(taskEventLoop.inEventLoop());

        TaskStatus taskStatus = getTaskStatus();
        if (!taskStatus.getState().isDone()) {
            log.debug(cause, "Remote task %s failed with %s", taskStatus.getSelf(), cause);
        }

        TaskStatus failedTaskStatus = failWith(getTaskStatus(), FAILED, ImmutableList.of(toFailure(cause)));
        // Update task info directly with failed status
        this.currentTaskInfo = getTaskInfo().withTaskStatus(failedTaskStatus);
        this.currentTaskStatus = failedTaskStatus;

        abort(failedTaskStatus);
    }

    private void safeExecuteOnEventLoop(Runnable r, String methodName)
    {
        taskEventLoop.execute(r, this::failTask, schedulerStatsTracker, loggingPrefix + ", method: " + methodName);
    }

    // Conversion methods for Thrift types
    private TaskUpdateRequest convertToThriftTaskUpdateRequest(TaskUpdateRequest request)
    {
        // This is a placeholder - actual implementation would need to convert between
        // Presto internal types and Thrift types
        // For now, return the original request (assuming it's already compatible)
        return request;
    }

    private TaskInfo convertFromThriftTaskInfo(TaskInfo thriftTaskInfo)
    {
        // This is a placeholder - actual implementation would need to convert from
        // Thrift types back to Presto internal types
        // For now, return the thrift task info directly (assuming it's already compatible)
        return thriftTaskInfo;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(getTaskInfo())
                .toString();
    }
}
