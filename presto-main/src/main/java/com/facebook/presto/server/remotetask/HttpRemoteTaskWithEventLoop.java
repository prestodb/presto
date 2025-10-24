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

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.HttpUriBuilder;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.http.client.ResponseHandler;
import com.facebook.airlift.http.client.StatusResponseHandler.StatusResponse;
import com.facebook.airlift.http.client.thrift.ThriftRequestUtils;
import com.facebook.airlift.http.client.thrift.ThriftResponse;
import com.facebook.airlift.http.client.thrift.ThriftResponseHandler;
import com.facebook.airlift.json.Codec;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.smile.SmileCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.airlift.stats.DecayCounter;
import com.facebook.airlift.units.DataSize;
import com.facebook.airlift.units.Duration;
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
import com.facebook.presto.execution.TaskState;
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
import com.facebook.presto.server.SimpleHttpResponseCallback;
import com.facebook.presto.server.SimpleHttpResponseHandler;
import com.facebook.presto.server.TaskUpdateRequest;
import com.facebook.presto.server.smile.BaseResponse;
import com.facebook.presto.server.thrift.ThriftHttpResponseHandler;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SplitWeight;
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
import com.sun.management.ThreadMXBean;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import jakarta.annotation.Nullable;

import java.lang.management.ManagementFactory;
import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static com.facebook.airlift.http.client.HttpStatus.NO_CONTENT;
import static com.facebook.airlift.http.client.HttpStatus.OK;
import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static com.facebook.airlift.http.client.Request.Builder.prepareDelete;
import static com.facebook.airlift.http.client.Request.Builder.preparePost;
import static com.facebook.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static com.facebook.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static com.facebook.presto.SystemSessionProperties.getMaxUnacknowledgedSplitsPerTask;
import static com.facebook.presto.execution.TaskInfo.createInitialTask;
import static com.facebook.presto.execution.TaskState.ABORTED;
import static com.facebook.presto.execution.TaskState.FAILED;
import static com.facebook.presto.execution.TaskStatus.failWith;
import static com.facebook.presto.server.RequestErrorTracker.isExpectedError;
import static com.facebook.presto.server.RequestErrorTracker.taskRequestErrorTracker;
import static com.facebook.presto.server.RequestHelpers.setContentTypeHeaders;
import static com.facebook.presto.server.RequestHelpers.setTaskInfoAcceptTypeHeaders;
import static com.facebook.presto.server.RequestHelpers.setTaskUpdateRequestContentTypeHeaders;
import static com.facebook.presto.server.smile.AdaptingJsonResponseHandler.createAdaptingJsonResponseHandler;
import static com.facebook.presto.server.smile.FullSmileResponseHandler.createFullSmileResponseHandler;
import static com.facebook.presto.server.thrift.ThriftCodecWrapper.unwrapThriftCodec;
import static com.facebook.presto.spi.StandardErrorCode.EXCEEDED_TASK_UPDATE_SIZE_LIMIT;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.REMOTE_TASK_ERROR;
import static com.facebook.presto.util.Failures.toFailure;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.util.concurrent.Futures.addCallback;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.lang.Math.addExact;
import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Represents a task running on a remote worker.
 * <p>
 * This class now uses an event loop concurrency model to eliminate the need for explicit synchronization:
 * <ul>
 * <li>All mutable state access and modifications are performed on a single dedicated event loop thread</li>
 * <li>External threads submit operations to the event loop using {@code safeExecuteOnEventLoop()}</li>
 * <li>The event loop serializes all operations, eliminating race conditions without using locks</li>
 * </ul>
 * <p>
 * Key benefits of this approach:
 * <ul>
 * <li>Improved performance by creating fewer event processing threads to support running more tasks</li>
 * <li>Simplified reasoning about concurrent operations since they are serialized</li>
 * </ul>
 */
public final class HttpRemoteTaskWithEventLoop
        implements RemoteTask
{
    private static final Logger log = Logger.get(HttpRemoteTaskWithEventLoop.class);
    private static final double UPDATE_WITHOUT_PLAN_STATS_SAMPLE_RATE = 0.01;
    private static final ThreadMXBean THREAD_MX_BEAN = (ThreadMXBean) ManagementFactory.getThreadMXBean();

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
    private final TaskInfoFetcherWithEventLoop taskInfoFetcher;
    private final ContinuousTaskStatusFetcherWithEventLoop taskStatusFetcher;

    private final LongArrayList taskUpdateTimeline = new LongArrayList();
    private Future<?> currentRequest;
    private long currentRequestStartNanos;
    private long currentRequestLastTaskUpdate;

    private final SetMultimap<PlanNodeId, ScheduledSplit> pendingSplits = HashMultimap.create();
    private final AtomicInteger pendingSourceSplitCount = new AtomicInteger();
    private final AtomicLong pendingSourceSplitsWeight = new AtomicLong();
    private final SetMultimap<PlanNodeId, Lifespan> pendingNoMoreSplitsForLifespan = HashMultimap.create();
    // The keys of this map represent all plan nodes that have "no more splits".
    // The boolean value of each entry represents whether the "no more splits" notification is pending delivery to workers.
    private final Map<PlanNodeId, Boolean> noMoreSplits = new HashMap<>();
    private OutputBuffers outputBuffers;
    private final FutureStateChange<?> whenSplitQueueHasSpace = new FutureStateChange<>();
    private volatile long whenSplitQueueWeightThreshold = Long.MAX_VALUE;

    private final boolean summarizeTaskInfo;

    private final HttpClient httpClient;

    private final Codec<TaskInfo> taskInfoCodec;
    //Json codec required for TaskUpdateRequest endpoint which uses JSON and returns a TaskInfo
    private final Codec<TaskInfo> taskInfoJsonCodec;
    private final Codec<TaskUpdateRequest> taskUpdateRequestCodec;
    private final Codec<TaskInfo> taskInfoResponseCodec;
    private final Codec<PlanFragment> planFragmentCodec;

    private final RequestErrorTracker updateErrorTracker;

    private boolean needsUpdate = true;
    private boolean sendPlan = true;

    private final NodeStatsTracker nodeStatsTracker;

    private boolean started;
    private boolean aborting;

    private final boolean binaryTransportEnabled;
    private final boolean thriftTransportEnabled;
    private final boolean taskInfoThriftTransportEnabled;
    private final boolean taskUpdateRequestThriftSerdeEnabled;
    private final boolean taskInfoResponseThriftSerdeEnabled;
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

    private long startTime;
    private long startedTime;

    public static HttpRemoteTaskWithEventLoop createHttpRemoteTaskWithEventLoop(
            Session session,
            TaskId taskId,
            String nodeId,
            URI location,
            URI remoteLocation,
            PlanFragment planFragment,
            Multimap<PlanNodeId, Split> initialSplits,
            OutputBuffers outputBuffers,
            HttpClient httpClient,
            Duration maxErrorDuration,
            Duration taskStatusRefreshMaxWait,
            Duration taskInfoRefreshMaxWait,
            Duration taskInfoUpdateInterval,
            boolean summarizeTaskInfo,
            Codec<TaskStatus> taskStatusCodec,
            Codec<TaskInfo> taskInfoCodec,
            Codec<TaskInfo> taskInfoJsonCodec,
            Codec<TaskUpdateRequest> taskUpdateRequestCodec,
            Codec<TaskInfo> taskInfoResponseCodec,
            Codec<PlanFragment> planFragmentCodec,
            NodeStatsTracker nodeStatsTracker,
            RemoteTaskStats stats,
            boolean binaryTransportEnabled,
            boolean thriftTransportEnabled,
            boolean taskInfoThriftTransportEnabled,
            boolean taskUpdateRequestThriftTransportEnabled,
            boolean taskInfoResponseThriftTransportEnabled,
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
        HttpRemoteTaskWithEventLoop task = new HttpRemoteTaskWithEventLoop(session,
                taskId,
                nodeId,
                location,
                remoteLocation,
                planFragment,
                initialSplits,
                outputBuffers,
                httpClient,
                maxErrorDuration,
                taskStatusRefreshMaxWait,
                taskInfoRefreshMaxWait,
                taskInfoUpdateInterval,
                summarizeTaskInfo,
                taskStatusCodec,
                taskInfoCodec,
                taskInfoJsonCodec,
                taskUpdateRequestCodec,
                taskInfoResponseCodec,
                planFragmentCodec,
                nodeStatsTracker,
                stats,
                binaryTransportEnabled,
                thriftTransportEnabled,
                taskInfoThriftTransportEnabled,
                taskUpdateRequestThriftTransportEnabled,
                taskInfoResponseThriftTransportEnabled,
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

    private HttpRemoteTaskWithEventLoop(Session session,
            TaskId taskId,
            String nodeId,
            URI location,
            URI remoteLocation,
            PlanFragment planFragment,
            Multimap<PlanNodeId, Split> initialSplits,
            OutputBuffers outputBuffers,
            HttpClient httpClient,
            Duration maxErrorDuration,
            Duration taskStatusRefreshMaxWait,
            Duration taskInfoRefreshMaxWait,
            Duration taskInfoUpdateInterval,
            boolean summarizeTaskInfo,
            Codec<TaskStatus> taskStatusCodec,
            Codec<TaskInfo> taskInfoCodec,
            Codec<TaskInfo> taskInfoJsonCodec,
            Codec<TaskUpdateRequest> taskUpdateRequestCodec,
            Codec<TaskInfo> taskInfoResponseCodec,
            Codec<PlanFragment> planFragmentCodec,
            NodeStatsTracker nodeStatsTracker,
            RemoteTaskStats stats,
            boolean binaryTransportEnabled,
            boolean thriftTransportEnabled,
            boolean taskInfoThriftTransportEnabled,
            boolean taskUpdateRequestThriftSerdeEnabled,
            boolean taskInfoResponseThriftSerdeEnabled,
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
        requireNonNull(httpClient, "httpClient is null");
        requireNonNull(taskStatusCodec, "taskStatusCodec is null");
        requireNonNull(taskInfoCodec, "taskInfoCodec is null");
        requireNonNull(taskUpdateRequestCodec, "taskUpdateRequestCodec is null");
        requireNonNull(planFragmentCodec, "planFragmentCodec is null");
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
        this.httpClient = httpClient;
        this.summarizeTaskInfo = summarizeTaskInfo;
        this.taskInfoCodec = taskInfoCodec;
        this.taskInfoJsonCodec = taskInfoJsonCodec;
        this.taskUpdateRequestCodec = taskUpdateRequestCodec;
        this.taskInfoResponseCodec = taskInfoResponseCodec;
        this.planFragmentCodec = planFragmentCodec;
        this.updateErrorTracker = taskRequestErrorTracker(taskId, location, maxErrorDuration, taskEventLoop, "updating task");
        this.nodeStatsTracker = requireNonNull(nodeStatsTracker, "nodeStatsTracker is null");
        this.maxErrorDuration = maxErrorDuration;
        this.stats = stats;
        this.binaryTransportEnabled = binaryTransportEnabled;
        this.thriftTransportEnabled = thriftTransportEnabled;
        this.taskInfoThriftTransportEnabled = taskInfoThriftTransportEnabled;
        this.taskUpdateRequestThriftSerdeEnabled = taskUpdateRequestThriftSerdeEnabled;
        this.taskInfoResponseThriftSerdeEnabled = taskInfoResponseThriftSerdeEnabled;
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

        for (Entry<PlanNodeId, Split> entry : requireNonNull(initialSplits, "initialSplits is null").entries()) {
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
        this.pendingSourceSplitCount.set(pendingSourceSplitCount);
        this.pendingSourceSplitsWeight.set(pendingSourceSplitsWeight);

        List<BufferInfo> bufferStates = outputBuffers.getBuffers()
                .keySet().stream()
                .map(outputId -> new BufferInfo(outputId, false, 0, 0, PageBufferInfo.empty()))
                .collect(toImmutableList());

        TaskInfo initialTask = createInitialTask(taskId, location, bufferStates, new TaskStats(currentTimeMillis(), 0), nodeId);

        this.taskStatusFetcher = new ContinuousTaskStatusFetcherWithEventLoop(
                this::failTask,
                taskId,
                initialTask.getTaskStatus(),
                taskStatusRefreshMaxWait,
                taskStatusCodec,
                taskEventLoop,
                httpClient,
                maxErrorDuration,
                stats,
                binaryTransportEnabled,
                thriftTransportEnabled,
                thriftProtocol);

        this.taskInfoFetcher = new TaskInfoFetcherWithEventLoop(
                this::failTask,
                initialTask,
                httpClient,
                taskInfoUpdateInterval,
                taskInfoRefreshMaxWait,
                taskInfoCodec,
                maxErrorDuration,
                summarizeTaskInfo,
                taskEventLoop,
                stats,
                binaryTransportEnabled,
                taskInfoThriftTransportEnabled,
                session,
                metadataManager,
                queryManager,
                handleResolver,
                thriftProtocol);
        this.loggingPrefix = format("Query: %s, Task: %s", session.getQueryId(), taskId);
    }

    // this is a separate method to ensure that the `this` reference is not leaked during construction
    private void initialize()
    {
        taskStatusFetcher.addStateChangeListener(newStatus -> {
            verify(taskEventLoop.inEventLoop());

            TaskState state = newStatus.getState();
            if (state.isDone()) {
                cleanUpTask();
            }
            else {
                updateTaskStats();
                updateSplitQueueSpace();
            }
        });

        updateTaskStats();
        safeExecuteOnEventLoop(this::updateSplitQueueSpace, "updateSplitQueueSpace");
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
        startTime = System.nanoTime();
        safeExecuteOnEventLoop(() -> {
            // to start we just need to trigger an update
            started = true;
            startedTime = System.nanoTime();
            schedulerStatsTracker.recordStartWaitForEventLoop(startedTime - startTime);
            scheduleUpdate();

            taskStatusFetcher.start();
            taskInfoFetcher.start();
        }, "start");
    }

    @Override
    public void addSplits(Multimap<PlanNodeId, Split> splitsBySource)
    {
        requireNonNull(splitsBySource, "splitsBySource is null");

        // only add pending split if not done
        if (getTaskStatus().getState().isDone()) {
            return;
        }

        int count = 0;
        long weight = 0;
        for (Entry<PlanNodeId, Collection<Split>> entry : splitsBySource.asMap().entrySet()) {
            PlanNodeId sourceId = entry.getKey();
            Collection<Split> splits = entry.getValue();

            if (tableScanPlanNodeIds.contains(sourceId)) {
                count += splits.size();
                weight += splits.stream().map(Split::getSplitWeight)
                        .mapToLong(SplitWeight::getRawValue)
                        .sum();
            }
        }
        if (count != 0) {
            pendingSourceSplitCount.addAndGet(count);
            pendingSourceSplitsWeight.addAndGet(weight);
            updateTaskStats();
        }

        safeExecuteOnEventLoop(() -> {
            boolean updateNeeded = false;
            for (Entry<PlanNodeId, Collection<Split>> entry : splitsBySource.asMap().entrySet()) {
                PlanNodeId sourceId = entry.getKey();
                Collection<Split> splits = entry.getValue();

                checkState(!noMoreSplits.containsKey(sourceId), "noMoreSplits has already been set for %s", sourceId);
                for (Split split : splits) {
                    pendingSplits.put(sourceId, new ScheduledSplit(nextSplitId++, sourceId, split));
                }
                updateNeeded = true;
            }

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
                taskEventLoop,
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
                verify(taskEventLoop.inEventLoop());

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
                verify(taskEventLoop.inEventLoop());

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
                    errorRateLimit.addListener(() -> doRemoveRemoteSource(errorTracker, request, future), taskEventLoop);
                }
            }
        };

        addCallback(httpClient.executeAsync(request, createStatusResponseHandler()), callback, taskEventLoop);
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
        return PartitionedSplitsInfo.forSplitCountAndWeightSum(pendingSourceSplitCount.get(), pendingSourceSplitsWeight.get());
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
        return pendingSourceSplitCount.get();
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
        return pendingSourceSplitsWeight.get();
    }

    @Override
    public void addStateChangeListener(StateChangeListener<TaskStatus> stateChangeListener)
    {
        taskStatusFetcher.addStateChangeListener(stateChangeListener);
    }

    @Override
    public void addFinalTaskInfoListener(StateChangeListener<TaskInfo> stateChangeListener)
    {
        taskInfoFetcher.addFinalTaskInfoListener(stateChangeListener);
    }

    @Override
    public ListenableFuture<?> whenSplitQueueHasSpace(long weightThreshold)
    {
        setSplitQueueWeightThreshold(weightThreshold);

        if (splitQueueHasSpace()) {
            return immediateFuture(null);
        }
        SettableFuture<?> future = SettableFuture.create();
        safeExecuteOnEventLoop(() -> {
            if (splitQueueHasSpace()) {
                future.set(null);
            }
            else {
                whenSplitQueueHasSpace.createNewListener().addListener(() -> future.set(null), taskEventLoop);
            }
        }, "whenSplitQueueHasSpace");
        return future;
    }

    private void setSplitQueueWeightThreshold(long weightThreshold)
    {
        long currentValue = whenSplitQueueWeightThreshold;
        if (currentValue != Long.MAX_VALUE) {
            checkArgument(weightThreshold == currentValue, "Multiple split queue space notification thresholds not supported");
        }
        else {
            whenSplitQueueWeightThreshold = weightThreshold;
        }
    }

    private boolean splitQueueHasSpace()
    {
        return getUnacknowledgedPartitionedSplitCount() < maxUnacknowledgedSplits &&
                getQueuedPartitionedSplitsWeight() < whenSplitQueueWeightThreshold;
    }

    private void updateSplitQueueSpace()
    {
        verify(taskEventLoop.inEventLoop());
        // Only trigger notifications if a listener might be registered
        if (splitQueueHasSpace()) {
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

    private void processTaskUpdate(TaskInfo newValue, List<TaskSource> sources)
    {
        verify(taskEventLoop.inEventLoop());

        //Setting the flag as false since TaskUpdateRequest is not on thrift yet.
        //Once it is converted to thrift we can use the isThrift enabled flag here.
        updateTaskInfo(newValue);

        int removed = 0;
        long removedWeight = 0;

        // remove acknowledged splits, which frees memory
        for (TaskSource source : sources) {
            PlanNodeId planNodeId = source.getPlanNodeId();
            boolean isTableScanSource = tableScanPlanNodeIds.contains(planNodeId);
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
        }
        // Update stats before split queue space to ensure node stats are up to date before waking up the scheduler
        if (removed != 0) {
            pendingSourceSplitCount.addAndGet(-removed);
            pendingSourceSplitsWeight.addAndGet(-removedWeight);
            updateTaskStats();
            updateSplitQueueSpace();
        }
    }

    private void onSuccessTaskInfo(TaskInfo result)
    {
        verify(taskEventLoop.inEventLoop());

        try {
            updateTaskInfo(result);
        }
        finally {
            if (!getTaskInfo().getTaskStatus().getState().isDone()) {
                cleanUpLocally();
            }
        }
    }

    private void updateTaskInfo(TaskInfo taskInfo)
    {
        verify(taskEventLoop.inEventLoop());

        taskStatusFetcher.updateTaskStatus(taskInfo.getTaskStatus());
        taskInfoFetcher.updateTaskInfo(taskInfo);
    }

    private void cleanUpLocally()
    {
        verify(taskEventLoop.inEventLoop());
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

    private void onFailureTaskInfo(
            Throwable t,
            String action,
            Request request,
            Backoff cleanupBackoff)
    {
        verify(taskEventLoop.inEventLoop());

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
            taskEventLoop.schedule(() -> doScheduleAsyncCleanupRequest(cleanupBackoff, request, action), delayNanos, NANOSECONDS);
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
            // don't update if the task hasn't been started yet or if it is already finished
            if (!started || !needsUpdate || taskStatus.getState().isDone()) {
                return;
            }

            // if there is a request already running, wait for it to complete
            if (this.currentRequest != null && !this.currentRequest.isDone()) {
                return;
            }

            // if throttled due to error, asynchronously wait for timeout and try again
            ListenableFuture<?> errorRateLimit = updateErrorTracker.acquireRequestPermit();
            if (!errorRateLimit.isDone()) {
                errorRateLimit.addListener(this::sendUpdate, taskEventLoop);
                return;
            }

            List<TaskSource> sources = getSources();

            Optional<byte[]> fragment = Optional.empty();
            if (sendPlan) {
                long start = THREAD_MX_BEAN.getCurrentThreadCpuTime();
                fragment = Optional.of(planFragment.bytesForTaskSerialization(planFragmentCodec));
                schedulerStatsTracker.recordTaskPlanSerializedCpuTime(THREAD_MX_BEAN.getCurrentThreadCpuTime() - start);
            }
            Optional<TableWriteInfo> writeInfo = sendPlan ? Optional.of(tableWriteInfo) : Optional.empty();
            TaskUpdateRequest updateRequest = new TaskUpdateRequest(
                    session.toSessionRepresentation(),
                    session.getIdentity().getExtraCredentials(),
                    fragment,
                    sources,
                    outputBuffers,
                    writeInfo);
            long serializeStartCpuTimeNanos = THREAD_MX_BEAN.getCurrentThreadCpuTime();
            Request.Builder requestBuilder;
            HttpUriBuilder uriBuilder = getHttpUriBuilder(taskStatus);

            byte[] taskUpdateRequestBytes = taskUpdateRequestCodec.toBytes(updateRequest);
            schedulerStatsTracker.recordTaskUpdateSerializedCpuTime(THREAD_MX_BEAN.getCurrentThreadCpuTime() - serializeStartCpuTimeNanos);

            if (taskUpdateRequestBytes.length > maxTaskUpdateSizeInBytes) {
                failTask(new PrestoException(EXCEEDED_TASK_UPDATE_SIZE_LIMIT, getExceededTaskUpdateSizeMessage(taskUpdateRequestBytes)));
            }

            if (taskUpdateSizeTrackingEnabled) {
                taskUpdateRequestSize.add(taskUpdateRequestBytes.length);

                if (fragment.isPresent()) {
                    stats.updateWithPlanSize(taskUpdateRequestBytes.length);
                }
                else {
                    if (ThreadLocalRandom.current().nextDouble() < UPDATE_WITHOUT_PLAN_STATS_SAMPLE_RATE) {
                        // This is to keep track of the task update size even when the plan fragment is NOT present
                        stats.updateWithoutPlanSize(taskUpdateRequestBytes.length);
                    }
                }
            }

            requestBuilder = setTaskUpdateRequestContentTypeHeaders(taskUpdateRequestThriftSerdeEnabled, binaryTransportEnabled, preparePost());
            requestBuilder = setTaskInfoAcceptTypeHeaders(taskInfoResponseThriftSerdeEnabled, binaryTransportEnabled, requestBuilder);
            Request request = requestBuilder
                    .setUri(uriBuilder.build())
                    .setBodyGenerator(createStaticBodyGenerator(taskUpdateRequestBytes))
                    .build();

            ResponseHandler responseHandler;
            if (taskInfoResponseThriftSerdeEnabled) {
                responseHandler = new ThriftResponseHandler(unwrapThriftCodec(taskInfoResponseCodec));
            }
            else if (binaryTransportEnabled) {
                responseHandler = createFullSmileResponseHandler((SmileCodec<TaskInfo>) taskInfoResponseCodec);
            }
            else {
                responseHandler = createAdaptingJsonResponseHandler((JsonCodec<TaskInfo>) taskInfoResponseCodec);
            }

            updateErrorTracker.startRequest();

            ListenableFuture<?> future = httpClient.executeAsync(request, responseHandler);
            currentRequest = future;
            currentRequestStartNanos = System.nanoTime();
            if (!taskUpdateTimeline.isEmpty()) {
                currentRequestLastTaskUpdate = taskUpdateTimeline.getLong(taskUpdateTimeline.size() - 1);
            }

            // The needsUpdate flag needs to be set to false BEFORE adding the Future callback since callback might change the flag value
            // and does so without grabbing the instance lock.
            needsUpdate = false;

            if (taskInfoResponseThriftSerdeEnabled) {
                Futures.addCallback(
                        (ListenableFuture<ThriftResponse<TaskInfo>>) future,
                        new ThriftHttpResponseHandler<>(new UpdateResponseHandler(sources), request.getUri(), stats.getHttpResponseStats(), REMOTE_TASK_ERROR),
                        taskEventLoop);
            }
            else {
                Futures.addCallback(
                        (ListenableFuture<BaseResponse<TaskInfo>>) future,
                        new SimpleHttpResponseHandler<>(new UpdateResponseHandler(sources), request.getUri(), stats.getHttpResponseStats(), REMOTE_TASK_ERROR),
                        taskEventLoop);
            }
        }, "sendUpdate");
    }

    private String getExceededTaskUpdateSizeMessage(byte[] taskUpdateRequestJson)
    {
        DataSize taskUpdateSize = DataSize.succinctBytes(taskUpdateRequestJson.length);
        return format("TaskUpdate size of %s has exceeded the limit of %s", taskUpdateSize.toString(), this.maxTaskUpdateDataSize.toString());
    }

    private List<TaskSource> getSources()
    {
        return Stream.concat(tableScanPlanNodeIds.stream(), remoteSourcePlanNodeIds.stream())
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

    @Override
    public void cancel()
    {
        safeExecuteOnEventLoop(() -> {
            TaskStatus taskStatus = getTaskStatus();
            if (taskStatus.getState().isDone()) {
                return;
            }

            // send cancel to task and ignore response
            HttpUriBuilder uriBuilder = getHttpUriBuilder(taskStatus).addParameter("abort", "false");
            Request.Builder builder = setContentTypeHeaders(binaryTransportEnabled, prepareDelete());
            if (taskInfoThriftTransportEnabled) {
                builder = ThriftRequestUtils.prepareThriftDelete(thriftProtocol);
            }
            Request request = builder.setUri(uriBuilder.build())
                    .build();
            scheduleAsyncCleanupRequest(createCleanupBackoff(), request, "cancel");
        }, "cancel");
    }

    private void cleanUpTask()
    {
        safeExecuteOnEventLoop(() -> {
            checkState(getTaskStatus().getState().isDone(), "attempt to clean up a task that is not done yet");

            // clear pending splits to free memory
            pendingSplits.clear();
            pendingSourceSplitCount.set(0);
            pendingSourceSplitsWeight.set(0);
            updateTaskStats();
            whenSplitQueueHasSpace.complete(null, taskEventLoop);

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
            Request.Builder requestBuilder = setContentTypeHeaders(binaryTransportEnabled, prepareDelete());
            if (taskInfoThriftTransportEnabled) {
                requestBuilder = ThriftRequestUtils.prepareThriftDelete(thriftProtocol);
            }
            Request request = requestBuilder
                    .setUri(uriBuilder.build())
                    .build();

            scheduleAsyncCleanupRequest(createCleanupBackoff(), request, "cleanup");
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

            taskStatusFetcher.updateTaskStatus(status);

            // send abort to task
            HttpUriBuilder uriBuilder = getHttpUriBuilder(getTaskStatus());
            Request.Builder builder = setContentTypeHeaders(binaryTransportEnabled, prepareDelete());
            if (taskInfoThriftTransportEnabled) {
                builder = ThriftRequestUtils.prepareThriftDelete(thriftProtocol);
            }

            Request request = builder.setUri(uriBuilder.build())
                    .build();
            scheduleAsyncCleanupRequest(createCleanupBackoff(), request, "abort");
        }, "abort");
    }

    private void scheduleAsyncCleanupRequest(Backoff cleanupBackoff, Request request, String action)
    {
        verify(taskEventLoop.inEventLoop());

        if (aborting) {
            // Do not initiate another round of cleanup requests if one had been initiated.
            // Otherwise, we can get into an asynchronous recursion here. For example, when aborting a task after REMOTE_TASK_MISMATCH.
            return;
        }
        aborting = true;
        doScheduleAsyncCleanupRequest(cleanupBackoff, request, action);
    }

    private void doScheduleAsyncCleanupRequest(Backoff cleanupBackoff, Request request, String action)
    {
        verify(taskEventLoop.inEventLoop());

        ResponseHandler responseHandler;
        if (taskInfoThriftTransportEnabled) {
            responseHandler = new ThriftResponseHandler(unwrapThriftCodec(taskInfoCodec));
            Futures.addCallback(httpClient.executeAsync(request, responseHandler),
                    new ThriftResponseFutureCallback(action, request, cleanupBackoff),
                    taskEventLoop);
        }
        else if (binaryTransportEnabled) {
            responseHandler = createFullSmileResponseHandler((SmileCodec<TaskInfo>) taskInfoCodec);
            Futures.addCallback(httpClient.executeAsync(request, responseHandler),
                    new BaseResponseFutureCallback(action, request, cleanupBackoff),
                    taskEventLoop);
        }
        else {
            responseHandler = createAdaptingJsonResponseHandler((JsonCodec<TaskInfo>) taskInfoCodec);
            Futures.addCallback(httpClient.executeAsync(request, responseHandler),
                    new BaseResponseFutureCallback(action, request, cleanupBackoff),
                    taskEventLoop);
        }
    }

    /**
     * Move the task directly to the failed state if there was a failure in this task
     */
    private void failTask(Throwable cause)
    {
        verify(taskEventLoop.inEventLoop());

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
            verify(taskEventLoop.inEventLoop());

            try {
                long oldestTaskUpdateTime = 0;
                currentRequest = null;
                sendPlan = value.isNeedsPlan();
                if (!taskUpdateTimeline.isEmpty()) {
                    oldestTaskUpdateTime = taskUpdateTimeline.getLong(0);
                }
                int deliveredUpdates = taskUpdateTimeline.size();
                while (deliveredUpdates > 0 && taskUpdateTimeline.getLong(deliveredUpdates - 1) > currentRequestLastTaskUpdate) {
                    deliveredUpdates--;
                }
                taskUpdateTimeline.removeElements(0, deliveredUpdates);

                updateStats(currentRequestStartNanos);
                processTaskUpdate(value, sources);
                updateErrorTracker.requestSucceeded();
                if (oldestTaskUpdateTime != 0) {
                    schedulerStatsTracker.recordDeliveredUpdates(deliveredUpdates);
                    schedulerStatsTracker.recordTaskUpdateDeliveredTime(System.nanoTime() - oldestTaskUpdateTime);
                }
            }
            finally {
                sendUpdate();
            }
        }

        @Override
        public void failed(Throwable cause)
        {
            verify(taskEventLoop.inEventLoop());

            try {
                long currentRequestStartNanos;
                currentRequest = null;
                currentRequestStartNanos = HttpRemoteTaskWithEventLoop.this.currentRequestStartNanos;
                updateStats(currentRequestStartNanos);

                // on failure assume we need to update again
                needsUpdate = true;

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

        @Override
        public void fatal(Throwable cause)
        {
            verify(taskEventLoop.inEventLoop());

            failTask(cause);
        }

        private void updateStats(long currentRequestStartNanos)
        {
            verify(taskEventLoop.inEventLoop());
            Duration requestRoundTrip = Duration.nanosSince(currentRequestStartNanos);
            stats.updateRoundTripMillis(requestRoundTrip.toMillis());
            schedulerStatsTracker.recordRoundTripTime(requestRoundTrip.toMillis() * 1000000);
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

    private class ThriftResponseFutureCallback
            implements FutureCallback<ThriftResponse<TaskInfo>>
    {
        private final String action;
        private final Request request;
        private final Backoff cleanupBackoff;

        public ThriftResponseFutureCallback(String action, Request request, Backoff cleanupBackoff)
        {
            this.action = action;
            this.request = request;
            this.cleanupBackoff = cleanupBackoff;
        }

        @Override
        public void onSuccess(ThriftResponse<TaskInfo> result)
        {
            verify(taskEventLoop.inEventLoop());
            if (result.getException() != null) {
                onFailure(result.getException());
                return;
            }

            TaskInfo taskInfo = result.getValue();
            if (taskInfo == null) {
                onFailure(new RuntimeException("TaskInfo is null"));
                return;
            }
            onSuccessTaskInfo(taskInfo);
        }

        @Override
        public void onFailure(Throwable throwable)
        {
            verify(taskEventLoop.inEventLoop());
            onFailureTaskInfo(throwable, this.action, this.request, this.cleanupBackoff);
        }
    }

    private class BaseResponseFutureCallback
            implements FutureCallback<BaseResponse<TaskInfo>>
    {
        private final String action;
        private final Request request;
        private final Backoff cleanupBackoff;

        public BaseResponseFutureCallback(String action, Request request, Backoff cleanupBackoff)
        {
            this.action = action;
            this.request = request;
            this.cleanupBackoff = cleanupBackoff;
        }

        @Override
        public void onSuccess(BaseResponse<TaskInfo> result)
        {
            verify(taskEventLoop.inEventLoop());
            onSuccessTaskInfo(result.getValue());
        }

        @Override
        public void onFailure(Throwable throwable)
        {
            verify(taskEventLoop.inEventLoop());
            onFailureTaskInfo(throwable, this.action, this.request, this.cleanupBackoff);
        }
    }

    private void safeExecuteOnEventLoop(Runnable r, String methodName)
    {
        taskEventLoop.execute(r, this::failTask, schedulerStatsTracker, loggingPrefix + ", method: " + methodName);
    }
}
