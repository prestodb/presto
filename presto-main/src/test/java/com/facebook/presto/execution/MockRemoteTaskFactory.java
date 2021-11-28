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
package com.facebook.presto.execution;

import com.facebook.airlift.stats.TestingGcMonitor;
import com.facebook.presto.Session;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.cost.StatsAndCosts;
import com.facebook.presto.execution.NodeTaskMap.NodeStatsTracker;
import com.facebook.presto.execution.buffer.LazyOutputBuffer;
import com.facebook.presto.execution.buffer.OutputBuffer;
import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.execution.buffer.SpoolingOutputBufferFactory;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.memory.MemoryPool;
import com.facebook.presto.memory.QueryContext;
import com.facebook.presto.memory.context.SimpleLocalMemoryContext;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.operator.StageExecutionDescriptor;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.operator.TaskMemoryReservationSummary;
import com.facebook.presto.operator.TaskStats;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.memory.MemoryPoolId;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spiller.SpillSpaceTracker;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.planner.Partitioning;
import com.facebook.presto.sql.planner.PartitioningScheme;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.testing.TestingHandle;
import com.facebook.presto.testing.TestingMetadata.TestingColumnHandle;
import com.facebook.presto.testing.TestingMetadata.TestingTableHandle;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.DataSize;
import org.joda.time.DateTime;

import javax.annotation.concurrent.GuardedBy;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static com.facebook.airlift.json.JsonCodec.listJsonCodec;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.execution.StateMachine.StateChangeListener;
import static com.facebook.presto.execution.buffer.OutputBuffers.BufferType.BROADCAST;
import static com.facebook.presto.execution.buffer.OutputBuffers.createInitialEmptyOutputBuffers;
import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.facebook.presto.metadata.MetadataUpdates.DEFAULT_METADATA_UPDATES;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static com.facebook.presto.util.Failures.toFailures;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.Futures.nonCancellationPropagating;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Objects.requireNonNull;

public class MockRemoteTaskFactory
        implements RemoteTaskFactory
{
    private static final UUID TASK_INSTANCE_ID = UUID.randomUUID();
    private final Executor executor;
    private final ScheduledExecutorService scheduledExecutor;

    public MockRemoteTaskFactory(Executor executor, ScheduledExecutorService scheduledExecutor)
    {
        this.executor = executor;
        this.scheduledExecutor = scheduledExecutor;
    }

    public MockRemoteTask createTableScanTask(TaskId taskId, InternalNode newNode, List<Split> splits, NodeTaskMap.NodeStatsTracker nodeStatsTracker)
    {
        VariableReferenceExpression variable = new VariableReferenceExpression("column", VARCHAR);
        PlanNodeId sourceId = new PlanNodeId("sourceId");
        PlanFragment testFragment = new PlanFragment(
                new PlanFragmentId(0),
                new TableScanNode(
                        sourceId,
                        new TableHandle(new ConnectorId("test"), new TestingTableHandle(), TestingTransactionHandle.create(), Optional.of(TestingHandle.INSTANCE)),
                        ImmutableList.of(variable),
                        ImmutableMap.of(variable, new TestingColumnHandle("column")),
                        TupleDomain.all(),
                        TupleDomain.all()),
                ImmutableSet.of(variable),
                SOURCE_DISTRIBUTION,
                ImmutableList.of(sourceId),
                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), ImmutableList.of(variable)),
                StageExecutionDescriptor.ungroupedExecution(),
                false,
                StatsAndCosts.empty(),
                Optional.empty());

        ImmutableMultimap.Builder<PlanNodeId, Split> initialSplits = ImmutableMultimap.builder();
        for (Split sourceSplit : splits) {
            initialSplits.put(sourceId, sourceSplit);
        }
        return createRemoteTask(
                TEST_SESSION,
                taskId,
                newNode,
                testFragment,
                initialSplits.build(),
                createInitialEmptyOutputBuffers(BROADCAST),
                nodeStatsTracker,
                true,
                new TableWriteInfo(Optional.empty(), Optional.empty(), Optional.empty()));
    }

    @Override
    public MockRemoteTask createRemoteTask(
            Session session,
            TaskId taskId,
            InternalNode node,
            PlanFragment fragment,
            Multimap<PlanNodeId, Split> initialSplits,
            OutputBuffers outputBuffers,
            NodeTaskMap.NodeStatsTracker nodeStatsTracker,
            boolean summarizeTaskInfo,
            TableWriteInfo tableWriteInfo)
    {
        return new MockRemoteTask(taskId, fragment, node.getNodeIdentifier(), executor, scheduledExecutor, initialSplits, nodeStatsTracker);
    }

    public static final class MockRemoteTask
            implements RemoteTask
    {
        private final AtomicLong nextTaskInfoVersion = new AtomicLong(TaskStatus.STARTING_VERSION);
        private final AtomicLong nextAgeOffset = new AtomicLong(0);

        private final URI location;
        private final TaskStateMachine taskStateMachine;
        private final TaskContext taskContext;
        private final OutputBuffer outputBuffer;
        private final String nodeId;

        private final PlanFragment fragment;

        @GuardedBy("this")
        private final Set<PlanNodeId> noMoreSplits = new HashSet<>();

        @GuardedBy("this")
        private final Multimap<PlanNodeId, Split> splits = HashMultimap.create();

        @GuardedBy("this")
        private int runningDrivers;

        @GuardedBy("this")
        private int maxUnacknowledgedSplits = Integer.MAX_VALUE;
        @GuardedBy("this")
        private int unacknowledgedSplits;

        @GuardedBy("this")
        private SettableFuture<?> whenSplitQueueHasSpace = SettableFuture.create();

        private final NodeStatsTracker nodeStatsTracker;

        public MockRemoteTask(TaskId taskId,
                PlanFragment fragment,
                String nodeId,
                Executor executor,
                ScheduledExecutorService scheduledExecutor,
                Multimap<PlanNodeId, Split> initialSplits,
                NodeTaskMap.NodeStatsTracker nodeStatsTracker)
        {
            this.taskStateMachine = new TaskStateMachine(requireNonNull(taskId, "taskId is null"), requireNonNull(executor, "executor is null"));

            MemoryPool memoryPool = new MemoryPool(new MemoryPoolId("test"), new DataSize(1, GIGABYTE));
            SpillSpaceTracker spillSpaceTracker = new SpillSpaceTracker(new DataSize(1, GIGABYTE));
            QueryContext queryContext = new QueryContext(taskId.getQueryId(),
                    new DataSize(1, MEGABYTE),
                    new DataSize(2, MEGABYTE),
                    new DataSize(1, MEGABYTE),
                    new DataSize(1, GIGABYTE),
                    memoryPool,
                    new TestingGcMonitor(),
                    executor,
                    scheduledExecutor,
                    new DataSize(1, MEGABYTE),
                    spillSpaceTracker,
                    listJsonCodec(TaskMemoryReservationSummary.class));
            this.taskContext = queryContext.addTaskContext(
                    taskStateMachine,
                    TEST_SESSION,
                    Optional.of(fragment.getRoot()),
                    true,
                    true,
                    true,
                    true,
                    false);

            this.location = URI.create("fake://task/" + taskId);

            this.outputBuffer = new LazyOutputBuffer(
                    taskId,
                    TASK_INSTANCE_ID.toString(),
                    executor,
                    new DataSize(1, BYTE),
                    () -> new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), "test"),
                    new SpoolingOutputBufferFactory(new FeaturesConfig()));

            this.fragment = requireNonNull(fragment, "fragment is null");
            this.nodeId = requireNonNull(nodeId, "nodeId is null");
            splits.putAll(initialSplits);
            this.nodeStatsTracker = requireNonNull(nodeStatsTracker, "nodeStatsTracker is null");
            updateTaskStats();
            updateSplitQueueSpace();
        }

        @Override
        public TaskId getTaskId()
        {
            return taskStateMachine.getTaskId();
        }

        @Override
        public String getNodeId()
        {
            return nodeId;
        }

        @Override
        public TaskInfo getTaskInfo()
        {
            TaskStats stats = taskContext.getTaskStats();
            TaskState state = taskStateMachine.getState();
            List<ExecutionFailureInfo> failures = ImmutableList.of();
            if (state == TaskState.FAILED) {
                failures = toFailures(taskStateMachine.getFailureCauses());
            }

            return new TaskInfo(
                    taskStateMachine.getTaskId(),
                    new TaskStatus(
                            TASK_INSTANCE_ID.getLeastSignificantBits(),
                            TASK_INSTANCE_ID.getMostSignificantBits(),
                            nextTaskInfoVersion.getAndIncrement(),
                            state,
                            location,
                            ImmutableSet.of(),
                            failures,
                            0,
                            0,
                            0.0,
                            false,
                            0,
                            0,
                            0,
                            0,
                            0,
                            0,
                            0,
                            System.currentTimeMillis() + 100 - stats.getCreateTime().getMillis()),
                    DateTime.now(),
                    outputBuffer.getInfo(),
                    ImmutableSet.of(),
                    taskContext.getTaskStats(),
                    true,
                    DEFAULT_METADATA_UPDATES,
                    nodeId);
        }

        @Override
        public URI getRemoteTaskLocation()
        {
            return location;
        }

        @Override
        public TaskStatus getTaskStatus()
        {
            TaskStats stats = taskContext.getTaskStats();
            return new TaskStatus(
                    TASK_INSTANCE_ID.getLeastSignificantBits(),
                    TASK_INSTANCE_ID.getMostSignificantBits(),
                    nextTaskInfoVersion.get(),
                    taskStateMachine.getState(),
                    location,
                    ImmutableSet.of(),
                    ImmutableList.of(),
                    stats.getQueuedPartitionedDrivers(),
                    stats.getRunningPartitionedDrivers(),
                    0.0,
                    false,
                    stats.getPhysicalWrittenDataSizeInBytes(),
                    stats.getUserMemoryReservationInBytes(),
                    stats.getSystemMemoryReservationInBytes(),
                    stats.getPeakNodeTotalMemoryInBytes(),
                    0,
                    0,
                    stats.getTotalCpuTimeInNanos(),
                    // Adding 100 millis to make sure task age > 0 for testing
                    System.currentTimeMillis() + 100 - stats.getCreateTime().getMillis());
        }

        private void updateTaskStats()
        {
            TaskStatus taskStatus = getTaskStatus();
            if (taskStatus.getState().isDone()) {
                nodeStatsTracker.setPartitionedSplitCount(0);
                nodeStatsTracker.setMemoryUsage(0);
                nodeStatsTracker.setCpuUsage(taskStatus.getTaskAgeInMillis(), 0);
            }
            else {
                nodeStatsTracker.setPartitionedSplitCount(getPartitionedSplitCount());
                // setting some values for testing
                nodeStatsTracker.setMemoryUsage(100);
                long ageOffset = nextAgeOffset.addAndGet(1);
                nodeStatsTracker.setCpuUsage(taskStatus.getTaskAgeInMillis() + ageOffset, taskStatus.getTaskAgeInMillis() + ageOffset);
            }
        }

        private synchronized void updateSplitQueueSpace()
        {
            if (unacknowledgedSplits < maxUnacknowledgedSplits && getQueuedPartitionedSplitCount() < 9) {
                if (!whenSplitQueueHasSpace.isDone()) {
                    whenSplitQueueHasSpace.set(null);
                }
            }
            else {
                if (whenSplitQueueHasSpace.isDone()) {
                    whenSplitQueueHasSpace = SettableFuture.create();
                }
            }
        }

        public synchronized void finishSplits(int splits)
        {
            List<Map.Entry<PlanNodeId, Split>> toRemove = new ArrayList<>();
            Iterator<Map.Entry<PlanNodeId, Split>> iterator = this.splits.entries().iterator();
            while (toRemove.size() < splits && iterator.hasNext()) {
                toRemove.add(iterator.next());
            }
            for (Map.Entry<PlanNodeId, Split> entry : toRemove) {
                this.splits.remove(entry.getKey(), entry.getValue());
            }
            updateSplitQueueSpace();
        }

        public synchronized void clearSplits()
        {
            unacknowledgedSplits = 0;
            splits.clear();
            updateTaskStats();
            runningDrivers = 0;
            updateSplitQueueSpace();
        }

        public synchronized void startSplits(int maxRunning)
        {
            runningDrivers = splits.size();
            runningDrivers = Math.min(runningDrivers, maxRunning);
            updateSplitQueueSpace();
        }

        public synchronized void setMaxUnacknowledgedSplits(int maxUnacknowledgedSplits)
        {
            checkArgument(maxUnacknowledgedSplits > 0);
            this.maxUnacknowledgedSplits = maxUnacknowledgedSplits;
            updateSplitQueueSpace();
        }

        public synchronized void setUnacknowledgedSplits(int unacknowledgedSplits)
        {
            checkArgument(unacknowledgedSplits >= 0);
            this.unacknowledgedSplits = unacknowledgedSplits;
            updateSplitQueueSpace();
        }

        @Override
        public void start()
        {
            taskStateMachine.addStateChangeListener(newValue -> {
                if (newValue.isDone()) {
                    clearSplits();
                }
            });
        }

        @Override
        public void addSplits(Multimap<PlanNodeId, Split> splits)
        {
            synchronized (this) {
                this.splits.putAll(splits);
            }
            updateTaskStats();
            updateSplitQueueSpace();
        }

        @Override
        public synchronized void noMoreSplits(PlanNodeId sourceId)
        {
            noMoreSplits.add(sourceId);

            boolean allSourcesComplete = Stream.concat(
                    fragment.getTableScanSchedulingOrder().stream(),
                    fragment.getRemoteSourceNodes().stream()
                            .map(PlanNode::getId))
                    .allMatch(noMoreSplits::contains);

            if (allSourcesComplete) {
                taskStateMachine.finished();
            }
        }

        @Override
        public void noMoreSplits(PlanNodeId sourceId, Lifespan lifespan)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setOutputBuffers(OutputBuffers outputBuffers)
        {
            outputBuffer.setOutputBuffers(outputBuffers);
        }

        @Override
        public ListenableFuture<?> removeRemoteSource(TaskId remoteSourceTaskId)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addStateChangeListener(StateChangeListener<TaskStatus> stateChangeListener)
        {
            taskStateMachine.addStateChangeListener(newValue -> stateChangeListener.stateChanged(getTaskStatus()));
        }

        @Override
        public void addFinalTaskInfoListener(StateChangeListener<TaskInfo> stateChangeListener)
        {
            AtomicBoolean done = new AtomicBoolean();
            StateChangeListener<TaskState> fireOnceStateChangeListener = state -> {
                if (state.isDone() && done.compareAndSet(false, true)) {
                    stateChangeListener.stateChanged(getTaskInfo());
                }
            };
            taskStateMachine.addStateChangeListener(fireOnceStateChangeListener);
            fireOnceStateChangeListener.stateChanged(taskStateMachine.getState());
        }

        @Override
        public synchronized ListenableFuture<?> whenSplitQueueHasSpace(int threshold)
        {
            return nonCancellationPropagating(whenSplitQueueHasSpace);
        }

        @Override
        public void cancel()
        {
            taskStateMachine.cancel();
        }

        @Override
        public void abort()
        {
            taskStateMachine.abort();
            clearSplits();
        }

        @Override
        public int getPartitionedSplitCount()
        {
            if (taskStateMachine.getState().isDone()) {
                return 0;
            }
            synchronized (this) {
                int count = 0;
                for (PlanNodeId tableScanPlanNodeId : fragment.getTableScanSchedulingOrder()) {
                    Collection<Split> partitionedSplits = splits.get(tableScanPlanNodeId);
                    count += partitionedSplits.size();
                }
                return count;
            }
        }

        @Override
        public synchronized int getQueuedPartitionedSplitCount()
        {
            if (taskStateMachine.getState().isDone()) {
                return 0;
            }
            return getPartitionedSplitCount() - runningDrivers;
        }

        @Override
        public synchronized int getUnacknowledgedPartitionedSplitCount()
        {
            return unacknowledgedSplits;
        }
    }
}
