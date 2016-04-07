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

import com.facebook.presto.OutputBuffers;
import com.facebook.presto.Session;
import com.facebook.presto.execution.NodeTaskMap.PartitionedSplitCountTracker;
import com.facebook.presto.execution.buffer.OutputBuffer;
import com.facebook.presto.execution.buffer.SharedOutputBuffer;
import com.facebook.presto.memory.MemoryPool;
import com.facebook.presto.memory.MemoryPoolId;
import com.facebook.presto.memory.QueryContext;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.operator.TaskStats;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.Partitioning;
import com.facebook.presto.sql.planner.PartitioningScheme;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.TestingColumnHandle;
import com.facebook.presto.sql.planner.TestingTableHandle;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
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
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static com.facebook.presto.OutputBuffers.BufferType.BROADCAST;
import static com.facebook.presto.OutputBuffers.createInitialEmptyOutputBuffers;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.execution.StateMachine.StateChangeListener;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static com.facebook.presto.util.Failures.toFailures;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Objects.requireNonNull;

public class MockRemoteTaskFactory
        implements RemoteTaskFactory
{
    private static final String TASK_INSTANCE_ID = "task-instance-id";
    private final Executor executor;

    public MockRemoteTaskFactory(Executor executor)
    {
        this.executor = executor;
    }

    public MockRemoteTask createTableScanTask(TaskId taskId, Node newNode, List<Split> splits, PartitionedSplitCountTracker partitionedSplitCountTracker)
    {
        Symbol symbol = new Symbol("column");
        PlanNodeId sourceId = new PlanNodeId("sourceId");
        PlanFragment testFragment = new PlanFragment(
                new PlanFragmentId("test"),
                new TableScanNode(
                        sourceId,
                        new TableHandle("test", new TestingTableHandle()),
                        ImmutableList.of(symbol),
                        ImmutableMap.of(symbol, new TestingColumnHandle("column")),
                        Optional.empty(),
                        TupleDomain.all(),
                        null),
                ImmutableMap.<Symbol, Type>of(symbol, VARCHAR),
                SOURCE_DISTRIBUTION,
                ImmutableList.of(sourceId),
                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), ImmutableList.of(symbol)));

        ImmutableMultimap.Builder<PlanNodeId, Split> initialSplits = ImmutableMultimap.builder();
        for (Split sourceSplit : splits) {
            initialSplits.put(sourceId, sourceSplit);
        }
        return createRemoteTask(TEST_SESSION, taskId, newNode, testFragment, initialSplits.build(), createInitialEmptyOutputBuffers(BROADCAST), partitionedSplitCountTracker, true);
    }

    @Override
    public MockRemoteTask createRemoteTask(
            Session session,
            TaskId taskId,
            Node node,
            PlanFragment fragment,
            Multimap<PlanNodeId, Split> initialSplits,
            OutputBuffers outputBuffers,
            PartitionedSplitCountTracker partitionedSplitCountTracker,
            boolean summarizeTaskInfo)
    {
        return new MockRemoteTask(taskId, fragment, node.getNodeIdentifier(), executor, initialSplits, partitionedSplitCountTracker);
    }

    public static final class MockRemoteTask
            implements RemoteTask
    {
        private final AtomicLong nextTaskInfoVersion = new AtomicLong(TaskStatus.STARTING_VERSION);

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

        private final PartitionedSplitCountTracker partitionedSplitCountTracker;

        public MockRemoteTask(TaskId taskId,
                PlanFragment fragment,
                String nodeId,
                Executor executor,
                Multimap<PlanNodeId, Split> initialSplits,
                PartitionedSplitCountTracker partitionedSplitCountTracker)
        {
            this.taskStateMachine = new TaskStateMachine(requireNonNull(taskId, "taskId is null"), requireNonNull(executor, "executor is null"));

            MemoryPool memoryPool = new MemoryPool(new MemoryPoolId("test"), new DataSize(1, GIGABYTE));
            MemoryPool memorySystemPool = new MemoryPool(new MemoryPoolId("testSystem"), new DataSize(1, GIGABYTE));
            this.taskContext = new QueryContext(taskId.getQueryId(), new DataSize(1, MEGABYTE), memoryPool, memorySystemPool, executor).addTaskContext(taskStateMachine, TEST_SESSION, new DataSize(1, MEGABYTE), true, true);

            this.location = URI.create("fake://task/" + taskId);

            this.outputBuffer = new SharedOutputBuffer(taskId, TASK_INSTANCE_ID, executor, requireNonNull(new DataSize(1, BYTE), "maxBufferSize is null"));
            this.fragment = requireNonNull(fragment, "fragment is null");
            this.nodeId = requireNonNull(nodeId, "nodeId is null");
            splits.putAll(initialSplits);
            this.partitionedSplitCountTracker = requireNonNull(partitionedSplitCountTracker, "partitionedSplitCountTracker is null");
            partitionedSplitCountTracker.setPartitionedSplitCount(getPartitionedSplitCount());
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
            TaskState state = taskStateMachine.getState();
            List<ExecutionFailureInfo> failures = ImmutableList.of();
            if (state == TaskState.FAILED) {
                failures = toFailures(taskStateMachine.getFailureCauses());
            }

            return new TaskInfo(new TaskStatus(taskStateMachine.getTaskId(), TASK_INSTANCE_ID, nextTaskInfoVersion.getAndIncrement(), state, location, failures, 0, 0, new DataSize(0, BYTE)),
                    DateTime.now(),
                    outputBuffer.getInfo(),
                    ImmutableSet.<PlanNodeId>of(),
                    taskContext.getTaskStats(),
                    true);
        }

        @Override
        public TaskStatus getTaskStatus()
        {
            TaskStats stats = taskContext.getTaskStats();
            return new TaskStatus(taskStateMachine.getTaskId(),
                    TASK_INSTANCE_ID,
                    nextTaskInfoVersion.get(),
                    taskStateMachine.getState(),
                    location,
                    ImmutableList.of(),
                    stats.getQueuedPartitionedDrivers(),
                    stats.getRunningPartitionedDrivers(),
                    stats.getMemoryReservation());
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
        }

        public synchronized void clearSplits()
        {
            splits.clear();
            partitionedSplitCountTracker.setPartitionedSplitCount(getPartitionedSplitCount());
            runningDrivers = 0;
        }

        public synchronized void startSplits(int maxRunning)
        {
            runningDrivers = splits.size();
            runningDrivers = Math.min(runningDrivers, maxRunning);
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
            partitionedSplitCountTracker.setPartitionedSplitCount(getPartitionedSplitCount());
        }

        @Override
        public synchronized void noMoreSplits(PlanNodeId sourceId)
        {
            noMoreSplits.add(sourceId);

            boolean allSourcesComplete = Stream.concat(fragment.getPartitionedSourceNodes().stream(), fragment.getRemoteSourceNodes().stream())
                    .filter(Objects::nonNull)
                    .map(PlanNode::getId)
                    .allMatch(noMoreSplits::contains);

            if (allSourcesComplete) {
                taskStateMachine.finished();
            }
        }

        @Override
        public void setOutputBuffers(OutputBuffers outputBuffers)
        {
            outputBuffer.setOutputBuffers(outputBuffers);
        }

        @Override
        public void addStateChangeListener(StateChangeListener<TaskStatus> stateChangeListener)
        {
            taskStateMachine.addStateChangeListener(newValue -> stateChangeListener.stateChanged(getTaskStatus()));
        }

        @Override
        public CompletableFuture<TaskStatus> getStateChange(TaskStatus taskStatus)
        {
            return taskStateMachine.getStateChange(taskStatus.getState()).thenApply(ignored -> getTaskStatus());
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
                for (PlanNodeId partitionedSource : fragment.getPartitionedSources()) {
                    Collection<Split> partitionedSplits = splits.get(partitionedSource);
                    count += partitionedSplits.size();
                }
                return count;
            }
        }

        @Override
        public int getQueuedPartitionedSplitCount()
        {
            if (taskStateMachine.getState().isDone()) {
                return 0;
            }
            return getPartitionedSplitCount() - runningDrivers;
        }
    }
}
