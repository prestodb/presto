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
import com.facebook.presto.execution.NodeTaskMap.SplitCountChangeListener;
import com.facebook.presto.memory.MemoryPool;
import com.facebook.presto.memory.MemoryPoolId;
import com.facebook.presto.memory.QueryContext;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
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
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import io.airlift.units.DataSize;
import org.joda.time.DateTime;

import javax.annotation.concurrent.GuardedBy;

import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.execution.StateMachine.StateChangeListener;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.util.Failures.toFailures;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Objects.requireNonNull;

public class MockRemoteTaskFactory
        implements RemoteTaskFactory
{
    private final Executor executor;

    public MockRemoteTaskFactory(Executor executor)
    {
        this.executor = executor;
    }

    public RemoteTask createTableScanTask(Node newNode, List<Split> splits, SplitCountChangeListener splitCountChangeListener)
    {
        TaskId taskId = new TaskId(new StageId("test", "1"), "1");
        Symbol symbol = new Symbol("column");
        PlanNodeId tableScanNodeId = new PlanNodeId("test");
        PlanNodeId sourceId = new PlanNodeId("sourceId");
        PlanFragment testFragment = new PlanFragment(
                new PlanFragmentId("test"),
                new TableScanNode(
                        new PlanNodeId("test"),
                        new TableHandle("test", new TestingTableHandle()),
                        ImmutableList.of(symbol),
                        ImmutableMap.of(symbol, new TestingColumnHandle("column")),
                        Optional.empty(),
                        TupleDomain.all(),
                        null),
                ImmutableMap.<Symbol, Type>of(symbol, VARCHAR),
                ImmutableList.of(symbol),
                PlanFragment.PlanDistribution.SOURCE,
                tableScanNodeId,
                PlanFragment.OutputPartitioning.NONE,
                Optional.empty(),
                Optional.empty(),
                Optional.empty()
        );

        ImmutableMultimap.Builder<PlanNodeId, Split> initialSplits = ImmutableMultimap.builder();
        for (Split sourceSplit : splits) {
            initialSplits.put(sourceId, sourceSplit);
        }
        return createRemoteTask(TEST_SESSION, taskId, newNode, testFragment, initialSplits.build(), OutputBuffers.INITIAL_EMPTY_OUTPUT_BUFFERS, splitCountChangeListener);
    }

    @Override
    public RemoteTask createRemoteTask(
            Session session,
            TaskId taskId,
            Node node,
            PlanFragment fragment,
            Multimap<PlanNodeId, Split> initialSplits,
            OutputBuffers outputBuffers,
            SplitCountChangeListener splitCountChangeListener)
    {
        return new MockRemoteTask(taskId, fragment, node.getNodeIdentifier(), executor, initialSplits, splitCountChangeListener);
    }

    public static class MockRemoteTask
            implements RemoteTask
    {
        private final AtomicLong nextTaskInfoVersion = new AtomicLong(TaskInfo.STARTING_VERSION);

        private final URI location;
        private final TaskStateMachine taskStateMachine;
        private final TaskContext taskContext;
        private final SharedBuffer sharedBuffer;
        private final String nodeId;

        private final PlanFragment fragment;

        @GuardedBy("this")
        private final Set<PlanNodeId> noMoreSplits = new HashSet<>();

        @GuardedBy("this")
        private final Multimap<PlanNodeId, Split> splits = HashMultimap.create();

        private final SplitCountChangeListener splitCountChangeListener;

        public MockRemoteTask(TaskId taskId,
                PlanFragment fragment,
                String nodeId,
                Executor executor,
                Multimap<PlanNodeId, Split> initialSplits,
                SplitCountChangeListener splitCountChangeListener)
        {
            this.taskStateMachine = new TaskStateMachine(requireNonNull(taskId, "taskId is null"), requireNonNull(executor, "executor is null"));

            MemoryPool memoryPool = new MemoryPool(new MemoryPoolId("test"), new DataSize(1, GIGABYTE));
            MemoryPool memorySystemPool = new MemoryPool(new MemoryPoolId("testSystem"), new DataSize(1, GIGABYTE));
            this.taskContext = new QueryContext(taskId.getQueryId(), new DataSize(1, MEGABYTE), memoryPool, memorySystemPool, executor).addTaskContext(taskStateMachine, TEST_SESSION, new DataSize(1, MEGABYTE), true, true);

            this.location = URI.create("fake://task/" + taskId);

            this.sharedBuffer = new SharedBuffer(taskId, executor, requireNonNull(new DataSize(1, DataSize.Unit.BYTE), "maxBufferSize is null"));
            this.fragment = requireNonNull(fragment, "fragment is null");
            this.nodeId = requireNonNull(nodeId, "nodeId is null");
            splits.putAll(initialSplits);
            this.splitCountChangeListener = requireNonNull(splitCountChangeListener, "splitCountChangeListener is null");
            this.splitCountChangeListener.splitCountChanged(initialSplits.size());
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

            return new TaskInfo(
                    taskStateMachine.getTaskId(),
                    Optional.empty(),
                    nextTaskInfoVersion.getAndIncrement(),
                    state,
                    location,
                    DateTime.now(),
                    sharedBuffer.getInfo(),
                    ImmutableSet.<PlanNodeId>of(),
                    taskContext.getTaskStats(),
                    failures);
        }

        public synchronized void clearSplits()
        {
            splitCountChangeListener.splitCountChanged(-splits.size());
            splits.clear();
        }

        @Override
        public void start()
        {
        }

        @Override
        public synchronized void addSplits(PlanNodeId sourceId, Iterable<Split> splits)
        {
            requireNonNull(splits, "splits is null");
            for (Split split : splits) {
                this.splits.put(sourceId, split);
            }
            splitCountChangeListener.splitCountChanged(Iterables.size(splits));
        }

        @Override
        public void noMoreSplits(PlanNodeId sourceId)
        {
            noMoreSplits.add(sourceId);

            boolean allSourcesComplete = Stream.concat(Stream.of(fragment.getPartitionedSourceNode()), fragment.getRemoteSourceNodes().stream())
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
            sharedBuffer.setOutputBuffers(outputBuffers);
        }

        @Override
        public void addStateChangeListener(StateChangeListener<TaskInfo> stateChangeListener)
        {
            taskStateMachine.addStateChangeListener(newValue -> {
                if (newValue.isDone()) {
                    synchronized (this) {
                        splitCountChangeListener.splitCountChanged(-splits.size());
                        splits.clear();
                    }
                }
                stateChangeListener.stateChanged(getTaskInfo());
            });
        }

        @Override
        public CompletableFuture<TaskInfo> getStateChange(TaskInfo taskInfo)
        {
            return taskStateMachine.getStateChange(taskInfo.getState()).thenApply(ignored -> getTaskInfo());
        }

        @Override
        public void cancel()
        {
            taskStateMachine.cancel();
        }

        @Override
        public synchronized void abort()
        {
            taskStateMachine.abort();
            splitCountChangeListener.splitCountChanged(-splits.size());
            splits.clear();
        }

        @Override
        public synchronized int getPartitionedSplitCount()
        {
            if (taskStateMachine.getState().isDone()) {
                return 0;
            }
            return splits.size();
        }

        @Override
        public synchronized int getQueuedPartitionedSplitCount()
        {
            if (taskStateMachine.getState().isDone()) {
                return 0;
            }
            return splits.size();
        }
    }
}
