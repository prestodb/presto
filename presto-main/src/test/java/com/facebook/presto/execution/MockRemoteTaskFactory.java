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
import com.facebook.presto.metadata.ColumnHandle;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.TestingColumnHandle;
import com.facebook.presto.sql.planner.TestingTableHandle;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import javax.annotation.concurrent.GuardedBy;

import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.execution.StateMachine.StateChangeListener;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.util.Failures.toFailures;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class MockRemoteTaskFactory
        implements RemoteTaskFactory
{
    private final Executor executor;

    MockRemoteTaskFactory(Executor executor)
    {
        this.executor = executor;
    }

    public RemoteTask createTableScanTask(Node newNode, List<Split> splits)
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
                        ImmutableMap.of(symbol, new ColumnHandle("test", new TestingColumnHandle("column"))),
                        null,
                        Optional.empty()),
                ImmutableMap.<Symbol, Type>of(symbol, VARCHAR),
                ImmutableList.of(symbol),
                PlanFragment.PlanDistribution.SOURCE,
                tableScanNodeId,
                PlanFragment.OutputPartitioning.NONE,
                ImmutableList.<Symbol>of(),
                Optional.empty()
        );

        ImmutableMultimap.Builder<PlanNodeId, Split> initialSplits = ImmutableMultimap.builder();
        for (Split sourceSplit : splits) {
            initialSplits.put(sourceId, sourceSplit);
        }
        return createRemoteTask(TEST_SESSION, taskId, newNode, testFragment, initialSplits.build(), OutputBuffers.INITIAL_EMPTY_OUTPUT_BUFFERS);
    }

    @Override
    public RemoteTask createRemoteTask(
            Session session,
            TaskId taskId,
            Node node,
            PlanFragment fragment,
            Multimap<PlanNodeId, Split> initialSplits,
            OutputBuffers outputBuffers)
    {
        return new MockRemoteTask(taskId, fragment, node.getNodeIdentifier(), executor, initialSplits);
    }

    private class MockRemoteTask
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

        public MockRemoteTask(TaskId taskId,
                PlanFragment fragment,
                String nodeId,
                Executor executor,
                Multimap<PlanNodeId, Split> initialSplits)
        {
            this.taskStateMachine = new TaskStateMachine(checkNotNull(taskId, "taskId is null"), checkNotNull(executor, "executor is null"));

            this.taskContext = new TaskContext(taskStateMachine, executor, TEST_SESSION, new DataSize(256, MEGABYTE), new DataSize(1, MEGABYTE), true, true);

            this.location = URI.create("fake://task/" + taskId);

            this.sharedBuffer = new SharedBuffer(taskId, executor, checkNotNull(new DataSize(1, DataSize.Unit.BYTE), "maxBufferSize is null"));
            this.fragment = checkNotNull(fragment, "fragment is null");
            this.nodeId = checkNotNull(nodeId, "nodeId is null");
            splits.putAll(initialSplits);
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

        @Override
        public void start()
        {
        }

        @Override
        public void addSplits(PlanNodeId sourceId, Iterable<Split> splits)
        {
            checkNotNull(splits, "splits is null");
            for (Split split : splits) {
                this.splits.put(sourceId, split);
            }
        }

        @Override
        public void noMoreSplits(PlanNodeId sourceId)
        {
            noMoreSplits.add(sourceId);
            if (noMoreSplits.containsAll(fragment.getSourceIds())) {
                taskStateMachine.finished();
            }
        }

        @Override
        public void setOutputBuffers(OutputBuffers outputBuffers)
        {
            sharedBuffer.setOutputBuffers(outputBuffers);
        }

        @Override
        public void addStateChangeListener(final StateChangeListener<TaskInfo> stateChangeListener)
        {
            taskStateMachine.addStateChangeListener(new StateChangeListener<TaskState>()
            {
                @Override
                public void stateChanged(TaskState newValue)
                {
                    stateChangeListener.stateChanged(getTaskInfo());
                }
            });
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
        }

        @Override
        public int getPartitionedSplitCount()
        {
            if (taskStateMachine.getState().isDone()) {
                return 0;
            }
            return splits.size();
        }

        @Override
        public int getQueuedPartitionedSplitCount()
        {
            if (taskStateMachine.getState().isDone()) {
                return 0;
            }
            return splits.size();
        }

        @Override
        public Duration waitForTaskToFinish(Duration maxWait)
                throws InterruptedException
        {
            while (true) {
                TaskState currentState = taskStateMachine.getState();
                if (maxWait.toMillis() <= 1 || currentState.isDone()) {
                    return maxWait;
                }
                maxWait = taskStateMachine.waitForStateChange(currentState, maxWait);
            }
        }
    }
}
