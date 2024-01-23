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
package com.facebook.presto.execution.executor;

import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.execution.ScheduledSplit;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TestSqlTaskExecution;
import com.facebook.presto.execution.executor.SimulationController.TaskSpecification;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import io.airlift.units.Duration;

import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.execution.TaskTestUtils.TABLE_SCAN_NODE_ID;
import static com.facebook.presto.spi.SplitContext.NON_CACHEABLE;
import static java.util.concurrent.TimeUnit.SECONDS;

abstract class SimulationTask
{
    private final TaskSpecification specification;
    private final TaskId taskId;

    private final Set<SimulationSplit> runningSplits = Sets.newConcurrentHashSet();
    private final Set<SimulationSplit> completedSplits = Sets.newConcurrentHashSet();

    private final TaskHandle taskHandle;
    private final AtomicBoolean killed = new AtomicBoolean();

    public SimulationTask(TaskExecutor taskExecutor, TaskSpecification specification, TaskId taskId)
    {
        this.specification = specification;
        this.taskId = taskId;
        taskHandle = taskExecutor.addTask(taskId, () -> 0, 10, new Duration(1, SECONDS), OptionalInt.empty());
    }

    public void setKilled()
    {
        killed.set(true);
    }

    public boolean isKilled()
    {
        return killed.get();
    }

    public Set<SimulationSplit> getCompletedSplits()
    {
        return completedSplits;
    }

    TaskId getTaskId()
    {
        return taskId;
    }

    public TaskHandle getTaskHandle()
    {
        return taskHandle;
    }

    public Set<SimulationSplit> getRunningSplits()
    {
        return runningSplits;
    }

    public synchronized void splitComplete(SimulationSplit split)
    {
        runningSplits.remove(split);
        completedSplits.add(split);
    }

    public TaskSpecification getSpecification()
    {
        return specification;
    }

    public long getTotalWaitTimeNanos()
    {
        long runningWaitTime = runningSplits.stream()
                .mapToLong(SimulationSplit::getWaitNanos)
                .sum();

        long completedWaitTime = completedSplits.stream()
                .mapToLong(SimulationSplit::getWaitNanos)
                .sum();

        return runningWaitTime + completedWaitTime;
    }

    public long getProcessedTimeNanos()
    {
        long runningProcessedTime = runningSplits.stream()
                .mapToLong(SimulationSplit::getCompletedProcessNanos)
                .sum();

        long completedProcessedTime = completedSplits.stream()
                .mapToLong(SimulationSplit::getCompletedProcessNanos)
                .sum();

        return runningProcessedTime + completedProcessedTime;
    }

    public long getScheduledTimeNanos()
    {
        long runningWallTime = runningSplits.stream()
                .mapToLong(SimulationSplit::getScheduledTimeNanos)
                .sum();

        long completedWallTime = completedSplits.stream()
                .mapToLong(SimulationSplit::getScheduledTimeNanos)
                .sum();

        return runningWallTime + completedWallTime;
    }

    public abstract void schedule(TaskExecutor taskExecutor, int numSplits);

    public static class LeafTask
            extends SimulationTask
    {
        private final TaskSpecification taskSpecification;
        private static final ConnectorId CONNECTOR_ID = new ConnectorId("test");
        private static final ConnectorTransactionHandle TRANSACTION_HANDLE = TestingTransactionHandle.create();

        public LeafTask(TaskExecutor taskExecutor, TaskSpecification specification, TaskId taskId)
        {
            super(taskExecutor, specification, taskId);
            this.taskSpecification = specification;
        }

        private ScheduledSplit newScheduledSplit(int sequenceId, PlanNodeId planNodeId, Lifespan lifespan, int begin, int count)
        {
            return new ScheduledSplit(sequenceId, planNodeId, new Split(CONNECTOR_ID, TRANSACTION_HANDLE, new TestSqlTaskExecution.TestingSplit(begin, begin + count), lifespan, NON_CACHEABLE));
        }

        public void schedule(TaskExecutor taskExecutor, int numSplits)
        {
            ImmutableList.Builder<SimulationSplit> splits = ImmutableList.builder();
            ImmutableList.Builder<ScheduledSplit> scheduledSplits = ImmutableList.builder();
            for (int i = 0; i < numSplits; i++) {
                splits.add(taskSpecification.nextSpecification().instantiate(this));
                // Simulation would not check the content of the scheduledSplit, just put placeholder data.
                scheduledSplits.add(newScheduledSplit(0, TABLE_SCAN_NODE_ID, Lifespan.taskWide(), 100000, 123));
            }
            super.runningSplits.addAll(splits.build());
            taskExecutor.enqueueSplits(getTaskHandle(), false, splits.build());
        }
    }

    public static class IntermediateTask
            extends SimulationTask
    {
        private final SplitSpecification splitSpecification;

        public IntermediateTask(TaskExecutor taskExecutor, TaskSpecification specification, TaskId taskId)
        {
            super(taskExecutor, specification, taskId);
            this.splitSpecification = specification.nextSpecification();
        }

        public void schedule(TaskExecutor taskExecutor, int numSplits)
        {
            ImmutableList.Builder<SimulationSplit> splits = ImmutableList.builder();
            for (int i = 0; i < numSplits; i++) {
                splits.add(splitSpecification.instantiate(this));
            }
            super.runningSplits.addAll(splits.build());
            taskExecutor.enqueueSplits(getTaskHandle(), true, splits.build());
        }
    }
}
