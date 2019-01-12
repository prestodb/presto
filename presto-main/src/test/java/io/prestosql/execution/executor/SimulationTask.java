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
package io.prestosql.execution.executor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import io.airlift.units.Duration;
import io.prestosql.execution.TaskId;
import io.prestosql.execution.executor.SimulationController.TaskSpecification;

import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

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

        public LeafTask(TaskExecutor taskExecutor, TaskSpecification specification, TaskId taskId)
        {
            super(taskExecutor, specification, taskId);
            this.taskSpecification = specification;
        }

        public void schedule(TaskExecutor taskExecutor, int numSplits)
        {
            ImmutableList.Builder<SimulationSplit> splits = ImmutableList.builder();
            for (int i = 0; i < numSplits; i++) {
                splits.add(taskSpecification.nextSpecification().instantiate(this));
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
