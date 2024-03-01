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
package com.facebook.presto.execution.scheduler;

import com.facebook.presto.execution.PartialResultQueryManager;
import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.spi.PrestoWarning;
import com.facebook.presto.spi.WarningCollector;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.spi.StandardWarningCode.PARTIAL_RESULT_WARNING;
import static com.google.common.collect.Sets.SetView;
import static com.google.common.collect.Sets.difference;
import static com.google.common.collect.Sets.newConcurrentHashSet;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class PartialResultQueryTaskTracker
{
    private final PartialResultQueryManager partialResultQueryManager;
    private final double minCompletionRatioThreshold;
    private final double timeMultiplier;
    private final WarningCollector warningCollector;
    private final long startTime;
    private final Map<TaskId, RemoteTask> taskIdMap = new HashMap<>();
    private final Set<TaskId> completedTaskIds = newConcurrentHashSet();
    private final AtomicBoolean addedToQueryManager = new AtomicBoolean();

    private long maxEndTime;
    private boolean taskSchedulingCompleted;

    public PartialResultQueryTaskTracker(
            PartialResultQueryManager partialResultQueryManager,
            double minCompletionRatioThreshold,
            double timeMultiplier,
            WarningCollector warningCollector)
    {
        this.partialResultQueryManager = requireNonNull(partialResultQueryManager, "partialResultQueryManager is null");
        this.minCompletionRatioThreshold = minCompletionRatioThreshold;
        this.timeMultiplier = timeMultiplier;
        this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
        this.startTime = System.nanoTime();
    }

    public double getTaskCompletionRatio()
    {
        if (completedTaskIds.isEmpty() || taskIdMap.isEmpty()) {
            return 0.0;
        }
        return (double) completedTaskIds.size() / (double) taskIdMap.size();
    }

    private void checkAndAddToQueryManager()
    {
        // If completion ratio greater than equal to threshold, then query is eligible for partial results
        if (taskSchedulingCompleted && getTaskCompletionRatio() >= minCompletionRatioThreshold && addedToQueryManager.compareAndSet(false, true)) {
            // Set max task time = timeMultiplier x time taken to reach minCompletionRatioThreshold
            long elapsedTime = System.nanoTime() - startTime;
            maxEndTime = startTime + (long) (timeMultiplier * elapsedTime);

            partialResultQueryManager.addQueryTaskTracker(this);
        }
    }

    public void trackTask(RemoteTask task)
    {
        taskIdMap.put(task.getTaskId(), task);
    }

    public void recordTaskFinish(TaskInfo taskInfo)
    {
        completedTaskIds.add(taskInfo.getTaskId());
        checkAndAddToQueryManager();
    }

    public long getMaxEndTime()
    {
        return maxEndTime;
    }

    public void completeTaskScheduling()
    {
        this.taskSchedulingCompleted = true;
    }

    public void cancelUnfinishedTasks()
    {
        SetView<TaskId> pendingTaskIds = difference(taskIdMap.keySet(), completedTaskIds);
        double partialResultPercentage = getTaskCompletionRatio() * 100;
        for (TaskId taskId : pendingTaskIds) {
            RemoteTask pendingTask = taskIdMap.get(taskId);
            // Cancel pending tasks
            pendingTask.cancel();
        }
        if (!pendingTaskIds.isEmpty()) {
            String warningMessage = format("Partial results are returned. Only %.2f percent of the data is read.", partialResultPercentage);
            warningCollector.add(new PrestoWarning(PARTIAL_RESULT_WARNING, warningMessage));
        }
    }
}
