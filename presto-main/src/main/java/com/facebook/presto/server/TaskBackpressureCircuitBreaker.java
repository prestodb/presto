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
package com.facebook.presto.server;

import com.facebook.airlift.stats.CounterStat;
import com.facebook.presto.execution.TaskManager;
import com.facebook.presto.execution.TaskManagerConfig;
import org.weakref.jmx.Managed;

import javax.inject.Inject;

import java.util.Optional;

public class TaskBackpressureCircuitBreaker
{
    private final boolean isTaskCreationBackpressureEnabled;
    private final int taskCreationBackpressureRetryAfterSeconds;
    private final int taskCreationTaskCountBackpressureThreshold;
    private final CounterStat taskCreationBackpressureByTaskCount = new CounterStat();
    private final TaskManager taskManager;

    @Inject
    public TaskBackpressureCircuitBreaker(TaskManagerConfig config, TaskManager taskManager)
    {
        this.taskManager = taskManager;
        isTaskCreationBackpressureEnabled = config.getTaskCreationBackpressureEnabled();
        taskCreationBackpressureRetryAfterSeconds = config.getTaskCreationBackpressureRetryAfterSeconds();
        taskCreationTaskCountBackpressureThreshold = config.getTaskCreationTaskCountBackpressureThreshold();
    }

    public Boolean shouldBackPressure(Optional<Boolean> firstUpdate)
    {
        if (!isTaskCreationBackpressureEnabled) {
            return false;
        }

        if (!firstUpdate.isPresent() || !firstUpdate.get()) {
            return false;
        }

        if (taskManager.getTaskCount() >= taskCreationTaskCountBackpressureThreshold) {
            taskCreationBackpressureByTaskCount.update(1);
            return true;
        }

        return false;
    }

    public int retryAfter()
    {
        //Adding jittering to retries
        Double retryAfter = taskCreationBackpressureRetryAfterSeconds * Math.random();
        return retryAfter.intValue();
    }

    @Managed
    public CounterStat getTaskCreationBackPressureByTaskCount()
    {
        return taskCreationBackpressureByTaskCount;
    }
}
