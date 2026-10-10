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
package com.facebook.presto.spark.execution;

import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.execution.StageExecutionTaskSummary;
import com.facebook.presto.execution.StageTaskStatsAggregator;
import com.facebook.presto.execution.TaskInfo;

/**
 * What the stage and query infos need from the tasks of a fragment, accumulated without retaining the tasks.
 * Not thread safe.
 */
public class FragmentTaskAggregates
{
    private final StageTaskStatsAggregator stageTaskStatsAggregator = new StageTaskStatsAggregator(new RuntimeStats());
    private final StageExecutionTaskSummary.Builder taskSummary = StageExecutionTaskSummary.builder();
    private final TaskMemoryStatsAccumulator taskMemoryStats = new TaskMemoryStatsAccumulator();

    public void addTask(TaskInfo taskInfo)
    {
        stageTaskStatsAggregator.addTask(taskInfo);
        taskSummary.addTask(taskInfo);
        taskMemoryStats.addTask(taskInfo);
    }

    public StageTaskStatsAggregator getStageTaskStatsAggregator()
    {
        return stageTaskStatsAggregator;
    }

    public StageExecutionTaskSummary.Builder getTaskSummary()
    {
        return taskSummary;
    }

    public TaskMemoryStatsAccumulator getTaskMemoryStats()
    {
        return taskMemoryStats;
    }
}
