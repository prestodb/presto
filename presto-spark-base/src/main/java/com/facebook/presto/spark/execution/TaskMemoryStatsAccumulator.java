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

import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.operator.TaskStats;

import static java.lang.Math.max;

/**
 * The task count and memory statistics that {@code PrestoSparkQueryExecutionFactory.createStageInfo} and
 * {@code createQueryInfo} compute from the task infos of each stage, accumulated one task at a time.
 */
public final class TaskMemoryStatsAccumulator
{
    private int taskCount;
    private long totalPeakUserMemoryInBytes;
    private long maxPeakUserMemoryInBytes;
    private long totalPeakTotalMemoryInBytes;
    private long maxPeakTotalMemoryInBytes;
    private long maxPeakNodeTotalMemoryInBytes;
    private long totalUserMemoryReservationInBytes;

    public void addTask(TaskInfo taskInfo)
    {
        TaskStats stats = taskInfo.getStats();
        taskCount++;
        totalPeakUserMemoryInBytes += stats.getPeakUserMemoryInBytes();
        maxPeakUserMemoryInBytes = max(maxPeakUserMemoryInBytes, stats.getPeakUserMemoryInBytes());
        totalPeakTotalMemoryInBytes += stats.getPeakTotalMemoryInBytes();
        maxPeakTotalMemoryInBytes = max(maxPeakTotalMemoryInBytes, stats.getPeakTotalMemoryInBytes());
        maxPeakNodeTotalMemoryInBytes = max(maxPeakNodeTotalMemoryInBytes, stats.getPeakNodeTotalMemoryInBytes());
        totalUserMemoryReservationInBytes += stats.getUserMemoryReservationInBytes();
    }

    /**
     * Adds the tasks accumulated by {@code other}, for example to combine the stages of a query.
     */
    public void merge(TaskMemoryStatsAccumulator other)
    {
        taskCount += other.taskCount;
        totalPeakUserMemoryInBytes += other.totalPeakUserMemoryInBytes;
        maxPeakUserMemoryInBytes = max(maxPeakUserMemoryInBytes, other.maxPeakUserMemoryInBytes);
        totalPeakTotalMemoryInBytes += other.totalPeakTotalMemoryInBytes;
        maxPeakTotalMemoryInBytes = max(maxPeakTotalMemoryInBytes, other.maxPeakTotalMemoryInBytes);
        maxPeakNodeTotalMemoryInBytes = max(maxPeakNodeTotalMemoryInBytes, other.maxPeakNodeTotalMemoryInBytes);
        totalUserMemoryReservationInBytes += other.totalUserMemoryReservationInBytes;
    }

    /**
     * The total task count of a stage, and the peak running task count of a query.
     */
    public int getTaskCount()
    {
        return taskCount;
    }

    /**
     * The peak user memory reservation of a query.
     */
    public long getTotalPeakUserMemoryInBytes()
    {
        return totalPeakUserMemoryInBytes;
    }

    /**
     * The peak task user memory of a query.
     */
    public long getMaxPeakUserMemoryInBytes()
    {
        return maxPeakUserMemoryInBytes;
    }

    /**
     * The peak total memory reservation of a query.
     */
    public long getTotalPeakTotalMemoryInBytes()
    {
        return totalPeakTotalMemoryInBytes;
    }

    /**
     * The peak task total memory of a query.
     */
    public long getMaxPeakTotalMemoryInBytes()
    {
        return maxPeakTotalMemoryInBytes;
    }

    /**
     * The peak node total memory of a stage or a query.
     */
    public long getMaxPeakNodeTotalMemoryInBytes()
    {
        return maxPeakNodeTotalMemoryInBytes;
    }

    /**
     * The sum of the last reported user memory reservation of each task. It is passed as the peak user memory
     * reservation of the stage, as the stage info is built from the task infos.
     */
    public long getTotalUserMemoryReservationInBytes()
    {
        return totalUserMemoryReservationInBytes;
    }
}
