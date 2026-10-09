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
import com.facebook.presto.execution.StageExecutionId;
import com.facebook.presto.execution.StageId;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskState;
import com.facebook.presto.operator.TaskStats;
import com.facebook.presto.spark.classloader_interface.SerializedTaskInfo;
import com.facebook.presto.spi.QueryId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.spark.util.CollectionAccumulator;

import java.net.URI;

import static com.facebook.presto.execution.TaskStatus.failWith;
import static com.facebook.presto.spark.execution.TaskInfoAggregator.createLockingTaskInfoCollectorForTesting;
import static com.facebook.presto.spark.execution.TaskInfoAggregator.isIncrementalAggregationSupported;

public final class TestingTaskInfos
{
    public static final QueryId QUERY_ID = new QueryId("test_query");
    public static final String SPARK_VERSION = org.apache.spark.package$.MODULE$.SPARK_VERSION();

    private static final URI LOCATION = URI.create("http://fake.invalid/task");
    private static final TaskStats EMPTY_TASK_STATS = new TaskStats(0L, 0L);

    private TestingTaskInfos() {}

    /**
     * @param marker identifies the attempt; stored as the node id
     */
    public static TaskInfo createTaskInfo(int fragmentId, int partitionId, int attemptNumber, TaskState state, String marker)
    {
        return createTaskInfo(fragmentId, partitionId, attemptNumber, state, marker, System.currentTimeMillis(), EMPTY_TASK_STATS);
    }

    public static TaskInfo createTaskInfo(int fragmentId, int partitionId, int attemptNumber, TaskState state, String marker, long lastHeartbeatInMillis, TaskStats taskStats)
    {
        TaskId taskId = new TaskId(new StageExecutionId(new StageId(QUERY_ID, fragmentId), 0), partitionId, attemptNumber);
        TaskInfo initialTaskInfo = TaskInfo.createInitialTask(taskId, LOCATION, ImmutableList.of(), taskStats, marker);
        return new TaskInfo(
                taskId,
                failWith(initialTaskInfo.getTaskStatus(), state, ImmutableList.of()),
                lastHeartbeatInMillis,
                initialTaskInfo.getOutputBuffers(),
                initialTaskInfo.getNoMoreSplits(),
                taskStats,
                false,
                marker);
    }

    public static TaskStats createTaskStats(long userMemoryReservationInBytes, long peakTotalMemoryInBytes, long peakUserMemoryInBytes, long peakNodeTotalMemoryInBytes)
    {
        return new TaskStats(
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0,
                0,
                0,
                0L,
                0,
                0,
                0L,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0.0,
                0.0,
                userMemoryReservationInBytes,
                0L,
                0L,
                peakTotalMemoryInBytes,
                peakUserMemoryInBytes,
                peakNodeTotalMemoryInBytes,
                0L,
                0L,
                0L,
                false,
                ImmutableSet.of(),
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0,
                0L,
                ImmutableList.of(),
                new RuntimeStats());
    }

    /**
     * Returns a task info collector that can be drained atomically: the production one on Spark 3, and one that
     * reproduces its locking on Spark 2.
     */
    public static CollectionAccumulator<SerializedTaskInfo> createTaskInfoCollector()
    {
        if (isIncrementalAggregationSupported(SPARK_VERSION)) {
            return new CollectionAccumulator<>();
        }
        return createLockingTaskInfoCollectorForTesting();
    }
}
