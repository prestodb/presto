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

package com.facebook.presto.common;

/**
 * Names for RuntimeMetrics used in the core presto code base.
 * Connectors could use arbitrary metric names not included in this class.
 */
public class RuntimeMetricName
{
    private RuntimeMetricName()
    {
    }

    public static final String DRIVER_COUNT_PER_TASK = "driverCountPerTask";
    public static final String TASK_ELAPSED_TIME_NANOS = "taskElapsedTimeNanos";
    public static final String OPTIMIZED_WITH_MATERIALIZED_VIEW = "optimizedWithMaterializedView";
    public static final String MANY_PARTITIONS_MISSING_IN_MATERIALIZED_VIEW = "manyPartitionsMissingInMaterializedView";
    public static final String SKIP_READING_FROM_MATERIALIZED_VIEW = "skipReadingFromMaterializedView";
    public static final String FRAGMENT_RESULT_CACHE_HIT = "fragmentResultCacheHitCount";
    public static final String FRAGMENT_RESULT_CACHE_MISS = "fragmentResultCacheMissCount";
    public static final String GET_VIEW_TIME_NANOS = "getViewTimeNanos";
    public static final String GET_MATERIALIZED_VIEW_TIME_NANOS = "getMaterializedViewTimeNanos";
    public static final String GET_TABLE_HANDLE_TIME_NANOS = "getTableHandleTimeNanos";
    public static final String GET_TABLE_METADATA_TIME_NANOS = "getTableMetadataTimeNanos";
    public static final String GET_SPLITS_TIME_NANOS = "getSplitsTimeNanos";
    public static final String LOGICAL_PLANNER_TIME_NANOS = "logicalPlannerTimeNanos";
    public static final String FRAGMENT_PLAN_TIME_NANOS = "fragmentPlanTimeNanos";
    public static final String GET_LAYOUT_TIME_NANOS = "getLayoutTimeNanos";
    public static final String REWRITE_AGGREGATION_IF_TO_FILTER_APPLIED = "rewriteAggregationIfToFilterApplied";
    // Time between task creation and start.
    public static final String TASK_QUEUED_TIME_NANOS = "taskQueuedTimeNanos";
    // Total operation time of a task on a worker. TASK_ELAPSED_TIME_NANOS - TASK_SCHEDULED_TIME_NANOS is the time when the task is doing nothing, e.g. it might be waiting for splits/inputs.
    public static final String TASK_SCHEDULED_TIME_NANOS = "taskScheduledTimeNanos";
    // Blocked time for the operators due to waiting for inputs.
    public static final String TASK_BLOCKED_TIME_NANOS = "taskBlockedTimeNanos";
    // Time taken for a read call to storage
    public static final String STORAGE_READ_TIME_NANOS = "storageReadTimeNanos";
    // Size of the data retrieved by read call to storage
    public static final String STORAGE_READ_DATA_BYTES = "storageReadDataBytes";
}
