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

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.facebook.presto.common.RuntimeUnit.NONE;
import static java.util.Objects.requireNonNull;

/**
 * Keys for RuntimeMetrics used in the core presto code base.
 * Connectors could use arbitrary metric keys not included in this class.
 */
@ThriftStruct
public class RuntimeMetricKey
{
    public static final RuntimeMetricKey DRIVER_COUNT_PER_TASK = new RuntimeMetricKey("driverCountPerTask");
    public static final RuntimeMetricKey TASK_ELAPSED_TIME_NANOS = new RuntimeMetricKey("taskElapsedTimeNanos");
    public static final RuntimeMetricKey OPTIMIZED_WITH_MATERIALIZED_VIEW_COUNT = new RuntimeMetricKey("optimizedWithMaterializedViewCount");
    public static final RuntimeMetricKey MANY_PARTITIONS_MISSING_IN_MATERIALIZED_VIEW_COUNT = new RuntimeMetricKey("manyPartitionsMissingInMaterializedViewCount");
    public static final RuntimeMetricKey SKIP_READING_FROM_MATERIALIZED_VIEW_COUNT = new RuntimeMetricKey("skipReadingFromMaterializedViewCount");
    public static final RuntimeMetricKey FRAGMENT_RESULT_CACHE_HIT = new RuntimeMetricKey("fragmentResultCacheHitCount");
    public static final RuntimeMetricKey FRAGMENT_RESULT_CACHE_MISS = new RuntimeMetricKey("fragmentResultCacheMissCount");
    public static final RuntimeMetricKey GET_VIEW_TIME_NANOS = new RuntimeMetricKey("getViewTimeNanos");
    public static final RuntimeMetricKey GET_MATERIALIZED_VIEW_TIME_NANOS = new RuntimeMetricKey("getMaterializedViewTimeNanos");
    public static final RuntimeMetricKey GET_MATERIALIZED_VIEW_STATUS_TIME_NANOS = new RuntimeMetricKey("getMaterializedViewStatusTimeNanos");
    public static final RuntimeMetricKey GET_TABLE_HANDLE_TIME_NANOS = new RuntimeMetricKey("getTableHandleTimeNanos");
    public static final RuntimeMetricKey GET_TABLE_METADATA_TIME_NANOS = new RuntimeMetricKey("getTableMetadataTimeNanos");
    public static final RuntimeMetricKey GET_SPLITS_TIME_NANOS = new RuntimeMetricKey("getSplitsTimeNanos");
    public static final RuntimeMetricKey LOGICAL_PLANNER_TIME_NANOS = new RuntimeMetricKey("logicalPlannerTimeNanos");
    public static final RuntimeMetricKey FRAGMENT_PLAN_TIME_NANOS = new RuntimeMetricKey("fragmentPlanTimeNanos");
    public static final RuntimeMetricKey GET_LAYOUT_TIME_NANOS = new RuntimeMetricKey("getLayoutTimeNanos");
    public static final RuntimeMetricKey REWRITE_AGGREGATION_IF_TO_FILTER_APPLIED = new RuntimeMetricKey("rewriteAggregationIfToFilterApplied");
    // Time between task creation and start.
    public static final RuntimeMetricKey TASK_QUEUED_TIME_NANOS = new RuntimeMetricKey("taskQueuedTimeNanos");
    // Total operation time of a task on a worker. TASK_ELAPSED_TIME_NANOS - TASK_SCHEDULED_TIME_NANOS is the time when the task is doing nothing, e.g. it might be waiting for splits/inputs.
    public static final RuntimeMetricKey TASK_SCHEDULED_TIME_NANOS = new RuntimeMetricKey("taskScheduledTimeNanos");
    // Blocked time for the operators due to waiting for inputs.
    public static final RuntimeMetricKey TASK_BLOCKED_TIME_NANOS = new RuntimeMetricKey("taskBlockedTimeNanos");
    // Time taken for a read call to storage
    public static final RuntimeMetricKey STORAGE_READ_TIME_NANOS = new RuntimeMetricKey("storageReadTimeNanos");
    // Size of the data retrieved by read call to storage
    public static final RuntimeMetricKey STORAGE_READ_DATA_BYTES = new RuntimeMetricKey("storageReadDataBytes");

    private final String name;
    private final RuntimeUnit unit;

    public RuntimeMetricKey(String name)
    {
        this(name, NONE);
    }

    @JsonCreator
    @ThriftConstructor
    public RuntimeMetricKey(@JsonProperty("name") String name, @JsonProperty("unit") RuntimeUnit unit)
    {
        this.name = requireNonNull(name);
        this.unit = requireNonNull(unit);
    }

    @JsonProperty
    @ThriftField(1)
    public String getName()
    {
        return name;
    }

    @JsonProperty
    @ThriftField(2)
    public RuntimeUnit getUnit()
    {
        return unit;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RuntimeMetricKey that = (RuntimeMetricKey) o;
        return Objects.equals(name, that.name) && unit == that.unit;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, unit);
    }
}
