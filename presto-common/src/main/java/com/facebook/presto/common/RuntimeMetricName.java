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
    public static final String OPTIMIZED_WITH_MATERIALIZED_VIEW_COUNT = "optimizedWithMaterializedViewCount";
    public static final String OPTIMIZED_WITH_MATERIALIZED_VIEW_SUBQUERY_COUNT = "optimizedWithMaterializedViewSubqueryCount";
    public static final String MANY_PARTITIONS_MISSING_IN_MATERIALIZED_VIEW_COUNT = "manyPartitionsMissingInMaterializedViewCount";
    public static final String SKIP_READING_FROM_MATERIALIZED_VIEW_COUNT = "skipReadingFromMaterializedViewCount";
    public static final String FRAGMENT_RESULT_CACHE_HIT = "fragmentResultCacheHitCount";
    public static final String FRAGMENT_RESULT_CACHE_MISS = "fragmentResultCacheMissCount";
    public static final String GET_VIEW_TIME_NANOS = "getViewTimeNanos";
    public static final String GET_MATERIALIZED_VIEW_TIME_NANOS = "getMaterializedViewTimeNanos";
    public static final String GET_MATERIALIZED_VIEW_STATUS_TIME_NANOS = "getMaterializedViewStatusTimeNanos";
    public static final String GET_TABLE_HANDLE_TIME_NANOS = "getTableHandleTimeNanos";
    public static final String GET_COLUMN_HANDLE_TIME_NANOS = "getColumnHandleTimeNanos";
    public static final String GET_COLUMN_METADATA_TIME_NANOS = "getColumnMetadataTimeNanos";
    public static final String GET_PARTITIONS_BY_NAMES_TIME_NANOS = "getPartitionsByNamesTimeNanos";
    public static final String GET_TABLE_TIME_NANOS = "getTableTimeNanos";
    public static final String GET_SPLITS_TIME_NANOS = "getSplitsTimeNanos";
    // CPU time taken to schedule a given stage
    public static final String SCHEDULER_CPU_TIME_NANOS = "schedulerCpuTimeNanos";
    // Wall time taken to schedule a given stage
    public static final String SCHEDULER_WALL_TIME_NANOS = "schedulerWallTimeNanos";
    // Blocked time of the scheduler during scheduling a given stage
    public static final String SCHEDULER_BLOCKED_TIME_NANOS = "schedulerBlockedTimeNanos";
    public static final String SCAN_STAGE_SCHEDULER_CPU_TIME_NANOS = "scanStageSchedulerCpuTimeNanos";
    public static final String SCAN_STAGE_SCHEDULER_WALL_TIME_NANOS = "scanStageSchedulerWallTimeNanos";
    public static final String SCAN_STAGE_SCHEDULER_BLOCKED_TIME_NANOS = "scanStageSchedulerBlockedTimeNanos";
    public static final String ANALYZE_TIME_NANOS = "analyzeTimeNanos";
    public static final String PLAN_AND_OPTIMIZE_TIME_NANOS = "planAndOptimizeTimeNanos";
    public static final String CREATE_SCHEDULER_TIME_NANOS = "createSchedulerTimeNanos";
    public static final String LOGICAL_PLANNER_TIME_NANOS = "logicalPlannerTimeNanos";
    public static final String OPTIMIZER_TIME_NANOS = "optimizerTimeNanos";
    public static final String VALIDATE_FINAL_PLAN_TIME_NANOS = "validateFinalPlanTimeNanos";
    public static final String VALIDATE_INTERMEDIATE_PLAN_TIME_NANOS = "validateIntermediatePlanTimeNanos";
    public static final String GET_CANONICAL_INFO_TIME_NANOS = "getCanonicalInfoTimeNanos";
    public static final String FRAGMENT_PLAN_TIME_NANOS = "fragmentPlanTimeNanos";
    public static final String GET_LAYOUT_TIME_NANOS = "getLayoutTimeNanos";
    public static final String GET_IDENTIFIER_NORMALIZATION_TIME_NANOS = "getIdentifierNormalizationTimeNanos";
    public static final String REWRITE_AGGREGATION_IF_TO_FILTER_APPLIED = "rewriteAggregationIfToFilterApplied";
    // Time between task creation and start.
    public static final String TASK_QUEUED_TIME_NANOS = "taskQueuedTimeNanos";
    // Total operation time of a task on a worker. TASK_ELAPSED_TIME_NANOS - TASK_SCHEDULED_TIME_NANOS is the time when the task is doing nothing, e.g. it might be waiting for splits/inputs.
    public static final String TASK_SCHEDULED_TIME_NANOS = "taskScheduledTimeNanos";
    // Blocked time for the operators due to waiting for inputs.
    public static final String TASK_BLOCKED_TIME_NANOS = "taskBlockedTimeNanos";
    public static final String TASK_UPDATE_DELIVERED_WALL_TIME_NANOS = "taskUpdateDeliveredWallTimeNanos";
    public static final String TASK_START_WAIT_FOR_EVENT_LOOP = "taskStartWaitForEventLoop";
    public static final String TASK_UPDATE_DELIVERED_UPDATES = "taskUpdateDeliveredUpdates";
    public static final String TASK_UPDATE_ROUND_TRIP_TIME = "taskUpdateRoundTripTime";
    public static final String TASK_UPDATE_SERIALIZED_CPU_TIME_NANOS = "taskUpdateSerializedCpuNanos";
    public static final String TASK_PLAN_SERIALIZED_CPU_TIME_NANOS = "taskPlanSerializedCpuNanos";
    // Time for event loop to execute a method
    public static final String EVENT_LOOP_METHOD_EXECUTION_CPU_TIME_NANOS = "eventLoopMethodExecutionCpuNanos";
    // Time taken for a read call to storage
    public static final String STORAGE_READ_TIME_NANOS = "storageReadTimeNanos";
    // Size of the data retrieved by read call to storage
    public static final String STORAGE_READ_DATA_BYTES = "storageReadDataBytes";
    public static final String WRITTEN_FILES_COUNT = "writtenFilesCount";
    public static final String HISTORY_OPTIMIZER_QUERY_REGISTRATION_GET_PLAN_NODE_HASHES = "historyOptimizerQueryRegistrationGetPlanNodeHashes";
    public static final String HISTORY_OPTIMIZER_QUERY_REGISTRATION_GET_STATISTICS = "historyOptimizerQueryRegistrationGetStatistics";
    public static final String DIRECTORY_LISTING_CACHE_HIT = "directoryListingCacheHit";
    public static final String DIRECTORY_LISTING_CACHE_MISS = "directoryListingCacheMiss";
    public static final String DIRECTORY_LISTING_TIME_NANOS = "directoryListingTimeNanos";
    public static final String FILES_READ_COUNT = "filesReadCount";

    // Dynamic filter metrics
    public static final String DYNAMIC_FILTER_SPLITS_PROCESSED = "dynamicFilterSplitsProcessed";
    public static final String DYNAMIC_FILTER_WAIT_TIME_NANOS = "dynamicFilterWaitTimeNanos";
    public static final String DYNAMIC_FILTER_COLLECTION_TIME_NANOS = "dynamicFilterCollectionTimeNanos";
    public static final String DYNAMIC_FILTER_PARTITIONS_RECEIVED = "dynamicFilterPartitionsReceived";
    public static final String DYNAMIC_FILTER_PUSHED_INTO_SCAN = "dynamicFilterPushedIntoScan";
    public static final String DYNAMIC_FILTER_SPLITS_BEFORE_FILTER = "dynamicFilterSplitsBeforeFilter";
    public static final String DYNAMIC_FILTER_CONSTRAINT_COLUMNS = "dynamicFilterConstraintColumns";
    // Dynamic filter diagnostic metrics
    public static final String DYNAMIC_FILTER_EXPECTED_PARTITIONS = "dynamicFilterExpectedPartitions";
    public static final String DYNAMIC_FILTER_FETCHERS_STARTED = "dynamicFilterFetchersStarted";
    // Dynamic filter source operator metrics
    public static final String DYNAMIC_FILTER_SOURCE_DISTINCT_VALUES = "dynamicFilterSourceDistinctValues";
    public static final String DYNAMIC_FILTER_SOURCE_FALLBACK_TO_MIN_MAX = "dynamicFilterSourceFallbackToMinMax";
    public static final String DYNAMIC_FILTER_SOURCE_COLLECTION_TIME_NANOS = "dynamicFilterSourceCollectionTimeNanos";
    public static final String DYNAMIC_FILTER_TIMED_OUT = "dynamicFilterTimedOut";
    public static final String DYNAMIC_FILTER_DOMAIN_RANGE_COUNT = "dynamicFilterDomainRangeCount";
    public static final String DYNAMIC_FILTER_COORDINATOR_FALLBACK_TO_RANGE = "dynamicFilterCoordinatorFallbackToRange";
    public static final String DYNAMIC_FILTER_SHORT_CIRCUITED = "dynamicFilterShortCircuited";
    public static final String DYNAMIC_FILTER_SPLITS_WITHOUT_FILTER = "dynamicFilterSplitsWithoutFilter";
    public static final String DYNAMIC_FILTER_FETCHER_POLLS = "dynamicFilterFetcherPolls";
    public static final String DYNAMIC_FILTER_FETCHER_STOPPED_BY_CLEANUP = "dynamicFilterFetcherStoppedByCleanup";
    public static final String DYNAMIC_FILTER_FETCHER_FINAL_FETCH_COMPLETED = "dynamicFilterFetcherFinalFetchCompleted";
    public static final String DYNAMIC_FILTER_PARTITIONS_RECEIVED_FROM_TASK = "dynamicFilterPartitionsReceivedFromTask";
    public static final String DYNAMIC_FILTER_OPERATOR_CALLBACK = "dynamicFilterOperatorCallback";
    public static final String DYNAMIC_FILTER_FACTORY_CLOSED = "dynamicFilterFactoryClosed";
    public static final String DYNAMIC_FILTER_FLUSH_FIRED = "dynamicFilterFlushFired";
    public static final String DYNAMIC_FILTER_COMPLETED_ID_DELIVERED = "dynamicFilterCompletedIdDelivered";
    public static final String DYNAMIC_FILTER_PLAN_CREATED_PARTITION_FALLBACK = "dynamicFilterPlanCreatedPartitionFallback";
    public static final String DYNAMIC_FILTER_PLAN_SKIPPED_NOT_PARTITION_COLUMN = "dynamicFilterPlanSkippedNotPartitionColumn";
    public static final String DYNAMIC_FILTER_PLAN_SKIPPED_BUILD_COVERS_PROBE = "dynamicFilterPlanSkippedBuildCoversProbe";
    public static final String DYNAMIC_FILTER_PLAN_CREATED_LOW_NDV = "dynamicFilterPlanCreatedLowNdv";
    public static final String DYNAMIC_FILTER_PLAN_CREATED_FAVORABLE_RATIO = "dynamicFilterPlanCreatedFavorableRatio";
    public static final String DYNAMIC_FILTER_PLAN_SKIPPED_HIGH_CARDINALITY = "dynamicFilterPlanSkippedHighCardinality";
    // Speculative split enumeration metrics (always emitted when DPP is active)
    public static final String DYNAMIC_FILTER_SPLITS_SPECULATIVELY_BUFFERED = "dynamicFilterSplitsSpeculativelyBuffered";
    public static final String DYNAMIC_FILTER_SPLITS_RETROACTIVELY_PRUNED = "dynamicFilterSplitsRetroactivelyPruned";
    public static final String DYNAMIC_FILTER_SPECULATIVE_BUFFER_OVERFLOW = "dynamicFilterSpeculativeBufferOverflow";
    // Column selectivity check metrics (extended metrics only)
    public static final String DYNAMIC_FILTER_COLUMNS_SKIPPED = "dynamicFilterColumnsSkipped";
    public static final String DYNAMIC_FILTER_COLUMNS_RELEVANT = "dynamicFilterColumnsRelevant";
}
