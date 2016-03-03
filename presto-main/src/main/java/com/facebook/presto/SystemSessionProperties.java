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
package com.facebook.presto;

import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.execution.TaskManagerConfig;
import com.facebook.presto.memory.MemoryManagerConfig;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.inject.Inject;

import java.util.List;

import static com.facebook.presto.spi.session.PropertyMetadata.booleanSessionProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.integerSessionProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.stringSessionProperty;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public final class SystemSessionProperties
{
    public static final String OPTIMIZE_HASH_GENERATION = "optimize_hash_generation";
    public static final String DISTRIBUTED_JOIN = "distributed_join";
    public static final String DISTRIBUTED_INDEX_JOIN = "distributed_index_join";
    public static final String HASH_PARTITION_COUNT = "hash_partition_count";
    public static final String PREFER_STREAMING_OPERATORS = "prefer_streaming_operators";
    public static final String TASK_WRITER_COUNT = "task_writer_count";
    public static final String TASK_JOIN_CONCURRENCY = "task_join_concurrency";
    public static final String TASK_HASH_BUILD_CONCURRENCY = "task_hash_build_concurrency";
    public static final String TASK_AGGREGATION_CONCURRENCY = "task_aggregation_concurrency";
    public static final String TASK_INTERMEDIATE_AGGREGATION = "task_intermediate_aggregation";
    public static final String TASK_SHARE_INDEX_LOADING = "task_share_index_loading";
    public static final String QUERY_MAX_MEMORY = "query_max_memory";
    public static final String QUERY_MAX_RUN_TIME = "query_max_run_time";
    public static final String RESOURCE_OVERCOMMIT = "resource_overcommit";
    public static final String REDISTRIBUTE_WRITES = "redistribute_writes";
    public static final String PUSH_TABLE_WRITE_THROUGH_UNION = "push_table_write_through_union";
    public static final String EXECUTION_POLICY = "execution_policy";
    public static final String COLUMNAR_PROCESSING = "columnar_processing";
    public static final String COLUMNAR_PROCESSING_DICTIONARY = "columnar_processing_dictionary";
    public static final String DICTIONARY_AGGREGATION = "dictionary_aggregation";
    public static final String PLAN_WITH_TABLE_NODE_PARTITIONING = "plan_with_table_node_partitioning";
    public static final String INITIAL_SPLITS_PER_NODE = "initial_splits_per_node";
    public static final String SPLIT_CONCURRENCY_ADJUSTMENT_INTERVAL = "split_concurrency_adjustment_interval";

    private final List<PropertyMetadata<?>> sessionProperties;

    public SystemSessionProperties()
    {
        this(new QueryManagerConfig(), new TaskManagerConfig(), new MemoryManagerConfig(), new FeaturesConfig());
    }

    @Inject
    public SystemSessionProperties(
            QueryManagerConfig queryManagerConfig,
            TaskManagerConfig taskManagerConfig,
            MemoryManagerConfig memoryManagerConfig,
            FeaturesConfig featuresConfig)
    {
        sessionProperties = ImmutableList.of(
                stringSessionProperty(
                        EXECUTION_POLICY,
                        "Policy used for scheduling query tasks",
                        queryManagerConfig.getQueryExecutionPolicy(),
                        false),
                booleanSessionProperty(
                        OPTIMIZE_HASH_GENERATION,
                        "Compute hash codes for distribution, joins, and aggregations early in query plan",
                        featuresConfig.isOptimizeHashGeneration(),
                        false),
                booleanSessionProperty(
                        DISTRIBUTED_JOIN,
                        "Use a distributed join instead of a broadcast join",
                        featuresConfig.isDistributedJoinsEnabled(),
                        false),
                booleanSessionProperty(
                        DISTRIBUTED_INDEX_JOIN,
                        "Distribute index joins on join keys instead of executing inline",
                        featuresConfig.isDistributedIndexJoinsEnabled(),
                        false),
                integerSessionProperty(
                        HASH_PARTITION_COUNT,
                        "Number of partitions for distributed joins and aggregations",
                        queryManagerConfig.getInitialHashPartitions(),
                        false),
                booleanSessionProperty(
                        PREFER_STREAMING_OPERATORS,
                        "Prefer source table layouts that produce streaming operators",
                        false,
                        false),
                integerSessionProperty(
                        TASK_WRITER_COUNT,
                        "Default number of local parallel table writer jobs per worker",
                        taskManagerConfig.getWriterCount(),
                        false),
                booleanSessionProperty(
                        REDISTRIBUTE_WRITES,
                        "Force parallel distributed writes",
                        featuresConfig.isRedistributeWrites(),
                        false),
                booleanSessionProperty(
                        PUSH_TABLE_WRITE_THROUGH_UNION,
                        "Parallelize writes when using UNION ALL in queries that write data",
                        featuresConfig.isPushTableWriteThroughUnion(),
                        false),
                integerSessionProperty(
                        TASK_JOIN_CONCURRENCY,
                        "Experimental: Default number of local parallel join jobs per worker",
                        taskManagerConfig.getTaskJoinConcurrency(),
                        false),
                integerSessionProperty(
                        TASK_HASH_BUILD_CONCURRENCY,
                        "Experimental: Default number of local parallel hash build jobs per worker",
                        taskManagerConfig.getTaskDefaultConcurrency(),
                        false),
                integerSessionProperty(
                        TASK_AGGREGATION_CONCURRENCY,
                        "Experimental: Default number of local parallel aggregation jobs per worker",
                        taskManagerConfig.getTaskDefaultConcurrency(),
                        false),
                booleanSessionProperty(
                        TASK_INTERMEDIATE_AGGREGATION,
                        "Experimental: add intermediate aggregation jobs per worker",
                        featuresConfig.isIntermediateAggregationsEnabled(),
                        false),
                booleanSessionProperty(
                        TASK_SHARE_INDEX_LOADING,
                        "Share index join lookups and caching within a task",
                        taskManagerConfig.isShareIndexLoading(),
                        false),
                new PropertyMetadata<>(
                        QUERY_MAX_RUN_TIME,
                        "Maximum run time of a query",
                        VARCHAR,
                        Duration.class,
                        queryManagerConfig.getQueryMaxRunTime(),
                        false,
                        value -> Duration.valueOf((String) value)),
                new PropertyMetadata<>(
                        QUERY_MAX_MEMORY,
                        "Maximum amount of distributed memory a query can use",
                        VARCHAR,
                        DataSize.class,
                        memoryManagerConfig.getMaxQueryMemory(),
                        true,
                        value -> DataSize.valueOf((String) value)),
                booleanSessionProperty(
                        RESOURCE_OVERCOMMIT,
                        "Use resources which are not guaranteed to be available to the query",
                        false,
                        false),
                booleanSessionProperty(
                        COLUMNAR_PROCESSING,
                        "Use columnar processing",
                        featuresConfig.isColumnarProcessing(),
                        false),
                booleanSessionProperty(
                        COLUMNAR_PROCESSING_DICTIONARY,
                        "Use columnar processing with optimizations for dictionaries",
                        featuresConfig.isColumnarProcessingDictionary(),
                        false),
                booleanSessionProperty(
                        DICTIONARY_AGGREGATION,
                        "Enable optimization for aggregations on dictionaries",
                        featuresConfig.isDictionaryAggregation(),
                        false),
                integerSessionProperty(
                        INITIAL_SPLITS_PER_NODE,
                        "The number of splits each node will run per task, initially",
                        taskManagerConfig.getInitialSplitsPerNode(),
                        false),
                new PropertyMetadata<>(
                        SPLIT_CONCURRENCY_ADJUSTMENT_INTERVAL,
                        "Experimental: Interval between changes to the number of concurrent splits per node",
                        VARCHAR,
                        Duration.class,
                        taskManagerConfig.getSplitConcurrencyAdjustmentInterval(),
                        false,
                        value -> Duration.valueOf((String) value)),
                booleanSessionProperty(
                        PLAN_WITH_TABLE_NODE_PARTITIONING,
                        "Experimental: Adapt plan to pre-partitioned tables",
                        true,
                        false));
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static String getExecutionPolicy(Session session)
    {
        return session.getProperty(EXECUTION_POLICY, String.class);
    }

    public static boolean isOptimizeHashGenerationEnabled(Session session)
    {
        return session.getProperty(OPTIMIZE_HASH_GENERATION, Boolean.class);
    }

    public static boolean isDistributedJoinEnabled(Session session)
    {
        return session.getProperty(DISTRIBUTED_JOIN, Boolean.class);
    }

    public static boolean isDistributedIndexJoinEnabled(Session session)
    {
        return session.getProperty(DISTRIBUTED_INDEX_JOIN, Boolean.class);
    }

    public static int getHashPartitionCount(Session session)
    {
        return session.getProperty(HASH_PARTITION_COUNT, Integer.class);
    }

    public static boolean preferStreamingOperators(Session session)
    {
        return session.getProperty(PREFER_STREAMING_OPERATORS, Boolean.class);
    }

    public static int getTaskWriterCount(Session session)
    {
        return session.getProperty(TASK_WRITER_COUNT, Integer.class);
    }

    public static boolean isRedistributeWrites(Session session)
    {
        return session.getProperty(REDISTRIBUTE_WRITES, Boolean.class);
    }

    public static boolean isPushTableWriteThroughUnion(Session session)
    {
        return session.getProperty(PUSH_TABLE_WRITE_THROUGH_UNION, Boolean.class);
    }

    public static int getTaskJoinConcurrency(Session session)
    {
        return session.getProperty(TASK_JOIN_CONCURRENCY, Integer.class);
    }

    public static int getTaskHashBuildConcurrency(Session session)
    {
        return session.getProperty(TASK_HASH_BUILD_CONCURRENCY, Integer.class);
    }

    public static int getTaskAggregationConcurrency(Session session)
    {
        return session.getProperty(TASK_AGGREGATION_CONCURRENCY, Integer.class);
    }

    public static boolean isIntermediateAggregation(Session session)
    {
        return session.getProperty(TASK_INTERMEDIATE_AGGREGATION, Boolean.class);
    }

    public static boolean isShareIndexLoading(Session session)
    {
        return session.getProperty(TASK_SHARE_INDEX_LOADING, Boolean.class);
    }

    public static boolean isColumnarProcessingEnabled(Session session)
    {
        return session.getProperty(COLUMNAR_PROCESSING, Boolean.class);
    }

    public static boolean isColumnarProcessingDictionaryEnabled(Session session)
    {
        return session.getProperty(COLUMNAR_PROCESSING_DICTIONARY, Boolean.class);
    }

    public static boolean isDictionaryAggregationEnabled(Session session)
    {
        return session.getProperty(DICTIONARY_AGGREGATION, Boolean.class);
    }

    public static DataSize getQueryMaxMemory(Session session)
    {
        return session.getProperty(QUERY_MAX_MEMORY, DataSize.class);
    }

    public static Duration getQueryMaxRunTime(Session session)
    {
        return session.getProperty(QUERY_MAX_RUN_TIME, Duration.class);
    }

    public static boolean resourceOvercommit(Session session)
    {
        return session.getProperty(RESOURCE_OVERCOMMIT, Boolean.class);
    }

    public static boolean planWithTableNodePartitioning(Session session)
    {
        return session.getProperty(PLAN_WITH_TABLE_NODE_PARTITIONING, Boolean.class);
    }

    public static int getInitialSplitsPerNode(Session session)
    {
        return session.getProperty(INITIAL_SPLITS_PER_NODE, Integer.class);
    }

    public static Duration getSplitConcurrencyAdjustmentInterval(Session session)
    {
        return session.getProperty(SPLIT_CONCURRENCY_ADJUSTMENT_INTERVAL, Duration.class);
    }
}
