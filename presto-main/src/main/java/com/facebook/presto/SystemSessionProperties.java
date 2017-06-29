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
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.FeaturesConfig.JoinDistributionType;
import com.facebook.presto.sql.analyzer.FeaturesConfig.JoinReorderingStrategy;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.inject.Inject;

import java.util.List;
import java.util.stream.Stream;

import static com.facebook.presto.spi.session.PropertyMetadata.booleanSessionProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.integerSessionProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.stringSessionProperty;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.DataSize.succinctBytes;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

public final class SystemSessionProperties
{
    public static final String OPTIMIZE_HASH_GENERATION = "optimize_hash_generation";
    public static final String JOIN_DISTRIBUTION_TYPE = "join_distribution_type";
    public static final String DISTRIBUTED_INDEX_JOIN = "distributed_index_join";
    public static final String HASH_PARTITION_COUNT = "hash_partition_count";
    public static final String PREFER_STREAMING_OPERATORS = "prefer_streaming_operators";
    public static final String TASK_WRITER_COUNT = "task_writer_count";
    public static final String TASK_CONCURRENCY = "task_concurrency";
    public static final String TASK_SHARE_INDEX_LOADING = "task_share_index_loading";
    public static final String QUERY_MAX_MEMORY = "query_max_memory";
    public static final String QUERY_MAX_RUN_TIME = "query_max_run_time";
    public static final String RESOURCE_OVERCOMMIT = "resource_overcommit";
    public static final String QUERY_MAX_CPU_TIME = "query_max_cpu_time";
    public static final String REDISTRIBUTE_WRITES = "redistribute_writes";
    public static final String PUSH_TABLE_WRITE_THROUGH_UNION = "push_table_write_through_union";
    public static final String EXECUTION_POLICY = "execution_policy";
    public static final String DICTIONARY_AGGREGATION = "dictionary_aggregation";
    public static final String PLAN_WITH_TABLE_NODE_PARTITIONING = "plan_with_table_node_partitioning";
    public static final String COLOCATED_JOIN = "colocated_join";
    public static final String JOIN_REORDERING_STRATEGY = "join_reordering_strategy";
    public static final String INITIAL_SPLITS_PER_NODE = "initial_splits_per_node";
    public static final String SPLIT_CONCURRENCY_ADJUSTMENT_INTERVAL = "split_concurrency_adjustment_interval";
    public static final String OPTIMIZE_METADATA_QUERIES = "optimize_metadata_queries";
    public static final String FAST_INEQUALITY_JOINS = "fast_inequality_joins";
    public static final String QUERY_PRIORITY = "query_priority";
    public static final String SPILL_ENABLED = "spill_enabled";
    public static final String OPERATOR_MEMORY_LIMIT_BEFORE_SPILL = "operator_memory_limit_before_spill";
    public static final String OPTIMIZE_DISTINCT_AGGREGATIONS = "optimize_mixed_distinct_aggregations";
    public static final String LEGACY_ORDER_BY = "legacy_order_by";
    public static final String ITERATIVE_OPTIMIZER = "iterative_optimizer_enabled";
    public static final String ITERATIVE_OPTIMIZER_TIMEOUT = "iterative_optimizer_timeout";
    public static final String EXCHANGE_COMPRESSION = "exchange_compression";
    public static final String ENABLE_INTERMEDIATE_AGGREGATIONS = "enable_intermediate_aggregations";
    public static final String PUSH_AGGREGATION_THROUGH_JOIN = "push_aggregation_through_join";
    public static final String PUSH_PARTIAL_AGGREGATION_THROUGH_JOIN = "push_partial_aggregation_through_join";
    public static final String USE_NEW_STATS_CALCULATOR = "use_new_stats_calculator";

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
                new PropertyMetadata<>(
                        JOIN_DISTRIBUTION_TYPE,
                        format("The join method to use. Options are %s",
                                Stream.of(JoinDistributionType.values())
                                        .map(FeaturesConfig.JoinDistributionType::name)
                                        .collect(joining(","))),
                        VARCHAR,
                        JoinDistributionType.class,
                        featuresConfig.getJoinDistributionType(),
                        false,
                        value -> JoinDistributionType.valueOf(((String) value).toUpperCase()),
                        JoinDistributionType::name),
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
                new PropertyMetadata<>(
                        TASK_WRITER_COUNT,
                        "Default number of local parallel table writer jobs per worker",
                        BIGINT,
                        Integer.class,
                        taskManagerConfig.getWriterCount(),
                        false,
                        value -> {
                            int concurrency = ((Number) value).intValue();
                            if (Integer.bitCount(concurrency) != 1) {
                                throw new PrestoException(
                                        StandardErrorCode.INVALID_SESSION_PROPERTY,
                                        format("%s must be a power of 2: %s", TASK_WRITER_COUNT, concurrency));
                            }
                            return concurrency;
                        },
                        value -> value),
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
                new PropertyMetadata<>(
                        TASK_CONCURRENCY,
                        "Default number of local parallel jobs per worker",
                        BIGINT,
                        Integer.class,
                        taskManagerConfig.getTaskConcurrency(),
                        false,
                        value -> {
                            int concurrency = ((Number) value).intValue();
                            if (Integer.bitCount(concurrency) != 1) {
                                throw new PrestoException(
                                        StandardErrorCode.INVALID_SESSION_PROPERTY,
                                        format("%s must be a power of 2: %s", TASK_CONCURRENCY, concurrency));
                            }
                            return concurrency;
                        },
                        value -> value),
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
                        value -> Duration.valueOf((String) value),
                        Duration::toString),
                new PropertyMetadata<>(
                        QUERY_MAX_CPU_TIME,
                        "Maximum CPU time of a query",
                        VARCHAR,
                        Duration.class,
                        queryManagerConfig.getQueryMaxCpuTime(),
                        false,
                        value -> Duration.valueOf((String) value),
                        Duration::toString),
                new PropertyMetadata<>(
                        QUERY_MAX_MEMORY,
                        "Maximum amount of distributed memory a query can use (will be capped by global config property)",
                        VARCHAR,
                        DataSize.class,
                        memoryManagerConfig.getMaxQueryMemory(),
                        true,
                        value -> {
                            long sessionValue = DataSize.valueOf((String) value).toBytes();
                            long configValue = memoryManagerConfig.getMaxQueryMemory().toBytes();
                            return succinctBytes(Math.min(configValue, sessionValue));
                        },
                        DataSize::toString),
                booleanSessionProperty(
                        RESOURCE_OVERCOMMIT,
                        "Use resources which are not guaranteed to be available to the query",
                        false,
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
                        value -> Duration.valueOf((String) value),
                        Duration::toString),
                booleanSessionProperty(
                        OPTIMIZE_METADATA_QUERIES,
                        "Enable optimization for metadata queries",
                        featuresConfig.isOptimizeMetadataQueries(),
                        false),
                integerSessionProperty(
                        QUERY_PRIORITY,
                        "The priority of queries. Larger numbers are higher priority",
                        1,
                        false),
                booleanSessionProperty(
                        PLAN_WITH_TABLE_NODE_PARTITIONING,
                        "Experimental: Adapt plan to pre-partitioned tables",
                        true,
                        false),
                new PropertyMetadata<>(
                        JOIN_REORDERING_STRATEGY,
                        format("The join reordering strategy to use. Options are %s",
                                Stream.of(JoinReorderingStrategy.values())
                                        .map(FeaturesConfig.JoinReorderingStrategy::name)
                                        .collect(joining(","))),
                        VARCHAR,
                        JoinReorderingStrategy.class,
                        featuresConfig.getJoinReorderingStrategy(),
                        false,
                        value -> JoinReorderingStrategy.valueOf(((String) value).toUpperCase()),
                        JoinReorderingStrategy::name),
                booleanSessionProperty(
                        FAST_INEQUALITY_JOINS,
                        "Use faster handling of inequality join if it is possible",
                        featuresConfig.isFastInequalityJoins(),
                        false),
                booleanSessionProperty(
                        COLOCATED_JOIN,
                        "Experimental: Use a colocated join when possible",
                        featuresConfig.isColocatedJoinsEnabled(),
                        false),
                new PropertyMetadata<>(
                        SPILL_ENABLED,
                        "Experimental: Enable spilling",
                        BOOLEAN,
                        Boolean.class,
                        featuresConfig.isSpillEnabled(),
                        false,
                        value -> {
                            boolean spillEnabled = (Boolean) value;
                            if (spillEnabled && featuresConfig.getSpillerSpillPaths().isEmpty()) {
                                throw new PrestoException(
                                        StandardErrorCode.INVALID_SESSION_PROPERTY,
                                        format("%s cannot be set to true; no spill paths configured", SPILL_ENABLED));
                            }
                            return spillEnabled;
                        },
                        value -> value),
                new PropertyMetadata<>(
                        OPERATOR_MEMORY_LIMIT_BEFORE_SPILL,
                        "Experimental: Operator memory limit before spill",
                        VARCHAR,
                        DataSize.class,
                        featuresConfig.getOperatorMemoryLimitBeforeSpill(),
                        false,
                        value -> DataSize.valueOf((String) value),
                        DataSize::toString),
                booleanSessionProperty(
                        OPTIMIZE_DISTINCT_AGGREGATIONS,
                        "Optimize mixed non-distinct and distinct aggregations",
                        featuresConfig.isOptimizeMixedDistinctAggregations(),
                        false),
                booleanSessionProperty(
                        LEGACY_ORDER_BY,
                        "Use legacy rules for column resolution in ORDER BY clause",
                        featuresConfig.isLegacyOrderBy(),
                        false),
                booleanSessionProperty(
                        ITERATIVE_OPTIMIZER,
                        "Experimental: enable iterative optimizer",
                        featuresConfig.isIterativeOptimizerEnabled(),
                        false),
                new PropertyMetadata<>(
                        ITERATIVE_OPTIMIZER_TIMEOUT,
                        "Timeout for plan optimization in iterative optimizer",
                        VARCHAR,
                        Duration.class,
                        featuresConfig.getIterativeOptimizerTimeout(),
                        false,
                        value -> Duration.valueOf((String) value),
                        Duration::toString),
                booleanSessionProperty(
                        EXCHANGE_COMPRESSION,
                        "Enable compression in exchanges",
                        featuresConfig.isExchangeCompressionEnabled(),
                        false),
                booleanSessionProperty(
                        ENABLE_INTERMEDIATE_AGGREGATIONS,
                        "Enable the use of intermediate aggregations",
                        featuresConfig.isEnableIntermediateAggregations(),
                        false),
                booleanSessionProperty(
                        PUSH_AGGREGATION_THROUGH_JOIN,
                        "Allow pushing aggregations below joins",
                        featuresConfig.isPushAggregationThroughJoin(),
                        false),
                booleanSessionProperty(
                        PUSH_PARTIAL_AGGREGATION_THROUGH_JOIN,
                        "Push partial aggregations below joins",
                        false,
                        false),
                booleanSessionProperty(
                        USE_NEW_STATS_CALCULATOR,
                        "Use new experimental statistics calculator",
                        featuresConfig.isUseNewStatsCalculator(),
                        true));
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static String getExecutionPolicy(Session session)
    {
        return session.getSystemProperty(EXECUTION_POLICY, String.class);
    }

    public static boolean isOptimizeHashGenerationEnabled(Session session)
    {
        return session.getSystemProperty(OPTIMIZE_HASH_GENERATION, Boolean.class);
    }

    public static boolean isDistributedIndexJoinEnabled(Session session)
    {
        return session.getSystemProperty(DISTRIBUTED_INDEX_JOIN, Boolean.class);
    }

    public static int getHashPartitionCount(Session session)
    {
        return session.getSystemProperty(HASH_PARTITION_COUNT, Integer.class);
    }

    public static boolean preferStreamingOperators(Session session)
    {
        return session.getSystemProperty(PREFER_STREAMING_OPERATORS, Boolean.class);
    }

    public static int getTaskWriterCount(Session session)
    {
        return session.getSystemProperty(TASK_WRITER_COUNT, Integer.class);
    }

    public static boolean isRedistributeWrites(Session session)
    {
        return session.getSystemProperty(REDISTRIBUTE_WRITES, Boolean.class);
    }

    public static boolean isPushTableWriteThroughUnion(Session session)
    {
        return session.getSystemProperty(PUSH_TABLE_WRITE_THROUGH_UNION, Boolean.class);
    }

    public static int getTaskConcurrency(Session session)
    {
        return session.getSystemProperty(TASK_CONCURRENCY, Integer.class);
    }

    public static boolean isShareIndexLoading(Session session)
    {
        return session.getSystemProperty(TASK_SHARE_INDEX_LOADING, Boolean.class);
    }

    public static boolean isDictionaryAggregationEnabled(Session session)
    {
        return session.getSystemProperty(DICTIONARY_AGGREGATION, Boolean.class);
    }

    public static boolean isOptimizeMetadataQueries(Session session)
    {
        return session.getSystemProperty(OPTIMIZE_METADATA_QUERIES, Boolean.class);
    }

    public static DataSize getQueryMaxMemory(Session session)
    {
        return session.getSystemProperty(QUERY_MAX_MEMORY, DataSize.class);
    }

    public static Duration getQueryMaxRunTime(Session session)
    {
        return session.getSystemProperty(QUERY_MAX_RUN_TIME, Duration.class);
    }

    public static boolean resourceOvercommit(Session session)
    {
        return session.getSystemProperty(RESOURCE_OVERCOMMIT, Boolean.class);
    }

    public static boolean planWithTableNodePartitioning(Session session)
    {
        return session.getSystemProperty(PLAN_WITH_TABLE_NODE_PARTITIONING, Boolean.class);
    }

    public static boolean isFastInequalityJoin(Session session)
    {
        return session.getSystemProperty(FAST_INEQUALITY_JOINS, Boolean.class);
    }

    public static JoinReorderingStrategy getJoinReorderingStrategy(Session session)
    {
        return session.getSystemProperty(JOIN_REORDERING_STRATEGY, JoinReorderingStrategy.class);
    }

    public static boolean isColocatedJoinEnabled(Session session)
    {
        return session.getSystemProperty(COLOCATED_JOIN, Boolean.class);
    }

    public static int getInitialSplitsPerNode(Session session)
    {
        return session.getSystemProperty(INITIAL_SPLITS_PER_NODE, Integer.class);
    }

    public static int getQueryPriority(Session session)
    {
        Integer priority = session.getSystemProperty(QUERY_PRIORITY, Integer.class);
        checkArgument(priority > 0, "Query priority must be positive");
        return priority;
    }

    public static Duration getSplitConcurrencyAdjustmentInterval(Session session)
    {
        return session.getSystemProperty(SPLIT_CONCURRENCY_ADJUSTMENT_INTERVAL, Duration.class);
    }

    public static Duration getQueryMaxCpuTime(Session session)
    {
        return session.getSystemProperty(QUERY_MAX_CPU_TIME, Duration.class);
    }

    public static boolean isSpillEnabled(Session session)
    {
        return session.getSystemProperty(SPILL_ENABLED, Boolean.class);
    }

    public static DataSize getOperatorMemoryLimitBeforeSpill(Session session)
    {
        DataSize memoryLimitBeforeSpill = session.getSystemProperty(OPERATOR_MEMORY_LIMIT_BEFORE_SPILL, DataSize.class);
        checkArgument(memoryLimitBeforeSpill.toBytes() >= 0, "%s must be positive", OPERATOR_MEMORY_LIMIT_BEFORE_SPILL);
        return memoryLimitBeforeSpill;
    }

    public static boolean isOptimizeDistinctAggregationEnabled(Session session)
    {
        return session.getSystemProperty(OPTIMIZE_DISTINCT_AGGREGATIONS, Boolean.class);
    }

    public static boolean isLegacyOrderByEnabled(Session session)
    {
        return session.getSystemProperty(LEGACY_ORDER_BY, Boolean.class);
    }

    public static boolean isNewOptimizerEnabled(Session session)
    {
        return session.getSystemProperty(ITERATIVE_OPTIMIZER, Boolean.class);
    }

    public static Duration getOptimizerTimeout(Session session)
    {
        return session.getSystemProperty(ITERATIVE_OPTIMIZER_TIMEOUT, Duration.class);
    }

    public static boolean isExchangeCompressionEnabled(Session session)
    {
        return session.getSystemProperty(EXCHANGE_COMPRESSION, Boolean.class);
    }

    public static boolean isEnableIntermediateAggregations(Session session)
    {
        return session.getSystemProperty(ENABLE_INTERMEDIATE_AGGREGATIONS, Boolean.class);
    }

    public static boolean shouldPushAggregationThroughJoin(Session session)
    {
        return session.getSystemProperty(PUSH_AGGREGATION_THROUGH_JOIN, Boolean.class);
    }

    public static boolean isPushAggregationThroughJoin(Session session)
    {
        return session.getSystemProperty(PUSH_PARTIAL_AGGREGATION_THROUGH_JOIN, Boolean.class);
    }

    public static boolean isUseNewStatsCalculator(Session session)
    {
        return session.getSystemProperty(USE_NEW_STATS_CALCULATOR, Boolean.class);
    }

    public static JoinDistributionType getJoinDistributionType(Session session)
    {
        return session.getSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.class);
    }
}
