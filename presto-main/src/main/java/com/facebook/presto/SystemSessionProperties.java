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
    public static final String TASK_DEFAULT_CONCURRENCY = "task_default_concurrency";
    public static final String TASK_JOIN_CONCURRENCY = "task_join_concurrency";
    public static final String TASK_HASH_BUILD_CONCURRENCY = "task_hash_build_concurrency";
    public static final String TASK_AGGREGATION_CONCURRENCY = "task_aggregation_concurrency";
    public static final String TASK_INTERMEDIATE_AGGREGATION = "task_intermediate_aggregation";
    public static final String QUERY_MAX_MEMORY = "query_max_memory";
    public static final String QUERY_MAX_RUN_TIME = "query_max_run_time";
    public static final String REDISTRIBUTE_WRITES = "redistribute_writes";
    public static final String EXECUTION_POLICY = "execution_policy";

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
                integerSessionProperty(
                        TASK_DEFAULT_CONCURRENCY,
                        "Experimental: Default number of local parallel jobs per worker",
                        taskManagerConfig.getTaskDefaultConcurrency(),
                        false),
                integerSessionProperty(
                        TASK_JOIN_CONCURRENCY,
                        "Experimental: Default number of local parallel join jobs per worker",
                        taskManagerConfig.getTaskDefaultConcurrency(),
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
                        value -> DataSize.valueOf((String) value)));
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

    public static int getTaskJoinConcurrency(Session session)
    {
        return getPropertyOr(session, TASK_JOIN_CONCURRENCY, TASK_DEFAULT_CONCURRENCY, Integer.class);
    }

    public static int getTaskHashBuildConcurrency(Session session)
    {
        return getPropertyOr(session, TASK_HASH_BUILD_CONCURRENCY, TASK_DEFAULT_CONCURRENCY, Integer.class);
    }

    public static int getTaskAggregationConcurrency(Session session)
    {
        return getPropertyOr(session, TASK_AGGREGATION_CONCURRENCY, TASK_DEFAULT_CONCURRENCY, Integer.class);
    }

    public static boolean isIntermediateAggregation(Session session)
    {
        return session.getProperty(TASK_INTERMEDIATE_AGGREGATION, Boolean.class);
    }

    public static DataSize getQueryMaxMemory(Session session)
    {
        return session.getProperty(QUERY_MAX_MEMORY, DataSize.class);
    }

    public static Duration getQueryMaxRunTime(Session session)
    {
        return session.getProperty(QUERY_MAX_RUN_TIME, Duration.class);
    }

    private static <T> T getPropertyOr(Session session, String propertyName, String defaultPropertyName, Class<T> type)
    {
        T value = session.getProperty(propertyName, type);
        if (value == null) {
            value = session.getProperty(defaultPropertyName, type);
        }
        return value;
    }
}
