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
package com.facebook.presto.spark;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.spi.plan.PlanNode;
import io.airlift.units.DataSize;

import java.util.OptionalInt;

import static com.facebook.presto.SystemSessionProperties.getHashPartitionCount;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.getAverageInputDataSizePerExecutor;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.getAverageInputDataSizePerPartition;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.getMaxExecutorCount;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.getMaxHashPartitionCount;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.getMinExecutorCount;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.getMinHashPartitionCount;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.isSparkExecutorAllocationStrategyEnabled;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.isSparkHashPartitionCountAllocationStrategyEnabled;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.isSparkResourceAllocationStrategyEnabled;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.units.DataSize.Unit.BYTE;

public class PrestoSparkPhysicalResourceCalculator
{
    private static final Logger log = Logger.get(PrestoSparkPhysicalResourceCalculator.class);

    /**
     * Calculates the final resource settings for the query. This takes into account all override values
     * with the following precedence:
     * <ul>
     *     <li>
     *         Session property enabling allocation strategy {@link PrestoSparkSessionProperties#SPARK_HASH_PARTITION_COUNT_ALLOCATION_STRATEGY_ENABLED}
     *         or {@link PrestoSparkSessionProperties#SPARK_EXECUTOR_ALLOCATION_STRATEGY_ENABLED}.
     *         If {@link PrestoSparkSessionProperties#SPARK_RESOURCE_ALLOCATION_STRATEGY_ENABLED} is enabled,
     *         both the properties will implicitly be enabled.
     *     </li>
     *     <li>
     *         Session property with explicit value for {@link SystemSessionProperties#HASH_PARTITION_COUNT}
     *         or {@link PrestoSparkSettingsRequirements#SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS_CONFIG}
     *     </li>
     *     <li>
     *         System property as provided by {@link SystemSessionProperties#HASH_PARTITION_COUNT}
     *         or {@link PrestoSparkSettingsRequirements#SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS_CONFIG}
     *     </li>
     * </ul>
     *
     */
    public PhysicalResourceSettings calculate(PlanNode plan, PrestoSparkSourceStatsCollector prestoSparkSourceStatsCollector, Session session)
    {
        int hashPartitionCount = getHashPartitionCount(session);
        OptionalInt maxExecutorCount = OptionalInt.empty();
        PhysicalResourceSettings defaultResourceSettings = new PhysicalResourceSettings(hashPartitionCount, maxExecutorCount);

        if (!anyAllocationStrategyEnabled(session)) {
            log.info(String.format("ResourceAllocationStrategy disabled. Executing query %s with %s", session.getQueryId(), defaultResourceSettings));
            return defaultResourceSettings;
        }

        double inputDataInBytes = prestoSparkSourceStatsCollector.collectSourceStats(plan);
        DataSize inputSize = new DataSize(inputDataInBytes, BYTE);
        if (inputDataInBytes < 0) {
            log.warn(String.format("Input data statistics missing, inputDataInBytes=%.2f skipping automatic resource tuning. Executing query %s with %s",
                    inputDataInBytes, session.getQueryId(), defaultResourceSettings));
            return defaultResourceSettings;
        }
        else if (Double.isNaN(inputDataInBytes)) {
            log.warn(String.format("Failed to retrieve correct size, inputDataInBytes=%.2f skipping automatic resource tuning. Executing query %s with %s",
                    inputDataInBytes, session.getQueryId(), defaultResourceSettings));
            return defaultResourceSettings;
        }
        // update hashPartitionCount only if resource allocation or hash partition allocation is enabled
        if (isSparkResourceAllocationStrategyEnabled(session) || isSparkHashPartitionCountAllocationStrategyEnabled(session)) {
            hashPartitionCount = calculateHashPartitionCount(session, inputSize);
        }

        // update maxExecutorCount only if resource allocation or executor allocation is enabled
        if (isSparkResourceAllocationStrategyEnabled(session) || isSparkExecutorAllocationStrategyEnabled(session)) {
            maxExecutorCount = OptionalInt.of(calculateExecutorCount(session, inputSize));
        }
        PhysicalResourceSettings finalResourceSettings = new PhysicalResourceSettings(hashPartitionCount, maxExecutorCount);

        log.info(String.format("Executing query %s with %s based on resource allocation strategy", session.getQueryId(), finalResourceSettings));
        return finalResourceSettings;
    }

    private static boolean anyAllocationStrategyEnabled(Session session)
    {
        return isSparkResourceAllocationStrategyEnabled(session)
                || isSparkExecutorAllocationStrategyEnabled(session)
                || isSparkHashPartitionCountAllocationStrategyEnabled(session);
    }

    private static int calculateExecutorCount(Session session, DataSize inputData)
    {
        int minExecutorCount = getMinExecutorCount(session);
        int maxExecutorCount = getMaxExecutorCount(session);
        checkState(((maxExecutorCount >= minExecutorCount) && (minExecutorCount > 0)), String.format(
                "maxExecutorCount: %d needs to greater than or equal to maxExecutorCount : %d", maxExecutorCount, maxExecutorCount));

        long averageInputDataSizePerExecutorInBytes = getAverageInputDataSizePerExecutor(session).toBytes();
        int calculatedNumberOfExecutors = (int) (inputData.toBytes() / averageInputDataSizePerExecutorInBytes);

        return (Math.max(minExecutorCount, Math.min(maxExecutorCount, calculatedNumberOfExecutors)));
    }

    private static int calculateHashPartitionCount(Session session, DataSize inputDataInGB)
    {
        int maxHashPartitionCount = getMaxHashPartitionCount(session);
        int minHashPartitionCount = getMinHashPartitionCount(session);
        checkState(((maxHashPartitionCount >= minHashPartitionCount) && (minHashPartitionCount > 0)), String.format(
                "maxHashPartitionCount : %d needs to greater than  or equal to minHashPartitionCount : %d", maxHashPartitionCount, minHashPartitionCount));

        long averageInputDataSizePerPartitionInBytes = getAverageInputDataSizePerPartition(session).toBytes();
        int calculatedNumberOfPartitions = (int) (inputDataInGB.toBytes() / averageInputDataSizePerPartitionInBytes);

        return (Math.max(minHashPartitionCount, Math.min(maxHashPartitionCount, calculatedNumberOfPartitions)));
    }
}
