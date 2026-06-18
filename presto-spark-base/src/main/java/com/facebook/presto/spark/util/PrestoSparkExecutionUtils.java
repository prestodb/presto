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
package com.facebook.presto.spark.util;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.spark.PrestoSparkExecutionSettings;
import com.facebook.presto.spark.PrestoSparkSessionContext;
import com.facebook.presto.spark.classloader_interface.ExecutionStrategy;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.google.common.collect.ImmutableMap;

import java.util.List;

import static com.facebook.presto.SystemSessionProperties.HASH_PARTITION_COUNT;
import static com.facebook.presto.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static com.facebook.presto.SystemSessionProperties.getHashPartitionCount;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.getHashPartitionCountScalingFactorOnOutOfMemory;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.getOutOfMemoryRetryPrestoSessionProperties;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.getOutOfMemoryRetrySparkConfigs;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_RETRY_EXECUTION_STRATEGY;

public class PrestoSparkExecutionUtils
{
    private static final Logger log = Logger.get(PrestoSparkSessionContext.class);

    private PrestoSparkExecutionUtils() {}

    public static PrestoSparkExecutionSettings getExecutionSettings(
            List<ExecutionStrategy> executionStrategies,
            Session session)
    {
        ImmutableMap.Builder<String, String> sparkConfigProperties = new ImmutableMap.Builder<>();
        ImmutableMap.Builder<String, String> prestoSessionProperties = new ImmutableMap.Builder<>();

        for (ExecutionStrategy strategy : executionStrategies) {
            log.info(String.format("Applying execution strategy: %s. Query Id: %s", strategy.name(), session.getQueryId().getId()));
            switch (strategy) {
                case DISABLE_BROADCAST_JOIN:
                    prestoSessionProperties.put(JOIN_DISTRIBUTION_TYPE, FeaturesConfig.JoinDistributionType.PARTITIONED.name());
                    break;

                case INCREASE_CONTAINER_SIZE:
                    sparkConfigProperties.putAll(getOutOfMemoryRetrySparkConfigs(session));
                    prestoSessionProperties.putAll(getOutOfMemoryRetryPrestoSessionProperties(session));
                    break;

                case INCREASE_HASH_PARTITION_COUNT:
                    long updatedPartitionCount = Math.round(getHashPartitionCount(session) *
                            getHashPartitionCountScalingFactorOnOutOfMemory(session));
                    prestoSessionProperties.put(HASH_PARTITION_COUNT, Long.toString(updatedPartitionCount));
                    break;
                default:
                    throw new PrestoException(INVALID_RETRY_EXECUTION_STRATEGY, "Execution strategy not supported: " + executionStrategies);
            }
        }

        return new PrestoSparkExecutionSettings(sparkConfigProperties.build(), prestoSessionProperties.build());
    }
}
