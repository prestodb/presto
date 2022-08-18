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
import com.facebook.presto.spark.PrestoSparkRetryExecutionSettings;
import com.facebook.presto.spark.PrestoSparkSessionContext;
import com.facebook.presto.spark.classloader_interface.RetryExecutionStrategy;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.google.common.collect.ImmutableMap;

import static com.facebook.presto.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.getOutOfMemoryRetryPrestoSessionProperties;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.getOutOfMemoryRetrySparkConfigs;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_RETRY_EXECUTION_STRATEGY;

public class PrestoSparkRetryExecutionUtils
{
    private static final Logger log = Logger.get(PrestoSparkSessionContext.class);

    private PrestoSparkRetryExecutionUtils() {}

    public static PrestoSparkRetryExecutionSettings getRetryExecutionSettings(
            RetryExecutionStrategy retryExecutionStrategy,
            Session session)
    {
        log.info(String.format("Applying retry execution strategy: %s. Query Id: %s", retryExecutionStrategy.name(), session.getQueryId().getId()));
        switch (retryExecutionStrategy) {
            case DISABLE_BROADCAST_JOIN:
                ImmutableMap.Builder<String, String> prestoSettings = new ImmutableMap.Builder<>();
                prestoSettings.put(JOIN_DISTRIBUTION_TYPE, FeaturesConfig.JoinDistributionType.PARTITIONED.name());
                return new PrestoSparkRetryExecutionSettings(ImmutableMap.of(), prestoSettings.build());

            case INCREASE_CONTAINER_SIZE:
                return new PrestoSparkRetryExecutionSettings(getOutOfMemoryRetrySparkConfigs(session), getOutOfMemoryRetryPrestoSessionProperties(session));

            default:
                throw new PrestoException(INVALID_RETRY_EXECUTION_STRATEGY, "Retry execution strategy not supported: " + retryExecutionStrategy);
        }
    }
}
