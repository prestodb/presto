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
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.plan.PlanNode;
import io.airlift.units.DataSize;

import static com.facebook.presto.spark.PhysicalResourceSettings.DISABLED_PHYSICAL_RESOURCE_SETTING;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.getAverageInputDataSizePerExecutor;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.getAverageInputDataSizePerPartition;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.getMaxExecutorCount;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.getMaxHashPartitionCount;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.getMinExecutorCount;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.getMinHashPartitionCount;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.isSparkResourceAllocationStrategyEnabled;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.units.DataSize.Unit.BYTE;

public class PrestoSparkPhysicalResourceCalculator
{
    private static final Logger log = Logger.get(PrestoSparkPhysicalResourceCalculator.class);

    public PhysicalResourceSettings calculate(PlanNode plan, Metadata metaData, Session session)
    {
        if (!isSparkResourceAllocationStrategyEnabled(session)) {
            return new PhysicalResourceSettings(0, 0);
        }

        double inputDataInBytes = new PrestoSparkSourceStatsCollector(metaData, session).collectSourceStats(plan);

        if (inputDataInBytes < 0) {
            log.warn(String.format("Input data statistics missing, inputDataInBytes=%.2f skipping automatic resource tuning.", inputDataInBytes));
            return DISABLED_PHYSICAL_RESOURCE_SETTING;
        }

        if ((inputDataInBytes > Double.MAX_VALUE) || (Double.isNaN(inputDataInBytes))) {
            log.warn(String.format("Failed to retrieve correct size, data read=%.2f, skipping automatic resource tuning.", inputDataInBytes));
            return DISABLED_PHYSICAL_RESOURCE_SETTING;
        }

        DataSize inputSize = new DataSize(inputDataInBytes, BYTE);
        int executorCount = calculateExecutorCount(session, inputSize);
        int hashPartitionCount = calculateHashPartitionCount(session, inputSize);

        return new PhysicalResourceSettings(executorCount, hashPartitionCount);
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
