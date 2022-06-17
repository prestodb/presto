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
import com.facebook.presto.sql.planner.CollectSourceStats;
import com.facebook.presto.sql.planner.CollectSourceStats.Pair;
import org.apache.spark.SparkContext;

import static com.facebook.presto.spark.PrestoSparkSessionProperties.getMultiplierForAutomaticResourceManagement;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.isResourceManagementDIRCUOptimized;

public class PhysicalResourceOptimizer
{
    private static final Logger log = Logger.get(PhysicalResourceOptimizer.class);
    private final Metadata metadata;
    private final Session session;
    private final PlanNode root;
    // This needs to be extracted from  "mode": "T1_MIGRATION_10G",
    private static final double CONTAINER_SIZE = 10.0;
    // ToDo this needs to be updated continuously
    private static final double THRESHOLD_SIZE_FOR_LONGER_QUERIES = 600; // in GB
    private static final int MAX_EXECUTOR_FOR_LARGE_QUERIES_DIRCU_OPTIMIZED = 600;
    private static final int MIN_EXECUTOR_FOR_LARGE_QUERIES_DIRCU_OPTIMIZED = 200;
    private static final int MAX_EXECUTOR_FOR_LARGE_QUERIES_LATENCY_OPTIMIZED = 1000;
    private static final int MIN_EXECUTOR_FOR_LARGE_QUERIES_LATENCY_OPTIMIZED = 400;
    private static final int MAX_EXECUTOR_FOR_SMALL_QUERIES_DIRCU_OPTIMIZED = 200;
    private static final int MIN_EXECUTOR_FOR_SMALL_QUERIES_DIRCU_OPTIMIZED = 50;
    private static final int MAX_EXECUTOR_FOR_SMALL_QUERIES_LATENCY_OPTIMIZED = 400;
    private static final int MIN_EXECUTOR_FOR_SMALL_QUERIES_LATENCY_OPTIMIZED = 200;

    public PhysicalResourceOptimizer(Metadata metadata, Session session, PlanNode root)
    {
        this.metadata = metadata;
        this.session = session;
        this.root = root;
    }

    public void optimize(Session session, SparkContext sparkContext)
    {
        Pair<Double, Boolean> sourceStatistics = new CollectSourceStats(metadata, session).collectSourceStats(root);

        if (0 == sourceStatistics.getValue().compareTo(false)) {
            log.warn(String.format("Source missing statistics, skipping automatic resource tuning."));
            return;
        }

        double inputDataInBytes = sourceStatistics.getKey();
        if (0 >= inputDataInBytes || inputDataInBytes > Double.MAX_VALUE) {
            log.warn(String.format("Failed to retrieve correct data to be read, skipping automatic resource tuning."));
            return;
        }

        double inputDataInGB = inputDataInBytes / 1000000000;
        boolean isCurrentQueryHourPlus = (inputDataInGB > THRESHOLD_SIZE_FOR_LONGER_QUERIES) ? true : false;
        double multiplier = getMultiplierForAutomaticResourceManagement(session);

        if (inputDataInGB * multiplier < Double.MAX_VALUE) {
            inputDataInGB = inputDataInGB * multiplier;
        }

        int desiredExecutorCount = calculateExecutorCount(inputDataInGB, session, isCurrentQueryHourPlus);
        sparkContext.conf().set("spark.dynamicAllocation.maxExecutors", Integer.toString(desiredExecutorCount));
    }

    private int calculateExecutorCount(double inputDataInGB, Session session, boolean isCurrentQueryHourPlus)
    {
        double numberOfExecutors = inputDataInGB / CONTAINER_SIZE;
        boolean isRMDIRCUOptimized = isResourceManagementDIRCUOptimized(session);

        if (isCurrentQueryHourPlus) {
            return (isRMDIRCUOptimized) ? calculateDircuOptimizedExecutorCountForLongerQueries((int) numberOfExecutors) :
                    calculateLatencyOptimizedExecutorCountForLongerQueries((int) numberOfExecutors);
        }
        else {
            return (isRMDIRCUOptimized) ? calculateDircuOptimizedExecutorCountForShorterQueries((int) numberOfExecutors) :
                    calculateLatencyOptimizedExecutorCountForShorterQueries((int) numberOfExecutors);
        }
    }

    private int calculateLatencyOptimizedExecutorCountForShorterQueries(int numberOfExecutors)
    {
        int updatedExecutorCount = Math.max(numberOfExecutors, MIN_EXECUTOR_FOR_SMALL_QUERIES_LATENCY_OPTIMIZED);
        int finalExecutorCount = Math.max(MIN_EXECUTOR_FOR_SMALL_QUERIES_LATENCY_OPTIMIZED, Math.min(MAX_EXECUTOR_FOR_SMALL_QUERIES_LATENCY_OPTIMIZED, updatedExecutorCount));
        return finalExecutorCount;
    }

    private int calculateDircuOptimizedExecutorCountForShorterQueries(int numberOfExecutors)
    {
        int updatedExecutorCount = Math.max(numberOfExecutors, MIN_EXECUTOR_FOR_SMALL_QUERIES_DIRCU_OPTIMIZED);
        int finalExecutorCount = Math.max(MIN_EXECUTOR_FOR_SMALL_QUERIES_DIRCU_OPTIMIZED, Math.min(MAX_EXECUTOR_FOR_SMALL_QUERIES_DIRCU_OPTIMIZED, updatedExecutorCount));
        return finalExecutorCount;
    }

    private int calculateDircuOptimizedExecutorCountForLongerQueries(int numberOfExecutors)
    {
        int updatedExecutorCount = Math.max(numberOfExecutors, MIN_EXECUTOR_FOR_LARGE_QUERIES_DIRCU_OPTIMIZED);
        int finalExecutorCount = Math.max(MIN_EXECUTOR_FOR_LARGE_QUERIES_DIRCU_OPTIMIZED, Math.min(MAX_EXECUTOR_FOR_LARGE_QUERIES_DIRCU_OPTIMIZED, updatedExecutorCount));
        return finalExecutorCount;
    }

    private int calculateLatencyOptimizedExecutorCountForLongerQueries(int numberOfExecutors)
    {
        int updatedExecutorCount = Math.max(numberOfExecutors, MIN_EXECUTOR_FOR_LARGE_QUERIES_LATENCY_OPTIMIZED);
        int finalExecutorCount = Math.max(MIN_EXECUTOR_FOR_LARGE_QUERIES_LATENCY_OPTIMIZED, Math.min(MAX_EXECUTOR_FOR_LARGE_QUERIES_LATENCY_OPTIMIZED, updatedExecutorCount));
        return finalExecutorCount;
    }
}
