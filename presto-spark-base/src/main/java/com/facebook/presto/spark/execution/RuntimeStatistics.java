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
package com.facebook.presto.spark.execution;

import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.spi.statistics.RuntimeSourceInfo;
import org.apache.spark.MapOutputStatistics;
import org.pcollections.HashTreePMap;

import java.util.Arrays;
import java.util.Optional;

/**
 * Statistics created at runtime during the adaptive execution of the plan to support re-optimization decisions.
 */
public class RuntimeStatistics
{
    private RuntimeStatistics() {}

    /**
     * Create a {@link PlanNodeStatsEstimate} given {@link MapOutputStatistics}.
     */
    public static Optional<PlanNodeStatsEstimate> createRuntimeStats(Optional<MapOutputStatistics> mapOutputStatistics)
    {
        return mapOutputStatistics.map(stats ->
        {
            double totalSize = Arrays.stream(stats.bytesByPartitionId()).sum();
            return new PlanNodeStatsEstimate(Double.NaN, totalSize, HashTreePMap.empty(), new RuntimeSourceInfo());
        });
    }
}
