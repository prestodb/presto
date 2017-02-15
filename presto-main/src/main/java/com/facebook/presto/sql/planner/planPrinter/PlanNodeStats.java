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
package com.facebook.presto.sql.planner.planPrinter;

import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.util.Mergeable;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.util.MoreMaps.mergeMaps;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.DataSize.succinctBytes;
import static java.lang.Double.max;
import static java.lang.Math.sqrt;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toMap;

// TODO: break into operator-specific stats classes instead of having a big union-class aggregating all stats together
@Deprecated
public class PlanNodeStats
        implements Mergeable<PlanNodeStats>
{
    private final PlanNodeId planNodeId;

    private final Duration planNodeWallTime;
    private final long planNodeInputPositions;
    private final DataSize planNodeInputDataSize;
    private final long planNodeOutputPositions;
    private final DataSize planNodeOutputDataSize;

    private final Map<String, OperatorInputStats> operatorInputStats;
    private final Map<String, OperatorHashCollisionsStats> operatorHashCollisionsStats;
    private final Optional<WindowOperatorStats> windowOperatorStats;

    PlanNodeStats(
            PlanNodeId planNodeId,
            Duration planNodeWallTime,
            long planNodeInputPositions,
            DataSize planNodeInputDataSize,
            long planNodeOutputPositions,
            DataSize planNodeOutputDataSize,
            Map<String, OperatorInputStats> operatorInputStats,
            Map<String, OperatorHashCollisionsStats> operatorHashCollisionsStats,
            Optional<WindowOperatorStats> windowOperatorStats)
    {
        this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");

        this.planNodeWallTime = requireNonNull(planNodeWallTime, "planNodeWallTime is null");
        this.planNodeInputPositions = planNodeInputPositions;
        this.planNodeInputDataSize = planNodeInputDataSize;
        this.planNodeOutputPositions = planNodeOutputPositions;
        this.planNodeOutputDataSize = planNodeOutputDataSize;

        this.operatorInputStats = requireNonNull(operatorInputStats, "operatorInputStats is null");
        this.operatorHashCollisionsStats = requireNonNull(operatorHashCollisionsStats, "operatorHashCollisionsStats is null");
        this.windowOperatorStats = requireNonNull(windowOperatorStats, "windowOperatorStats is null");
    }

    private static double computedStdDev(double sumSquared, double sum, long n)
    {
        double average = sum / n;
        double variance = (sumSquared - 2 * sum * average + average * average * n) / n;
        // variance might be negative because of numeric inaccuracy, therefore we need to use max
        return sqrt(max(variance, 0d));
    }

    public PlanNodeId getPlanNodeId()
    {
        return planNodeId;
    }

    public Duration getPlanNodeWallTime()
    {
        return planNodeWallTime;
    }

    public Set<String> getOperatorTypes()
    {
        return operatorInputStats.keySet();
    }

    public long getPlanNodeInputPositions()
    {
        return planNodeInputPositions;
    }

    public DataSize getPlanNodeInputDataSize()
    {
        return planNodeInputDataSize;
    }

    public long getPlanNodeOutputPositions()
    {
        return planNodeOutputPositions;
    }

    public DataSize getPlanNodeOutputDataSize()
    {
        return planNodeOutputDataSize;
    }

    public Map<String, Double> getOperatorInputPositionsAverages()
    {
        return operatorInputStats.entrySet().stream()
                .collect(toMap(
                        Map.Entry::getKey,
                        entry -> (double) entry.getValue().getInputPositions() / operatorInputStats.get(entry.getKey()).getTotalDrivers()));
    }

    public Map<String, Double> getOperatorInputPositionsStdDevs()
    {
        return operatorInputStats.entrySet().stream()
                .collect(toMap(
                        Map.Entry::getKey,
                        entry -> computedStdDev(
                                entry.getValue().getSumSquaredInputPositions(),
                                entry.getValue().getInputPositions(),
                                entry.getValue().getTotalDrivers())));
    }

    public Map<String, Double> getOperatorHashCollisionsAverages()
    {
        return operatorHashCollisionsStats.entrySet().stream()
                .collect(toMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().getWeightedHashCollisions() / operatorInputStats.get(entry.getKey()).getInputPositions()));
    }

    public Map<String, Double> getOperatorHashCollisionsStdDevs()
    {
        return operatorHashCollisionsStats.entrySet().stream()
                .collect(toMap(
                        Map.Entry::getKey,
                        entry -> computedWeightedStdDev(
                                entry.getValue().getWeightedSumSquaredHashCollisions(),
                                entry.getValue().getWeightedHashCollisions(),
                                operatorInputStats.get(entry.getKey()).getInputPositions())));
    }

    private static double computedWeightedStdDev(double sumSquared, double sum, double totalWeight)
    {
        double average = sum / totalWeight;
        double variance = (sumSquared - 2 * sum * average) / totalWeight + average * average;
        // variance might be negative because of numeric inaccuracy, therefore we need to use max
        return sqrt(max(variance, 0d));
    }

    public Map<String, Double> getOperatorExpectedCollisionsAverages()
    {
        return operatorHashCollisionsStats.entrySet().stream()
                .collect(toMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().getWeightedExpectedHashCollisions() / operatorInputStats.get(entry.getKey()).getInputPositions()));
    }

    public Optional<WindowOperatorStats> getWindowOperatorStats()
    {
        return windowOperatorStats;
    }

    @Override
    public PlanNodeStats mergeWith(PlanNodeStats other)
    {
        checkArgument(planNodeId.equals(other.getPlanNodeId()), "planNodeIds do not match. %s != %s", planNodeId, other.getPlanNodeId());

        long planNodeInputPositions = this.planNodeInputPositions + other.planNodeInputPositions;
        DataSize planNodeInputDataSize = succinctBytes(this.planNodeInputDataSize.toBytes() + other.planNodeInputDataSize.toBytes());
        long planNodeOutputPositions = this.planNodeOutputPositions + other.planNodeOutputPositions;
        DataSize planNodeOutputDataSize = succinctBytes(this.planNodeOutputDataSize.toBytes() + other.planNodeOutputDataSize.toBytes());

        Map<String, OperatorInputStats> operatorInputStats = mergeMaps(this.operatorInputStats, other.operatorInputStats, OperatorInputStats::merge);
        Map<String, OperatorHashCollisionsStats> operatorHashCollisionsStats = mergeMaps(this.operatorHashCollisionsStats, other.operatorHashCollisionsStats, OperatorHashCollisionsStats::merge);
        Optional<WindowOperatorStats> windowNodeStats = Mergeable.merge(this.windowOperatorStats, other.windowOperatorStats);

        return new PlanNodeStats(
                planNodeId,
                new Duration(planNodeWallTime.toMillis() + other.getPlanNodeWallTime().toMillis(), MILLISECONDS),
                planNodeInputPositions, planNodeInputDataSize,
                planNodeOutputPositions, planNodeOutputDataSize,
                operatorInputStats,
                operatorHashCollisionsStats,
                windowNodeStats);
    }
}
