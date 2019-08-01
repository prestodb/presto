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

import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.util.Mergeable;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import java.util.Map;
import java.util.Set;

import static com.facebook.presto.util.MoreMaps.mergeMaps;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.DataSize.succinctBytes;
import static java.lang.Double.max;
import static java.lang.Math.sqrt;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toMap;

public class PlanNodeStats
        implements Mergeable<PlanNodeStats>
{
    private final PlanNodeId planNodeId;

    private final Duration planNodeScheduledTime;
    private final Duration planNodeCpuTime;
    private final long planNodeInputPositions;
    private final DataSize planNodeInputDataSize;
    private final long planNodeRawInputPositions;
    private final DataSize planNodeRawInputDataSize;
    private final long planNodeOutputPositions;
    private final DataSize planNodeOutputDataSize;

    protected final Map<String, OperatorInputStats> operatorInputStats;

    PlanNodeStats(
            PlanNodeId planNodeId,
            Duration planNodeScheduledTime,
            Duration planNodeCpuTime,
            long planNodeInputPositions,
            DataSize planNodeInputDataSize,
            long planNodeRawInputPositions,
            DataSize planNodeRawInputDataSize,
            long planNodeOutputPositions,
            DataSize planNodeOutputDataSize,
            Map<String, OperatorInputStats> operatorInputStats)
    {
        this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");

        this.planNodeScheduledTime = requireNonNull(planNodeScheduledTime, "planNodeScheduledTime is null");
        this.planNodeCpuTime = requireNonNull(planNodeCpuTime, "planNodeCpuTime is null");
        this.planNodeInputPositions = planNodeInputPositions;
        this.planNodeInputDataSize = planNodeInputDataSize;
        this.planNodeRawInputPositions = planNodeRawInputPositions;
        this.planNodeRawInputDataSize = planNodeRawInputDataSize;
        this.planNodeOutputPositions = planNodeOutputPositions;
        this.planNodeOutputDataSize = planNodeOutputDataSize;

        this.operatorInputStats = requireNonNull(operatorInputStats, "operatorInputStats is null");
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

    public Duration getPlanNodeScheduledTime()
    {
        return planNodeScheduledTime;
    }

    public Duration getPlanNodeCpuTime()
    {
        return planNodeCpuTime;
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

    public long getPlanNodeRawInputPositions()
    {
        return planNodeRawInputPositions;
    }

    public DataSize getPlanNodeRawInputDataSize()
    {
        return planNodeRawInputDataSize;
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

    @Override
    public PlanNodeStats mergeWith(PlanNodeStats other)
    {
        checkArgument(planNodeId.equals(other.getPlanNodeId()), "planNodeIds do not match. %s != %s", planNodeId, other.getPlanNodeId());

        long planNodeInputPositions = this.planNodeInputPositions + other.planNodeInputPositions;
        DataSize planNodeInputDataSize = succinctBytes(this.planNodeInputDataSize.toBytes() + other.planNodeInputDataSize.toBytes());
        long planNodeRawInputPositions = this.planNodeRawInputPositions + other.planNodeRawInputPositions;
        DataSize planNodeRawInputDataSize = succinctBytes(this.planNodeRawInputDataSize.toBytes() + other.planNodeRawInputDataSize.toBytes());
        long planNodeOutputPositions = this.planNodeOutputPositions + other.planNodeOutputPositions;
        DataSize planNodeOutputDataSize = succinctBytes(this.planNodeOutputDataSize.toBytes() + other.planNodeOutputDataSize.toBytes());

        Map<String, OperatorInputStats> operatorInputStats = mergeMaps(this.operatorInputStats, other.operatorInputStats, OperatorInputStats::merge);

        return new PlanNodeStats(
                planNodeId,
                new Duration(planNodeScheduledTime.toMillis() + other.getPlanNodeScheduledTime().toMillis(), MILLISECONDS),
                new Duration(planNodeCpuTime.toMillis() + other.getPlanNodeCpuTime().toMillis(), MILLISECONDS),
                planNodeInputPositions, planNodeInputDataSize,
                planNodeRawInputPositions, planNodeRawInputDataSize,
                planNodeOutputPositions, planNodeOutputDataSize,
                operatorInputStats);
    }
}
