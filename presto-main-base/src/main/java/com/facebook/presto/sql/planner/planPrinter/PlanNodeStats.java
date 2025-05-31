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

import com.facebook.airlift.units.DataSize;
import com.facebook.airlift.units.Duration;
import com.facebook.presto.operator.DynamicFilterStats;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.util.Mergeable;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.airlift.units.DataSize.succinctBytes;
import static com.facebook.presto.util.MoreMaps.mergeMaps;
import static com.google.common.base.Preconditions.checkArgument;
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
    private final Duration planNodeBlockedWallTime;
    private final Duration planNodeAddInputWallTime;
    private final Duration planNodeGetOutputWallTime;
    private final Duration planNodeFinishWallTime;
    private final long planNodeInputPositions;
    private final DataSize planNodeInputDataSize;
    private final long planNodeRawInputPositions;
    private final DataSize planNodeRawInputDataSize;
    private final long planNodeOutputPositions;
    private final DataSize planNodeOutputDataSize;

    private final DataSize planNodePeakMemorySize;
    protected final Map<String, OperatorInputStats> operatorInputStats;
    private final long planNodeNullJoinBuildKeyCount;
    private final long planNodeJoinBuildKeyCount;
    private final long planNodeNullJoinProbeKeyCount;
    private final long planNodeJoinProbeKeyCount;
    private final Optional<DynamicFilterStats> dynamicFilterStats;

    @JsonCreator
    public PlanNodeStats(
            @JsonProperty("planNodeId") PlanNodeId planNodeId,
            @JsonProperty("planNodeScheduledTime") Duration planNodeScheduledTime,
            @JsonProperty("planNodeCpuTime") Duration planNodeCpuTime,
            @JsonProperty("planNodeBlockedWallTime") Duration planNodeBlockedWallTime,
            @JsonProperty("planNodeAddInputWallTime") Duration planNodeAddInputWallTime,
            @JsonProperty("planNodeGetOutputWallTime") Duration planNodeGetOutputWallTime,
            @JsonProperty("planNodeFinishWallTime") Duration planNodeFinishWallTime,
            @JsonProperty("planNodeInputPositions") long planNodeInputPositions,
            @JsonProperty("planNodeInputDataSize") DataSize planNodeInputDataSize,
            @JsonProperty("planNodeRawInputPositions") long planNodeRawInputPositions,
            @JsonProperty("planNodeRawInputDataSize") DataSize planNodeRawInputDataSize,
            @JsonProperty("planNodeOutputPositions") long planNodeOutputPositions,
            @JsonProperty("planNodeOutputDataSize") DataSize planNodeOutputDataSize,
            @JsonProperty("planNodePeakMemorySize") DataSize planNodePeakMemorySize,
            @JsonProperty("operatorInputStats") Map<String, OperatorInputStats> operatorInputStats,
            @JsonProperty("planNodeNullJoinBuildKeyCount") long planNodeNullJoinBuildKeyCount,
            @JsonProperty("planNodeJoinBuildKeyCount") long planNodeJoinBuildKeyCount,
            @JsonProperty("planNodeNullJoinProbeKeyCount") long planNodeNullJoinProbeKeyCount,
            @JsonProperty("planNodeJoinProbeKeyCount") long planNodeJoinProbeKeyCount,
            @JsonProperty("dynamicFilterStats") Optional<DynamicFilterStats> dynamicFilterStats)
    {
        this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");

        this.planNodeScheduledTime = requireNonNull(planNodeScheduledTime, "planNodeScheduledTime is null");
        this.planNodeCpuTime = requireNonNull(planNodeCpuTime, "planNodeCpuTime is null");
        this.planNodeBlockedWallTime = requireNonNull(planNodeBlockedWallTime, "planNodeBlockedWallTime is null");
        this.planNodeAddInputWallTime = requireNonNull(planNodeAddInputWallTime, "planNodeAddInputWallTime is null");
        this.planNodeGetOutputWallTime = requireNonNull(planNodeGetOutputWallTime, "planNodeGetOutputWallTime is null");
        this.planNodeFinishWallTime = requireNonNull(planNodeFinishWallTime, "planNodeFinishWallTime is null");
        this.planNodeInputPositions = planNodeInputPositions;
        this.planNodeInputDataSize = planNodeInputDataSize;
        this.planNodeRawInputPositions = planNodeRawInputPositions;
        this.planNodeRawInputDataSize = planNodeRawInputDataSize;
        this.planNodeOutputPositions = planNodeOutputPositions;
        this.planNodeOutputDataSize = planNodeOutputDataSize;

        this.operatorInputStats = requireNonNull(operatorInputStats, "operatorInputStats is null");
        this.planNodePeakMemorySize = planNodePeakMemorySize;
        this.planNodeNullJoinBuildKeyCount = planNodeNullJoinBuildKeyCount;
        this.planNodeJoinBuildKeyCount = planNodeJoinBuildKeyCount;
        this.planNodeNullJoinProbeKeyCount = planNodeNullJoinProbeKeyCount;
        this.planNodeJoinProbeKeyCount = planNodeJoinProbeKeyCount;
        this.dynamicFilterStats = dynamicFilterStats;
    }

    private static double computedStdDev(double sumSquared, double sum, long n)
    {
        double average = sum / n;
        double variance = (sumSquared - 2 * sum * average + average * average * n) / n;
        // variance might be negative because of numeric inaccuracy, therefore we need to use max
        return sqrt(max(variance, 0d));
    }

    @JsonProperty
    public PlanNodeId getPlanNodeId()
    {
        return planNodeId;
    }

    @JsonProperty
    public Duration getPlanNodeScheduledTime()
    {
        return planNodeScheduledTime;
    }

    @JsonProperty
    public Duration getPlanNodeCpuTime()
    {
        return planNodeCpuTime;
    }

    @JsonProperty
    public Duration getPlanNodeBlockedWallTime()
    {
        return planNodeBlockedWallTime;
    }

    @JsonProperty
    public Duration getPlanNodeAddInputWallTime()
    {
        return planNodeAddInputWallTime;
    }

    @JsonProperty
    public Duration getPlanNodeGetOutputWallTime()
    {
        return planNodeGetOutputWallTime;
    }

    @JsonProperty
    public Duration getPlanNodeFinishWallTime()
    {
        return planNodeFinishWallTime;
    }

    @JsonProperty
    public Map<String, OperatorInputStats> getOperatorInputStats()
    {
        // no need to copy, just prevent modifications
        return Collections.unmodifiableMap(operatorInputStats);
    }

    public Set<String> getOperatorTypes()
    {
        return operatorInputStats.keySet();
    }

    @JsonProperty
    public long getPlanNodeInputPositions()
    {
        return planNodeInputPositions;
    }

    @JsonProperty
    public DataSize getPlanNodeInputDataSize()
    {
        return planNodeInputDataSize;
    }

    @JsonProperty
    public long getPlanNodeRawInputPositions()
    {
        return planNodeRawInputPositions;
    }

    @JsonProperty
    public DataSize getPlanNodeRawInputDataSize()
    {
        return planNodeRawInputDataSize;
    }

    @JsonProperty
    public long getPlanNodeOutputPositions()
    {
        return planNodeOutputPositions;
    }

    @JsonProperty
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

    @JsonProperty
    public DataSize getPlanNodePeakMemorySize()
    {
        return planNodePeakMemorySize;
    }

    @JsonProperty
    public long getPlanNodeNullJoinBuildKeyCount()
    {
        return planNodeNullJoinBuildKeyCount;
    }

    @JsonProperty
    public long getPlanNodeJoinBuildKeyCount()
    {
        return planNodeJoinBuildKeyCount;
    }

    @JsonProperty
    public long getPlanNodeNullJoinProbeKeyCount()
    {
        return planNodeNullJoinProbeKeyCount;
    }

    @JsonProperty
    public long getPlanNodeJoinProbeKeyCount()
    {
        return planNodeJoinProbeKeyCount;
    }

    public Optional<DynamicFilterStats> getDynamicFilterStats()
    {
        return dynamicFilterStats;
    }

    public static Optional<DynamicFilterStats> mergeDynamicFilterStats(Optional<DynamicFilterStats> stats1, Optional<DynamicFilterStats> stats2)
    {
        Optional<DynamicFilterStats> optionalDynamicFilterStats = Optional.empty();
        if (stats1.isPresent()) {
            DynamicFilterStats dynamicFilterStats = stats1.get();
            stats2.ifPresent(dynamicFilterStats::mergeWith);
            optionalDynamicFilterStats = Optional.of(dynamicFilterStats);
        }
        else if (stats2.isPresent()) {
            optionalDynamicFilterStats = Optional.of(stats2.get());
        }
        return optionalDynamicFilterStats;
    }

    @Override
    public PlanNodeStats mergeWith(PlanNodeStats other)
    {
        checkArgument(planNodeId.equals(other.getPlanNodeId()), "planNodeIds do not match. %s != %s", planNodeId, other.getPlanNodeId());

        long planNodeInputPositions = this.planNodeInputPositions + other.planNodeInputPositions;
        DataSize planNodeInputDataSize = succinctBytes((long) ((double) this.planNodeInputDataSize.toBytes() + (double) other.planNodeInputDataSize.toBytes()));
        long planNodeRawInputPositions = this.planNodeRawInputPositions + other.planNodeRawInputPositions;
        DataSize planNodeRawInputDataSize = succinctBytes((long) ((double) this.planNodeRawInputDataSize.toBytes() + (double) other.planNodeRawInputDataSize.toBytes()));
        long planNodeOutputPositions = this.planNodeOutputPositions + other.planNodeOutputPositions;
        DataSize planNodeOutputDataSize = succinctBytes((long) ((double) this.planNodeOutputDataSize.toBytes() + (double) other.planNodeOutputDataSize.toBytes()));
        DataSize planNodePeakMemorySize = succinctBytes(Math.max(this.planNodePeakMemorySize.toBytes(), other.planNodePeakMemorySize.toBytes()));

        Map<String, OperatorInputStats> operatorInputStats = mergeMaps(this.operatorInputStats, other.operatorInputStats, OperatorInputStats::merge);
        long planNodeNullJoinBuildKeyCount = this.planNodeNullJoinBuildKeyCount + other.planNodeNullJoinBuildKeyCount;
        long planNodeJoinBuildKeyCount = this.planNodeJoinBuildKeyCount + other.planNodeJoinBuildKeyCount;
        long planNodeNullJoinProbeKeyCount = this.planNodeNullJoinProbeKeyCount + other.planNodeNullJoinProbeKeyCount;
        long planNodeJoinProbeKeyCount = this.planNodeJoinProbeKeyCount + other.planNodeJoinProbeKeyCount;
        Optional<DynamicFilterStats> optionalDynamicFilterStats = mergeDynamicFilterStats(this.dynamicFilterStats, other.dynamicFilterStats);

        return new PlanNodeStats(
                planNodeId,
                new Duration(planNodeScheduledTime.toMillis() + other.getPlanNodeScheduledTime().toMillis(), MILLISECONDS),
                new Duration(planNodeCpuTime.toMillis() + other.getPlanNodeCpuTime().toMillis(), MILLISECONDS),
                new Duration(planNodeBlockedWallTime.toMillis() + other.getPlanNodeBlockedWallTime().toMillis(), MILLISECONDS),
                new Duration(planNodeAddInputWallTime.toMillis() + other.getPlanNodeAddInputWallTime().toMillis(), MILLISECONDS),
                new Duration(planNodeGetOutputWallTime.toMillis() + other.getPlanNodeGetOutputWallTime().toMillis(), MILLISECONDS),
                new Duration(planNodeFinishWallTime.toMillis() + other.getPlanNodeFinishWallTime().toMillis(), MILLISECONDS),
                planNodeInputPositions, planNodeInputDataSize,
                planNodeRawInputPositions, planNodeRawInputDataSize,
                planNodeOutputPositions, planNodeOutputDataSize,
                planNodePeakMemorySize,
                operatorInputStats,

                planNodeNullJoinBuildKeyCount,
                planNodeJoinBuildKeyCount,
                planNodeNullJoinProbeKeyCount,
                planNodeJoinProbeKeyCount,
                optionalDynamicFilterStats);
    }
}
