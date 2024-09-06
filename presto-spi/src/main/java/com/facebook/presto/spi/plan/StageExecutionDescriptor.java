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
package com.facebook.presto.spi.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static com.facebook.presto.common.Utils.checkArgument;
import static com.facebook.presto.spi.plan.StageExecutionDescriptor.StageExecutionStrategy.DYNAMIC_LIFESPAN_SCHEDULE_GROUPED_EXECUTION;
import static com.facebook.presto.spi.plan.StageExecutionDescriptor.StageExecutionStrategy.FIXED_LIFESPAN_SCHEDULE_GROUPED_EXECUTION;
import static com.facebook.presto.spi.plan.StageExecutionDescriptor.StageExecutionStrategy.RECOVERABLE_GROUPED_EXECUTION;
import static com.facebook.presto.spi.plan.StageExecutionDescriptor.StageExecutionStrategy.UNGROUPED_EXECUTION;
import static java.util.Collections.emptySet;
import static java.util.Collections.unmodifiableSet;
import static java.util.Objects.requireNonNull;

public class StageExecutionDescriptor
{
    private final StageExecutionStrategy stageExecutionStrategy;
    private final Set<PlanNodeId> groupedExecutionScanNodes;
    private final int totalLifespans;

    private StageExecutionDescriptor(StageExecutionStrategy stageExecutionStrategy, Set<PlanNodeId> groupedExecutionScanNodes, int totalLifespans)
    {
        switch (stageExecutionStrategy) {
            case UNGROUPED_EXECUTION:
                checkArgument(groupedExecutionScanNodes.isEmpty(), "groupedExecutionScanNodes must be empty if stage execution strategy is ungrouped execution");
                break;
            case FIXED_LIFESPAN_SCHEDULE_GROUPED_EXECUTION:
            case DYNAMIC_LIFESPAN_SCHEDULE_GROUPED_EXECUTION:
            case RECOVERABLE_GROUPED_EXECUTION:
                checkArgument(!groupedExecutionScanNodes.isEmpty(), "groupedExecutionScanNodes cannot be empty if stage execution strategy is grouped execution");
                break;
            default:
                throw new IllegalArgumentException("Unsupported stage execution strategy: " + stageExecutionStrategy);
        }

        this.stageExecutionStrategy = requireNonNull(stageExecutionStrategy, "stageExecutionStrategy is null");
        this.groupedExecutionScanNodes = requireNonNull(groupedExecutionScanNodes, "groupedExecutionScanNodes is null");
        this.totalLifespans = totalLifespans;
    }

    public static StageExecutionDescriptor ungroupedExecution()
    {
        return new StageExecutionDescriptor(UNGROUPED_EXECUTION, emptySet(), 1);
    }

    public static StageExecutionDescriptor fixedLifespanScheduleGroupedExecution(List<PlanNodeId> capableScanNodes, int totalLifespans)
    {
        requireNonNull(capableScanNodes, "capableScanNodes is null");
        checkArgument(!capableScanNodes.isEmpty(), "capableScanNodes cannot be empty if stage execution strategy is grouped execution");
        return new StageExecutionDescriptor(FIXED_LIFESPAN_SCHEDULE_GROUPED_EXECUTION, unmodifiableSet(new HashSet<>(capableScanNodes)), totalLifespans);
    }

    public static StageExecutionDescriptor dynamicLifespanScheduleGroupedExecution(List<PlanNodeId> capableScanNodes, int totalLifespans)
    {
        requireNonNull(capableScanNodes, "capableScanNodes is null");
        checkArgument(!capableScanNodes.isEmpty(), "capableScanNodes cannot be empty if stage execution strategy is grouped execution");
        return new StageExecutionDescriptor(DYNAMIC_LIFESPAN_SCHEDULE_GROUPED_EXECUTION, unmodifiableSet(new HashSet<>(capableScanNodes)), totalLifespans);
    }

    public static StageExecutionDescriptor recoverableGroupedExecution(List<PlanNodeId> capableScanNodes, int totalLifespans)
    {
        requireNonNull(capableScanNodes, "capableScanNodes is null");
        checkArgument(!capableScanNodes.isEmpty(), "capableScanNodes cannot be empty if stage execution strategy is grouped execution");
        return new StageExecutionDescriptor(RECOVERABLE_GROUPED_EXECUTION, unmodifiableSet(new HashSet<>(capableScanNodes)), totalLifespans);
    }

    public StageExecutionStrategy getStageExecutionStrategy()
    {
        return stageExecutionStrategy;
    }

    public boolean isStageGroupedExecution()
    {
        return stageExecutionStrategy != UNGROUPED_EXECUTION;
    }

    public boolean isDynamicLifespanSchedule()
    {
        return stageExecutionStrategy == DYNAMIC_LIFESPAN_SCHEDULE_GROUPED_EXECUTION || stageExecutionStrategy == RECOVERABLE_GROUPED_EXECUTION;
    }

    public boolean isScanGroupedExecution(PlanNodeId scanNodeId)
    {
        return groupedExecutionScanNodes.contains(scanNodeId);
    }

    public boolean isRecoverableGroupedExecution()
    {
        return stageExecutionStrategy == RECOVERABLE_GROUPED_EXECUTION;
    }

    @JsonCreator
    public static StageExecutionDescriptor jsonCreator(
            @JsonProperty("stageExecutionStrategy") StageExecutionStrategy stageExecutionStrategy,
            @JsonProperty("groupedExecutionScanNodes") Set<PlanNodeId> groupedExecutionCapableScanNodes,
            @JsonProperty("totalLifespans") int totalLifespans)
    {
        return new StageExecutionDescriptor(
                requireNonNull(stageExecutionStrategy, "stageExecutionStrategy is null"),
                unmodifiableSet(requireNonNull(groupedExecutionCapableScanNodes, "groupedExecutionScanNodes is null")),
                totalLifespans);
    }

    @JsonProperty("stageExecutionStrategy")
    public StageExecutionStrategy getJsonSerializableStageExecutionStrategy()
    {
        return stageExecutionStrategy;
    }

    @JsonProperty("groupedExecutionScanNodes")
    public Set<PlanNodeId> getJsonSerializableGroupedExecutionScanNodes()
    {
        return groupedExecutionScanNodes;
    }

    @JsonProperty("totalLifespans")
    public int getTotalLifespans()
    {
        return totalLifespans;
    }

    public enum StageExecutionStrategy
    {
        UNGROUPED_EXECUTION,
        FIXED_LIFESPAN_SCHEDULE_GROUPED_EXECUTION,
        DYNAMIC_LIFESPAN_SCHEDULE_GROUPED_EXECUTION,
        RECOVERABLE_GROUPED_EXECUTION,
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StageExecutionDescriptor other = (StageExecutionDescriptor) o;
        return Objects.equals(this.stageExecutionStrategy, other.stageExecutionStrategy) &&
                Objects.equals(this.groupedExecutionScanNodes, other.groupedExecutionScanNodes) &&
                this.totalLifespans == other.totalLifespans;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(stageExecutionStrategy, groupedExecutionScanNodes, totalLifespans);
    }

    @Override
    public String toString()
    {
        String sb = "StageExecutionDescriptor{" + "stageExecutionStrategy=" + stageExecutionStrategy +
                ", groupedExecutionScanNodes=" + groupedExecutionScanNodes +
                ", totalLifespans=" + totalLifespans +
                '}';
        return sb;
    }
}
