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
package com.facebook.presto.operator;

import com.facebook.presto.spi.plan.PlanNodeId;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Set;

import static com.facebook.presto.operator.StageExecutionDescriptor.StageExecutionStrategy.DYNAMIC_LIFESPAN_SCHEDULE_GROUPED_EXECUTION;
import static com.facebook.presto.operator.StageExecutionDescriptor.StageExecutionStrategy.FIXED_LIFESPAN_SCHEDULE_GROUPED_EXECUTION;
import static com.facebook.presto.operator.StageExecutionDescriptor.StageExecutionStrategy.UNGROUPED_EXECUTION;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class StageExecutionDescriptor
{
    private final StageExecutionStrategy stageExecutionStrategy;
    private final Set<PlanNodeId> groupedExecutionScanNodes;

    private StageExecutionDescriptor(StageExecutionStrategy stageExecutionStrategy, Set<PlanNodeId> groupedExecutionScanNodes)
    {
        switch (stageExecutionStrategy) {
            case UNGROUPED_EXECUTION:
                checkArgument(groupedExecutionScanNodes.isEmpty(), "groupedExecutionScanNodes must be empty if stage execution strategy is ungrouped execution");
                break;
            case FIXED_LIFESPAN_SCHEDULE_GROUPED_EXECUTION:
            case DYNAMIC_LIFESPAN_SCHEDULE_GROUPED_EXECUTION:
                checkArgument(!groupedExecutionScanNodes.isEmpty(), "groupedExecutionScanNodes cannot be empty if stage execution strategy is grouped execution");
                break;
            default:
                throw new IllegalArgumentException("Unsupported stage execution strategy: " + stageExecutionStrategy);
        }

        this.stageExecutionStrategy = requireNonNull(stageExecutionStrategy, "stageExecutionStrategy is null");
        this.groupedExecutionScanNodes = requireNonNull(groupedExecutionScanNodes, "groupedExecutionScanNodes is null");
    }

    public static StageExecutionDescriptor ungroupedExecution()
    {
        return new StageExecutionDescriptor(UNGROUPED_EXECUTION, ImmutableSet.of());
    }

    public static StageExecutionDescriptor fixedLifespanScheduleGroupedExecution(List<PlanNodeId> capableScanNodes)
    {
        requireNonNull(capableScanNodes, "capableScanNodes is null");
        checkArgument(!capableScanNodes.isEmpty(), "capableScanNodes cannot be empty if stage execution strategy is grouped execution");
        return new StageExecutionDescriptor(FIXED_LIFESPAN_SCHEDULE_GROUPED_EXECUTION, ImmutableSet.copyOf(capableScanNodes));
    }

    public static StageExecutionDescriptor dynamicLifespanScheduleGroupedExecution(List<PlanNodeId> capableScanNodes)
    {
        requireNonNull(capableScanNodes, "capableScanNodes is null");
        checkArgument(!capableScanNodes.isEmpty(), "capableScanNodes cannot be empty if stage execution strategy is grouped execution");
        return new StageExecutionDescriptor(DYNAMIC_LIFESPAN_SCHEDULE_GROUPED_EXECUTION, ImmutableSet.copyOf(capableScanNodes));
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
        return stageExecutionStrategy == DYNAMIC_LIFESPAN_SCHEDULE_GROUPED_EXECUTION;
    }

    public boolean isScanGroupedExecution(PlanNodeId scanNodeId)
    {
        return groupedExecutionScanNodes.contains(scanNodeId);
    }

    @JsonCreator
    public static StageExecutionDescriptor jsonCreator(
            @JsonProperty("stageExecutionStrategy") StageExecutionStrategy stageExecutionStrategy,
            @JsonProperty("groupedExecutionScanNodes") Set<PlanNodeId> groupedExecutionCapableScanNodes)
    {
        return new StageExecutionDescriptor(
                requireNonNull(stageExecutionStrategy, "stageExecutionStrategy is null"),
                ImmutableSet.copyOf(requireNonNull(groupedExecutionCapableScanNodes, "groupedExecutionScanNodes is null")));
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

    public enum StageExecutionStrategy
    {
        UNGROUPED_EXECUTION,
        FIXED_LIFESPAN_SCHEDULE_GROUPED_EXECUTION,
        DYNAMIC_LIFESPAN_SCHEDULE_GROUPED_EXECUTION
    }
}
