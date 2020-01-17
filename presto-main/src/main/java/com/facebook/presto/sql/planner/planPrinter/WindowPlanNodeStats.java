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
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

public class WindowPlanNodeStats
        extends PlanNodeStats
{
    private final WindowOperatorStats windowOperatorStats;

    public WindowPlanNodeStats(
            PlanNodeId planNodeId,
            Duration planNodeScheduledTime,
            Duration planNodeCpuTime,
            long planNodeInputPositions,
            DataSize planNodeInputDataSize,
            long planNodeRawInputPositions,
            DataSize planNodeRawInputDataSize,
            long planNodeOutputPositions,
            DataSize planNodeOutputDataSize,
            Map<String, OperatorInputStats> operatorInputStats,
            WindowOperatorStats windowOperatorStats)
    {
        super(planNodeId, planNodeScheduledTime, planNodeCpuTime, planNodeInputPositions, planNodeInputDataSize, planNodeRawInputPositions, planNodeRawInputDataSize, planNodeOutputPositions, planNodeOutputDataSize, operatorInputStats);
        this.windowOperatorStats = windowOperatorStats;
    }

    public WindowOperatorStats getWindowOperatorStats()
    {
        return windowOperatorStats;
    }

    @Override
    public PlanNodeStats mergeWith(PlanNodeStats other)
    {
        checkArgument(other instanceof WindowPlanNodeStats, "other is not an instanceof WindowPlanNodeStats");
        PlanNodeStats merged = super.mergeWith(other);

        return new WindowPlanNodeStats(
                merged.getPlanNodeId(),
                merged.getPlanNodeScheduledTime(),
                merged.getPlanNodeCpuTime(),
                merged.getPlanNodeInputPositions(),
                merged.getPlanNodeInputDataSize(),
                merged.getPlanNodeRawInputPositions(),
                merged.getPlanNodeRawInputDataSize(),
                merged.getPlanNodeOutputPositions(),
                merged.getPlanNodeOutputDataSize(),
                merged.operatorInputStats,
                windowOperatorStats);
    }
}
