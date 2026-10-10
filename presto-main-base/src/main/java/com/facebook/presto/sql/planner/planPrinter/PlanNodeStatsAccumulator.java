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

import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.operator.DynamicFilterStats;
import com.facebook.presto.spi.plan.PlanNodeId;

import java.util.HashMap;
import java.util.Map;

import static com.facebook.presto.sql.planner.planPrinter.PlanNodeStatsSummarizer.getPlanNodeStats;

/**
 * Merges the plan node stats of tasks added one at a time. Adding tasks in a given order gives the
 * same result as {@link PlanNodeStatsSummarizer#aggregateTaskStats} on a list in that order.
 */
public class PlanNodeStatsAccumulator
{
    private final boolean copyDynamicFilterStats;
    private final Map<PlanNodeId, PlanNodeStats> planNodeStats = new HashMap<>();

    /**
     * @param copyDynamicFilterStats whether to merge copies of the {@link DynamicFilterStats} of the
     * operators of the added tasks, leaving the tasks unmodified, rather than merging into the
     * instances the operators hold as {@link PlanNodeStatsSummarizer#aggregateTaskStats} does
     */
    public PlanNodeStatsAccumulator(boolean copyDynamicFilterStats)
    {
        this.copyDynamicFilterStats = copyDynamicFilterStats;
    }

    public void add(TaskInfo taskInfo)
    {
        for (PlanNodeStats stats : getPlanNodeStats(taskInfo.getStats(), copyDynamicFilterStats)) {
            add(stats);
        }
    }

    void add(PlanNodeStats stats)
    {
        planNodeStats.merge(stats.getPlanNodeId(), stats, PlanNodeStats::mergeWith);
    }

    public Map<PlanNodeId, PlanNodeStats> build()
    {
        return planNodeStats;
    }
}
