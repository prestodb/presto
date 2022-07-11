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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.Session;
import com.facebook.presto.cost.HistoryBasedPlanStatisticsCalculator;
import com.facebook.presto.cost.StatsCalculator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.TypeProvider;

import java.util.Optional;

public class HistoricalStatisticsCachingPlanOptimizer
        implements PlanOptimizer
{
    private final Optional<HistoryBasedPlanStatisticsCalculator> historyBasedPlanStatisticsCalculator;

    public HistoricalStatisticsCachingPlanOptimizer(StatsCalculator statsCalculator)
    {
        historyBasedPlanStatisticsCalculator = statsCalculator instanceof HistoryBasedPlanStatisticsCalculator ? Optional.of((HistoryBasedPlanStatisticsCalculator) statsCalculator) : Optional.empty();
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, PlanVariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        if (!historyBasedPlanStatisticsCalculator.isPresent()) {
            return plan;
        }
        historyBasedPlanStatisticsCalculator.get().cacheHashBasedStatistics(session, plan, types);
        return plan;
    }
}
