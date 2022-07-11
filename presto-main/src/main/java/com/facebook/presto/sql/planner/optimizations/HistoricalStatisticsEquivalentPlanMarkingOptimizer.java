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
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.useExternalPlanStatisticsEnabled;
import static java.util.Objects.requireNonNull;

public class HistoricalStatisticsEquivalentPlanMarkingOptimizer
        implements PlanOptimizer
{
    public HistoricalStatisticsEquivalentPlanMarkingOptimizer() {}

    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, PlanVariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(types, "types is null");
        requireNonNull(variableAllocator, "variableAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");

        if (!useExternalPlanStatisticsEnabled(session)) {
            return plan;
        }

        // Assign 'statsEquivalentPlanNode' to plan nodes
        plan = SimplePlanRewriter.rewriteWith(new Rewriter(), plan);

        // TODO: Fetch and cache history based statistics of all plan nodes, so no serial network calls happen later.
        return plan;
    }

    private static class Rewriter
            extends SimplePlanRewriter<Void>
    {
        @Override
        public PlanNode visitPlan(PlanNode node, RewriteContext<Void> context)
        {
            return context.defaultRewrite(node.assignStatsEquivalentPlanNode(Optional.of(node)), context.get());
        }
    }
}
