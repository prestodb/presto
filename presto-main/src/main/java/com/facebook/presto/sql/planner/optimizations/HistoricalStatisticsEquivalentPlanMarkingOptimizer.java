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
import com.facebook.presto.cost.StatsCalculator;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.DistinctLimitNode;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.sql.planner.StatsEquivalentPlanNodeWithLimit;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.TopNRowNumberNode;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.getHistoryCanonicalPlanNodeLimit;
import static com.facebook.presto.SystemSessionProperties.trackHistoryBasedPlanStatisticsEnabled;
import static com.facebook.presto.SystemSessionProperties.useHistoryBasedPlanStatisticsEnabled;
import static java.util.Objects.requireNonNull;

public class HistoricalStatisticsEquivalentPlanMarkingOptimizer
        implements PlanOptimizer
{
    private static final Set<Class<? extends PlanNode>> LIMITING_NODES =
            ImmutableSet.of(TopNNode.class, LimitNode.class, DistinctLimitNode.class, TopNRowNumberNode.class);
    private final StatsCalculator statsCalculator;

    public HistoricalStatisticsEquivalentPlanMarkingOptimizer(StatsCalculator statsCalculator)
    {
        this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(types, "types is null");
        requireNonNull(variableAllocator, "variableAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");

        if (!useHistoryBasedPlanStatisticsEnabled(session) && !trackHistoryBasedPlanStatisticsEnabled(session)) {
            return plan;
        }

        // Find SUM(subtree_size^2) whenever we find a limiting plan node. This will be proportional to the extra cost
        // spent by History based optimization framework to canonicalize and hash the plan nodes.
        int historiesLimitingPlanNodeLimit = PlanNodeSearcher.searchFrom(plan)
                .recurseOnlyWhen(node -> !LIMITING_NODES.contains(node.getClass()))
                .where(node -> LIMITING_NODES.contains(node.getClass()))
                .findAll().stream()
                .mapToInt(node -> {
                    int size = PlanNodeSearcher.searchFrom(node).count();
                    return size * size;
                })
                .sum();

        if (historiesLimitingPlanNodeLimit > getHistoryCanonicalPlanNodeLimit(session)) {
            return plan;
        }

        // Assign 'statsEquivalentPlanNode' to plan nodes
        plan = SimplePlanRewriter.rewriteWith(new Rewriter(idAllocator), plan, new Context());

        // Fetch and cache history based statistics of all plan nodes, so no serial network calls happen later.
        statsCalculator.registerPlan(plan, session);
        return plan;
    }

    private static class Rewriter
            extends SimplePlanRewriter<Context>
    {
        private final PlanNodeIdAllocator idAllocator;

        public Rewriter(PlanNodeIdAllocator idAllocator)
        {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
        }

        @Override
        public PlanNode visitPlan(PlanNode node, RewriteContext<Context> context)
        {
            node = context.defaultRewrite(node, context.get());
            if (!node.getStatsEquivalentPlanNode().isPresent()) {
                PlanNode nodeWithLimit = node;
                // A LIMIT node can affect statistics of several children nodes, so let's put the limit node in the stats equivalent plan node.
                // For example:
                // `SELECT * FROM nation where <filter> LIMIT 5`
                // Filter node here will NOT have the same statistics as `SELECT * FROM nation where <filter>`, because
                // LIMIT node will stop the operator once it gets 5 rows. Hence, we cannot reuse these stats for other queries.
                // However, we can still use it if the whole query with same limit appears again. Hence, we include the limit node in the
                // stats equivalent form of filter node, so its stats are only used in queries where there is a limit node above it.
                if (!context.get().getLimits().isEmpty()) {
                    nodeWithLimit = new StatsEquivalentPlanNodeWithLimit(idAllocator.getNextId(), node, context.get().getLimits().peekFirst());
                }
                node = node.assignStatsEquivalentPlanNode(Optional.of(nodeWithLimit));
            }
            return node;
        }

        @Override
        public PlanNode visitLimit(LimitNode node, RewriteContext<Context> context)
        {
            return visitLimitingNode(node, context);
        }

        @Override
        public PlanNode visitTopN(TopNNode node, RewriteContext<Context> context)
        {
            return visitLimitingNode(node, context);
        }

        @Override
        public PlanNode visitTopNRowNumber(TopNRowNumberNode node, RewriteContext<Context> context)
        {
            return visitLimitingNode(node, context);
        }

        @Override
        public PlanNode visitDistinctLimit(DistinctLimitNode node, RewriteContext<Context> context)
        {
            return visitLimitingNode(node, context);
        }

        private PlanNode visitLimitingNode(PlanNode node, RewriteContext<Context> context)
        {
            context.get().getLimits().addLast(node);
            PlanNode result = visitPlan(node, context);
            context.get().getLimits().removeLast();
            return result;
        }
    }

    private static class Context
    {
        private final Deque<PlanNode> limits = new ArrayDeque<>();

        public Deque<PlanNode> getLimits()
        {
            return limits;
        }
    }
}
