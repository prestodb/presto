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
import com.facebook.presto.spi.eventlistener.PlanOptimizerInformation;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.DistinctLimitNode;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.sql.planner.StatsEquivalentPlanNodeWithLimit;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.TopNRowNumberNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.getHistoryBasedOptimizerTimeoutLimit;
import static com.facebook.presto.SystemSessionProperties.getHistoryCanonicalPlanNodeLimit;
import static com.facebook.presto.SystemSessionProperties.restrictHistoryBasedOptimizationToComplexQuery;
import static com.facebook.presto.SystemSessionProperties.trackHistoryBasedPlanStatisticsEnabled;
import static com.facebook.presto.SystemSessionProperties.useHistoryBasedPlanStatisticsEnabled;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class HistoricalStatisticsEquivalentPlanMarkingOptimizer
        implements PlanOptimizer
{
    private static final Set<Class<? extends PlanNode>> LIMITING_NODES =
            ImmutableSet.of(TopNNode.class, LimitNode.class, DistinctLimitNode.class, TopNRowNumberNode.class);
    private static final List<Class<? extends PlanNode>> PRECOMPUTE_PLAN_NODES = ImmutableList.of(JoinNode.class, SemiJoinNode.class, AggregationNode.class);
    private final StatsCalculator statsCalculator;
    private boolean isEnabledForTesting;

    public HistoricalStatisticsEquivalentPlanMarkingOptimizer(StatsCalculator statsCalculator)
    {
        this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
    }

    @Override
    public void setEnabledForTesting(boolean isSet)
    {
        isEnabledForTesting = isSet;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isEnabledForTesting || useHistoryBasedPlanStatisticsEnabled(session) || trackHistoryBasedPlanStatisticsEnabled(session);
    }

    @Override
    public PlanOptimizerResult optimize(PlanNode plan, Session session, TypeProvider types, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(types, "types is null");
        requireNonNull(variableAllocator, "variableAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");

        if (!isEnabled(session)) {
            return PlanOptimizerResult.optimizerResult(plan, false);
        }

        // Only enable history based optimization when plan has a join/aggregation.
        if (restrictHistoryBasedOptimizationToComplexQuery(session) &&
                !PlanNodeSearcher.searchFrom(plan).where(node -> PRECOMPUTE_PLAN_NODES.stream().anyMatch(clazz -> clazz.isInstance(node))).matches()) {
            return PlanOptimizerResult.optimizerResult(plan, false);
        }

        long startTimeInNano = System.nanoTime();
        long timeoutInMilliseconds = getHistoryBasedOptimizerTimeoutLimit(session).toMillis();

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
            return PlanOptimizerResult.optimizerResult(plan, false);
        }

        // Assign 'statsEquivalentPlanNode' to plan nodes
        PlanNode newPlan = SimplePlanRewriter.rewriteWith(new Rewriter(idAllocator), plan, new Context());
        // Return original plan if timeout
        if (checkTimeOut(startTimeInNano, timeoutInMilliseconds)) {
            logOptimizerFailure(session);
            return PlanOptimizerResult.optimizerResult(plan, false);
        }

        // Fetch and cache history based statistics of all plan nodes, so no serial network calls happen later.
        boolean registerSucceed = statsCalculator.registerPlan(newPlan, session, startTimeInNano, timeoutInMilliseconds);
        // Return original plan if timeout or registration not successful
        if (checkTimeOut(startTimeInNano, timeoutInMilliseconds) || !registerSucceed) {
            logOptimizerFailure(session);
            return PlanOptimizerResult.optimizerResult(plan, false);
        }
        return PlanOptimizerResult.optimizerResult(newPlan, true);
    }

    private boolean checkTimeOut(long startTimeInNano, long timeoutInMilliseconds)
    {
        return NANOSECONDS.toMillis(System.nanoTime() - startTimeInNano) > timeoutInMilliseconds;
    }

    private void logOptimizerFailure(Session session)
    {
        session.getOptimizerInformationCollector().addInformation(
                new PlanOptimizerInformation(HistoricalStatisticsEquivalentPlanMarkingOptimizer.class.getSimpleName(), false, Optional.empty(), Optional.of(true), Optional.empty(), Optional.empty()));
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
