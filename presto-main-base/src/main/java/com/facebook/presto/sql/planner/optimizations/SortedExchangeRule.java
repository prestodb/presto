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
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.SortNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.isSortedExchangeEnabled;
import static java.util.Objects.requireNonNull;

/**
 * Optimizer rule that pushes sort operations down to exchange nodes where possible.
 * This optimization is beneficial for distributed queries where data needs to be sorted
 * after shuffling, as it allows sorting to happen during the shuffle operation itself
 * rather than requiring an explicit SortNode afterward.
 *
 * The rule looks for SortNode → ExchangeNode patterns and attempts to merge them into
 * a single sorted exchange node when:
 * - The exchange is a REMOTE exchange (REMOTE_STREAMING or REMOTE_MATERIALIZED)
 * - The exchange is either REPARTITION or GATHER (merging) type
 * - REPLICATE exchanges are excluded as sorting is not beneficial for broadcast operations
 * - The exchange doesn't already have an ordering scheme
 * - All ordering variables are available in the exchange output
 */
public class SortedExchangeRule
        implements PlanOptimizer
{
    private final boolean isPrestoSparkExecution;
    private boolean isEnabledForTesting;

    /**
     * Constructor that accepts a flag indicating whether this is a Presto Spark execution environment.
     *
     * @param isPrestoSparkExecution true if running in Presto Spark execution environment
     */
    public SortedExchangeRule(boolean isPrestoSparkExecution)
    {
        this.isPrestoSparkExecution = isPrestoSparkExecution;
    }

    @Override
    public void setEnabledForTesting(boolean isSet)
    {
        isEnabledForTesting = isSet;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return (isSortedExchangeEnabled(session) && isPrestoSparkExecution) || isEnabledForTesting;
    }

    @Override
    public PlanOptimizerResult optimize(
            PlanNode plan,
            Session session,
            TypeProvider types,
            VariableAllocator variableAllocator,
            PlanNodeIdAllocator idAllocator,
            WarningCollector warningCollector)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(types, "types is null");
        requireNonNull(variableAllocator, "variableAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");
        requireNonNull(warningCollector, "warningCollector is null");

        if (isEnabled(session)) {
            Rewriter rewriter = new Rewriter(idAllocator);
            PlanNode rewrittenPlan = SimplePlanRewriter.rewriteWith(rewriter, plan, null);
            return PlanOptimizerResult.optimizerResult(rewrittenPlan, rewriter.isPlanChanged());
        }
        return PlanOptimizerResult.optimizerResult(plan, false);
    }

    private static class Rewriter
            extends SimplePlanRewriter<Void>
    {
        private final PlanNodeIdAllocator idAllocator;
        private boolean planChanged;

        public Rewriter(PlanNodeIdAllocator idAllocator)
        {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
        }

        public boolean isPlanChanged()
        {
            return planChanged;
        }

        @Override
        public PlanNode visitSort(SortNode node, RewriteContext<Void> context)
        {
            PlanNode source = context.rewrite(node.getSource());

            // Try to push sort down to exchange if we can find one in the source tree
            Optional<PlanNode> sortedExchange = pushSortToExchangeIfPossible(source, node.getOrderingScheme());
            if (sortedExchange.isPresent()) {
                planChanged = true;
                return sortedExchange.get();
            }

            // If we can't optimize, return a new SortNode with the rewritten source
            // to preserve any optimizations that happened deeper in the tree
            if (source != node.getSource()) {
                return new SortNode(
                        node.getSourceLocation(),
                        node.getId(),
                        source,
                        node.getOrderingScheme(),
                        node.isPartial(),
                        node.getPartitionBy());
            }

            return node;
        }

        /**
         * Attempts to push the sorting operation down to the Exchange node if the plan structure allows it.
         * This is beneficial for distributed queries where we can sort during the shuffle operation instead of
         * adding an explicit SortNode.
         *
         * IMPORTANT: This only optimizes if the immediate child is an ExchangeNode. We do NOT look through
         * intermediate nodes to find exchanges deeper in the tree.
         *
         * @param plan The plan node that needs sorting (must be immediate child of SortNode)
         * @param orderingScheme The required ordering scheme
         * @return Optional containing the sorted exchange if push-down is possible, empty otherwise
         */
        private Optional<PlanNode> pushSortToExchangeIfPossible(PlanNode plan, OrderingScheme orderingScheme)
        {
            // Only optimize if the immediate child is an exchange node
            if (!(plan instanceof ExchangeNode)) {
                return Optional.empty();
            }

            ExchangeNode exchangeNode = (ExchangeNode) plan;

            // TODO: Future work. Support rewrite for exchange node with
            // multiple sources.
            if (exchangeNode.getSources().size() > 1) {
                return Optional.empty();
            }

            // Only push sort down to exchanges in remote scope
            // These are the exchanges that involve shuffling data between executors
            if (!exchangeNode.getScope().isRemote()) {
                return Optional.empty();
            }

            // Only push sort down to REPARTITION and GATHER (merging) exchanges
            // Do not support REPLICATED exchanges
            if (exchangeNode.getType() == ExchangeNode.Type.REPLICATE) {
                return Optional.empty();
            }

            // Validate that all ordering variables are present in the source's output
            // This ensures we don't create invalid sorted exchanges
            // TODO: Translate variable names in multiple node scenario
            //       tracking issue - https://github.com/prestodb/presto/issues/26602
            PlanNode source = exchangeNode.getSources().get(0);
            List<VariableReferenceExpression> sourceOutputVariables = source.getOutputVariables();
            for (VariableReferenceExpression orderingVariable : orderingScheme.getOrderByVariables()) {
                if (!sourceOutputVariables.contains(orderingVariable)) {
                    // Cannot push sort down if ordering references variables not in source output
                    return Optional.empty();
                }
            }

            // Create a new sorted exchange node
            ExchangeNode sortedExchange = ExchangeNode.sortedPartitionedExchange(
                    idAllocator.getNextId(),
                    exchangeNode.getScope(),
                    exchangeNode.getSources().get(0),
                    exchangeNode.getPartitioningScheme().getPartitioning(),
                    exchangeNode.getPartitioningScheme().getHashColumn(),
                    orderingScheme);

            return Optional.of(sortedExchange);
        }
    }
}
