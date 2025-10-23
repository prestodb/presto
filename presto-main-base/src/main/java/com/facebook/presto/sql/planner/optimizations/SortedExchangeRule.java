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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.SortNode;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Optimizer rule that pushes sort operations down to exchange nodes where possible.
 * This optimization is beneficial for distributed queries where data needs to be sorted
 * after shuffling, as it allows sorting to happen during the shuffle operation itself
 * rather than requiring an explicit SortNode afterward.
 *
 * The rule looks for SortNode → ExchangeNode patterns and attempts to merge them into
 * a single sorted exchange node when:
 * - The exchange is a REMOTE REPARTITION exchange
 * - The exchange doesn't already have an ordering scheme
 * - All ordering variables are available in the exchange output
 */
public class SortedExchangeRule
        implements PlanOptimizer
{
    private static final Logger log = Logger.get(SortedExchangeRule.class);
    private final Metadata metadata;
    private boolean isEnabledForTesting;

    public SortedExchangeRule(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public void setEnabledForTesting(boolean isSet)
    {
        isEnabledForTesting = isSet;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return com.facebook.presto.SystemSessionProperties.isEnableSortedExchanges(session) || isEnabledForTesting;
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

            // Try to push sort down to exchange if the source is an exchange node
            Optional<PlanNode> sortedExchange = pushSortToExchangeIfPossible(source, node.getOrderingScheme());
            if (sortedExchange.isPresent()) {
                planChanged = true;
                return sortedExchange.get();
            }

            // If push-down not possible, keep the original sort
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
         * @param plan The plan node that needs sorting
         * @param orderingScheme The required ordering scheme
         * @return Optional containing the enhanced exchange node if push-down is possible, empty otherwise
         */
        private Optional<PlanNode> pushSortToExchangeIfPossible(PlanNode plan, OrderingScheme orderingScheme)
        {
            // Check if this is a suitable exchange node for sort push-down
            if (!(plan instanceof ExchangeNode)) {
                log.debug("Unable to push sorting to exchange because child is not exchange node");
                return Optional.empty();
            }

            ExchangeNode exchangeNode = (ExchangeNode) plan;

            // Only push sort down to REPARTITION exchanges in remote scope
            // These are the exchanges that involve shuffling data between executors
            if (exchangeNode.getType() != ExchangeNode.Type.REPARTITION ||
                    !exchangeNode.getScope().isRemote()) {
                log.debug("Unable to push sorting to exchange because it is not REMOTE REPARTITION exchange " +
                        "(Type: {}, Scope: {})", exchangeNode.getType(), exchangeNode.getScope());
                return Optional.empty();
            }

            // Don't push down if the exchange already has ordering requirements
            if (exchangeNode.getOrderingScheme().isPresent()) {
                log.debug("Unable to push sorting to exchange because exchange already has ordering scheme");
                return Optional.empty();
            }

            // Check if all variables in the ordering scheme are available in the exchange output
            if (!exchangeNode.getOutputVariables().containsAll(orderingScheme.getOrderByVariables())) {
                log.debug("Unable to push sorting to exchange because not all ordering variables are available in exchange output");
                return Optional.empty();
            }

            // Create a new sorted exchange node
            try {
                ExchangeNode sortedExchange = ExchangeNode.sortedPartitionedExchange(
                        idAllocator.getNextId(),
                        exchangeNode.getScope(),
                        exchangeNode.getSources().get(0),
                        exchangeNode.getPartitioningScheme().getPartitioning(),
                        exchangeNode.getPartitioningScheme().getHashColumn(),
                        orderingScheme);

                log.info("Successfully pushed sorting down to REMOTE REPARTITION exchange with orderingScheme=%s",
                        orderingScheme);
                return Optional.of(sortedExchange);
            }
            catch (Exception e) {
                log.warn("Failed to create sorted exchange: " + e.getMessage());
                // If creating sorted exchange fails, fall back to explicit sort
                return Optional.empty();
            }
        }
    }
}
