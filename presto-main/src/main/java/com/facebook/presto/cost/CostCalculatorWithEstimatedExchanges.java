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

package com.facebook.presto.cost;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.Map;

import static com.facebook.presto.cost.PlanNodeCostEstimate.ZERO_COST;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPLICATE;
import static java.util.Objects.requireNonNull;

/**
 * This is a wrapper class around CostCalculator that estimates ExchangeNodes cost.
 */
@ThreadSafe
public class CostCalculatorWithEstimatedExchanges
        implements CostCalculator
{
    private final CostCalculator costCalculator;
    private final int numberOfNodes;

    @Inject
    public CostCalculatorWithEstimatedExchanges(CostCalculator costCalculator, InternalNodeManager nodeManager)
    {
        this(costCalculator, nodeManager.getAllNodes().getActiveNodes().size());
    }

    public CostCalculatorWithEstimatedExchanges(CostCalculator costCalculator, int numberOfNodes)
    {
        this.costCalculator = requireNonNull(costCalculator, "costCalculator is null");
        this.numberOfNodes = numberOfNodes;
    }

    @Override
    public PlanNodeCostEstimate calculateCost(PlanNode planNode, Lookup lookup, Session session, Map<Symbol, Type> types)
    {
        ExchangeCostEstimator exchangeCostEstimator = new ExchangeCostEstimator(
                session,
                types,
                lookup,
                numberOfNodes);
        PlanNodeCostEstimate estimatedExchangeCost = planNode.accept(exchangeCostEstimator, null);

        return costCalculator.calculateCost(planNode, lookup, session, types).add(estimatedExchangeCost);
    }

    private class ExchangeCostEstimator
            extends PlanVisitor<PlanNodeCostEstimate, Void>
    {
        private final Session session;
        private final Map<Symbol, Type> types;
        private final Lookup lookup;
        private final int numberOfNodes;

        public ExchangeCostEstimator(Session session, Map<Symbol, Type> types, Lookup lookup, int numberOfNodes)
        {
            this.session = requireNonNull(session, "session is null");
            this.types = requireNonNull(types, "types is null");
            this.lookup = lookup;
            this.numberOfNodes = numberOfNodes;
        }

        @Override
        protected PlanNodeCostEstimate visitPlan(PlanNode node, Void context)
        {
            return ZERO_COST;
        }

        @Override
        public PlanNodeCostEstimate visitAggregation(AggregationNode node, Void context)
        {
            return CostCalculatorUsingExchanges.calculateExchangeCost(
                    numberOfNodes,
                    getStats(node.getSource()),
                    REPARTITION,
                    REMOTE);
        }

        @Override
        public PlanNodeCostEstimate visitJoin(JoinNode node, Void context)
        {
            return calculateJoinCost(
                    node.getLeft(),
                    node.getRight(),
                    node.getDistributionType().orElse(JoinNode.DistributionType.PARTITIONED).equals(JoinNode.DistributionType.REPLICATED));
        }

        @Override
        public PlanNodeCostEstimate visitSemiJoin(SemiJoinNode node, Void context)
        {
            return calculateJoinCost(
                    node.getSource(),
                    node.getFilteringSource(),
                    node.getDistributionType().orElse(SemiJoinNode.DistributionType.PARTITIONED).equals(SemiJoinNode.DistributionType.REPLICATED));
        }

        private PlanNodeCostEstimate calculateJoinCost(PlanNode probe, PlanNode build, boolean replicated)
        {
            if (replicated) {
                return CostCalculatorUsingExchanges.calculateExchangeCost(
                        numberOfNodes,
                        getStats(build),
                        REPLICATE,
                        REMOTE);
            }
            else {
                PlanNodeCostEstimate probeCost = CostCalculatorUsingExchanges.calculateExchangeCost(
                        numberOfNodes,
                        getStats(probe),
                        REPARTITION,
                        REMOTE);
                PlanNodeCostEstimate buildCost = CostCalculatorUsingExchanges.calculateExchangeCost(
                        numberOfNodes,
                        getStats(build),
                        REPARTITION,
                        REMOTE);
                return probeCost.add(buildCost);
            }
        }

        private PlanNodeStatsEstimate getStats(PlanNode node)
        {
            return lookup.getStats(node, session, types);
        }
    }
}
