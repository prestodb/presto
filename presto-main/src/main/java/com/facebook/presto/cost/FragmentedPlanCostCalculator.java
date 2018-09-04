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
import com.facebook.presto.execution.scheduler.NodeSchedulerConfig;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;

import java.util.function.IntSupplier;

import static com.facebook.presto.cost.CostCalculatorUsingExchanges.calculateExchangeCost;
import static com.facebook.presto.cost.CostCalculatorUsingExchanges.currentNumberOfWorkerNodes;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static java.util.Objects.requireNonNull;

public class FragmentedPlanCostCalculator
        implements CostCalculator
{
    private final CostCalculator delegate;
    private final FragmentedPlanSourceProvider sourceProvider;
    private final IntSupplier numberOfNodes;

    public FragmentedPlanCostCalculator(FragmentedPlanSourceProvider sourceProvider, CostCalculator delegate, InternalNodeManager nodeManager, NodeSchedulerConfig nodeSchedulerConfig)
    {
        this(delegate, sourceProvider, currentNumberOfWorkerNodes(nodeSchedulerConfig.isIncludeCoordinator(), nodeManager));
    }

    public FragmentedPlanCostCalculator(CostCalculator delegate, FragmentedPlanSourceProvider sourceProvider, IntSupplier numberOfNodes)
    {
        this.sourceProvider = requireNonNull(sourceProvider, "sourceProvider is null");
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.numberOfNodes = requireNonNull(numberOfNodes, "numberOfNodes is null");
    }

    @Override
    public PlanNodeCostEstimate calculateCost(PlanNode node, StatsProvider stats, Lookup lookup, Session session, TypeProvider types)
    {
        if (node instanceof RemoteSourceNode) {
            return calculateRemoteSourceNodeCost((RemoteSourceNode) node, stats, types);
        }
        else {
            return delegate.calculateCost(node, stats, lookup, session, types);
        }
    }

    private PlanNodeCostEstimate calculateRemoteSourceNodeCost(RemoteSourceNode node, StatsProvider stats, TypeProvider types)
    {
        PlanNodeCostEstimate costEstimate = PlanNodeCostEstimate.ZERO_COST;
        ExchangeNode.Type exchangeType = node.getExchangeType();
        for (PlanNode source : sourceProvider.getSources(node)) {
            PlanNodeCostEstimate exchangeCost = calculateExchangeCost(numberOfNodes.getAsInt(), stats.getStats(source), node.getOutputSymbols(), exchangeType, REMOTE, types);
            costEstimate.add(exchangeCost);
        }
        return costEstimate;
    }
}
