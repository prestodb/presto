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
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.HashMap;
import java.util.Map;

import static com.facebook.presto.cost.PlanNodeCost.UNKNOWN_COST;

/**
 * Simple implementation of CostCalculator. It make many arbitrary decisions (e.g filtering selectivity, join matching).
 * It serves POC purpose. To be replaced with more advanced implementation.
 */
@ThreadSafe
public class CoefficientBasedCostCalculator
        implements CostCalculator
{
    private static final Double FILTER_COEFFICIENT = 0.5;
    private static final Double JOIN_MATCHING_COEFFICIENT = 2.0;

    // todo some computation for outputSizeInBytes

    private final Metadata metadata;

    @Inject
    public CoefficientBasedCostCalculator(Metadata metadata)
    {
        this.metadata = metadata;
    }

    @Override
    public Map<PlanNode, PlanNodeCost> calculateCostForPlan(Session session, PlanNode planNode)
    {
        Visitor visitor = new Visitor(session);
        HashMap<PlanNode, PlanNodeCost> costMap = new HashMap<>();
        planNode.accept(visitor, costMap);
        return ImmutableMap.copyOf(costMap);
    }

    private class Visitor
            extends PlanVisitor<Map<PlanNode, PlanNodeCost>, Void>
    {
        private final Session session;

        public Visitor(Session session)
        {
            this.session = session;
        }

        @Override
        protected Void visitPlan(PlanNode node, Map<PlanNode, PlanNodeCost> context)
        {
            visitChildren(node, context);
            context.put(node, UNKNOWN_COST);
            return null;
        }

        private void visitChildren(PlanNode node, Map<PlanNode, PlanNodeCost> context)
        {
            for (PlanNode source : node.getSources()) {
                source.accept(this, context);
            }
        }

        @Override
        public Void visitFilter(FilterNode node, Map<PlanNode, PlanNodeCost> context)
        {
            visitChildren(node, context);
            PlanNodeCost sourceCost = context.get(node.getSource());
            PlanNodeCost filterCost = sourceCost
                    .mapOutputRowCount(value -> value * FILTER_COEFFICIENT);
            context.put(node, filterCost);
            return null;
        }

        @Override
        public Void visitProject(ProjectNode node, Map<PlanNode, PlanNodeCost> context)
        {
            return copySourceCost(node, context);
        }

        @Override
        public Void visitJoin(JoinNode node, Map<PlanNode, PlanNodeCost> context)
        {
            visitChildren(node, context);
            PlanNodeCost leftCost = context.get(node.getLeft());
            PlanNodeCost rightCost = context.get(node.getRight());

            PlanNodeCost.Builder joinCost = PlanNodeCost.builder();
            if (!leftCost.getOutputRowCount().isValueUnknown() && !rightCost.getOutputRowCount().isValueUnknown()) {
                double joinOutputRowCount = Math.max(leftCost.getOutputRowCount().getValue(), rightCost.getOutputRowCount().getValue()) * JOIN_MATCHING_COEFFICIENT;
                joinCost.setOutputRowCount(new Estimate(joinOutputRowCount));
            }

            context.put(node, joinCost.build());
            return null;
        }

        @Override
        public Void visitExchange(ExchangeNode node, Map<PlanNode, PlanNodeCost> context)
        {
            visitChildren(node, context);
            Estimate exchangeOutputRowCount = new Estimate(0);
            for (PlanNode child : node.getSources()) {
                PlanNodeCost childCost = context.get(child);
                if (childCost.getOutputRowCount().isValueUnknown()) {
                    exchangeOutputRowCount = Estimate.unknownValue();
                }
                else {
                    exchangeOutputRowCount = exchangeOutputRowCount.map(value -> value + childCost.getOutputRowCount().getValue());
                }
            }

            PlanNodeCost exchangeCost = PlanNodeCost.builder()
                    .setOutputRowCount(exchangeOutputRowCount)
                    .build();
            context.put(node, exchangeCost);
            return null;
        }

        @Override
        public Void visitTableScan(TableScanNode node, Map<PlanNode, PlanNodeCost> context)
        {
            PlanNodeCost.Builder tableScanCost = PlanNodeCost.builder();

            TableStatistics tableStatistics = metadata.getTableStatistics(session, node.getTable(), Constraint.alwaysTrue());
            tableScanCost.setOutputRowCount(tableStatistics.getRowCount());
            context.put(node, tableScanCost.build());
            return null;
        }

        @Override
        public Void visitValues(ValuesNode node, Map<PlanNode, PlanNodeCost> context)
        {
            Estimate valuesCount = new Estimate(node.getRows().size());
            PlanNodeCost valuesCost = PlanNodeCost.builder()
                    .setOutputRowCount(valuesCount)
                    .build();
            context.put(node, valuesCost);
            return null;
        }

        @Override
        public Void visitEnforceSingleRow(EnforceSingleRowNode node, Map<PlanNode, PlanNodeCost> context)
        {
            visitChildren(node, context);
            PlanNodeCost nodeCost = PlanNodeCost.builder()
                    .setOutputRowCount(new Estimate(1.0))
                    .build();
            context.put(node, nodeCost);
            return null;
        }

        @Override
        public Void visitSemiJoin(SemiJoinNode node, Map<PlanNode, PlanNodeCost> context)
        {
            visitChildren(node, context);
            PlanNodeCost sourceStatitics = context.get(node.getSource());
            PlanNodeCost semiJoinCost = sourceStatitics.mapOutputRowCount(rowCount -> rowCount * JOIN_MATCHING_COEFFICIENT);
            context.put(node, semiJoinCost);
            return null;
        }

        @Override
        public Void visitLimit(LimitNode node, Map<PlanNode, PlanNodeCost> context)
        {
            visitChildren(node, context);
            PlanNodeCost sourceCost = context.get(node.getSource());
            PlanNodeCost.Builder limitCost = PlanNodeCost.builder();
            if (sourceCost.getOutputRowCount().getValue() < node.getCount()) {
                limitCost.setOutputRowCount(sourceCost.getOutputRowCount());
            }
            else {
                limitCost.setOutputRowCount(new Estimate(node.getCount()));
            }
            context.put(node, limitCost.build());
            return null;
        }

        private Void copySourceCost(PlanNode node, Map<PlanNode, PlanNodeCost> context)
        {
            visitChildren(node, context);
            PlanNode source = Iterables.getOnlyElement(node.getSources());
            context.put(node, context.get(source));
            return null;
        }
    }
}
