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
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.DomainTranslator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
    public Map<PlanNodeId, PlanNodeCost> calculateCostForPlan(Session session, Map<Symbol, Type> types, PlanNode planNode)
    {
        Visitor visitor = new Visitor(session, types);
        planNode.accept(visitor, null);
        return ImmutableMap.copyOf(visitor.getCosts());
    }

    private class Visitor
            extends PlanVisitor<PlanNodeCost, Void>
    {
        private final Session session;
        private final Map<PlanNodeId, PlanNodeCost> costs;
        private final Map<Symbol, Type> types;

        public Visitor(Session session, Map<Symbol, Type> types)
        {
            this.costs = new HashMap<>();
            this.session = session;
            this.types = ImmutableMap.copyOf(types);
        }

        public Map<PlanNodeId, PlanNodeCost> getCosts()
        {
            return ImmutableMap.copyOf(costs);
        }

        @Override
        protected PlanNodeCost visitPlan(PlanNode node, Void context)
        {
            visitSources(node);
            costs.put(node.getId(), UNKNOWN_COST);
            return UNKNOWN_COST;
        }

        @Override
        public PlanNodeCost visitOutput(OutputNode node, Void context)
        {
            return copySourceCost(node);
        }

        @Override
        public PlanNodeCost visitFilter(FilterNode node, Void context)
        {
            PlanNodeCost sourceCost;
            if (node.getSource() instanceof TableScanNode) {
                sourceCost = visitTableScanWithPredicate((TableScanNode) node.getSource(), node.getPredicate());
            }
            else {
                sourceCost = visitSource(node);
            }

            final double filterCoefficient = FILTER_COEFFICIENT;
            PlanNodeCost filterCost = sourceCost
                    .mapOutputRowCount(value -> value * filterCoefficient);
            costs.put(node.getId(), filterCost);
            return filterCost;
        }

        @Override
        public PlanNodeCost visitProject(ProjectNode node, Void context)
        {
            return copySourceCost(node);
        }

        @Override
        public PlanNodeCost visitJoin(JoinNode node, Void context)
        {
            List<PlanNodeCost> sourceCosts = visitSources(node);
            PlanNodeCost leftCost = sourceCosts.get(0);
            PlanNodeCost rightCost = sourceCosts.get(1);

            PlanNodeCost.Builder joinCost = PlanNodeCost.builder();
            if (!leftCost.getOutputRowCount().isValueUnknown() && !rightCost.getOutputRowCount().isValueUnknown()) {
                double rowCount = Math.max(leftCost.getOutputRowCount().getValue(), rightCost.getOutputRowCount().getValue()) * JOIN_MATCHING_COEFFICIENT;
                joinCost.setOutputRowCount(new Estimate(rowCount));
            }

            costs.put(node.getId(), joinCost.build());
            return joinCost.build();
        }

        @Override
        public PlanNodeCost visitExchange(ExchangeNode node, Void context)
        {
            List<PlanNodeCost> sourceCosts = visitSources(node);
            Estimate rowCount = new Estimate(0);
            for (PlanNodeCost sourceCost : sourceCosts) {
                if (sourceCost.getOutputRowCount().isValueUnknown()) {
                    rowCount = Estimate.unknownValue();
                }
                else {
                    rowCount = rowCount.map(value -> value + sourceCost.getOutputRowCount().getValue());
                }
            }

            PlanNodeCost exchangeCost = PlanNodeCost.builder()
                    .setOutputRowCount(rowCount)
                    .build();
            costs.put(node.getId(), exchangeCost);
            return exchangeCost;
        }

        @Override
        public PlanNodeCost visitTableScan(TableScanNode node, Void context)
        {
            return visitTableScanWithPredicate(node, BooleanLiteral.TRUE_LITERAL);
        }

        private PlanNodeCost visitTableScanWithPredicate(TableScanNode node, Expression predicate)
        {
            Constraint<ColumnHandle> constraint = getConstraint(node, predicate);

            TableStatistics tableStatistics = metadata.getTableStatistics(session, node.getTable(), constraint);
            PlanNodeCost tableScanCost = PlanNodeCost.builder()
                    .setOutputRowCount(tableStatistics.getRowCount())
                    .build();

            costs.put(node.getId(), tableScanCost);
            return tableScanCost;
        }

        private Constraint<ColumnHandle> getConstraint(TableScanNode node, Expression predicate)
        {
            DomainTranslator.ExtractionResult decomposedPredicate = DomainTranslator.fromPredicate(
                    metadata,
                    session,
                    predicate,
                    types);

            TupleDomain<ColumnHandle> simplifiedConstraint = decomposedPredicate.getTupleDomain()
                    .transform(node.getAssignments()::get)
                    .intersect(node.getCurrentConstraint());

            return new Constraint<>(simplifiedConstraint, bindings -> true);
        }

        @Override
        public PlanNodeCost visitValues(ValuesNode node, Void context)
        {
            Estimate valuesCount = new Estimate(node.getRows().size());
            PlanNodeCost valuesCost = PlanNodeCost.builder()
                    .setOutputRowCount(valuesCount)
                    .build();
            costs.put(node.getId(), valuesCost);
            return valuesCost;
        }

        @Override
        public PlanNodeCost visitEnforceSingleRow(EnforceSingleRowNode node, Void context)
        {
            visitSources(node);
            PlanNodeCost nodeCost = PlanNodeCost.builder()
                    .setOutputRowCount(new Estimate(1.0))
                    .build();
            costs.put(node.getId(), nodeCost);
            return nodeCost;
        }

        @Override
        public PlanNodeCost visitSemiJoin(SemiJoinNode node, Void context)
        {
            visitSources(node);
            PlanNodeCost sourceStatitics = costs.get(node.getSource().getId());
            PlanNodeCost semiJoinCost = sourceStatitics.mapOutputRowCount(rowCount -> rowCount * JOIN_MATCHING_COEFFICIENT);
            costs.put(node.getId(), semiJoinCost);
            return semiJoinCost;
        }

        @Override
        public PlanNodeCost visitLimit(LimitNode node, Void context)
        {
            PlanNodeCost sourceCost = visitSource(node);
            PlanNodeCost.Builder limitCost = PlanNodeCost.builder();
            if (sourceCost.getOutputRowCount().getValue() < node.getCount()) {
                limitCost.setOutputRowCount(sourceCost.getOutputRowCount());
            }
            else {
                limitCost.setOutputRowCount(new Estimate(node.getCount()));
            }
            costs.put(node.getId(), limitCost.build());
            return limitCost.build();
        }

        private PlanNodeCost copySourceCost(PlanNode node)
        {
            PlanNodeCost sourceCost = visitSource(node);
            costs.put(node.getId(), sourceCost);
            return sourceCost;
        }

        private List<PlanNodeCost> visitSources(PlanNode node)
        {
            return node.getSources().stream()
                    .map(source -> source.accept(this, null))
                    .collect(Collectors.toList());
        }

        private PlanNodeCost visitSource(PlanNode node)
        {
            return Iterables.getOnlyElement(visitSources(node));
        }
    }
}
