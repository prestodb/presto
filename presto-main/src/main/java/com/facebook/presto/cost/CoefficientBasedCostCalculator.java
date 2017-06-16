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
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableMap;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.Map;

import static com.facebook.presto.cost.PlanNodeCost.UNKNOWN_COST;

/**
 * Simple implementation of StatsCalculator. It make many arbitrary decisions (e.g filtering selectivity, join matching).
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
    public PlanNodeCost calculateCost(
            PlanNode planNode,
            Lookup lookup,
            Session session,
            Map<Symbol, Type> types)
    {
        Visitor visitor = new Visitor(lookup, session, types);
        return planNode.accept(visitor, null);
    }

    private class Visitor
            extends PlanVisitor<PlanNodeCost, Void>
    {
        private final Lookup lookup;
        private final Session session;
        private final Map<Symbol, Type> types;

        public Visitor(Lookup lookup, Session session, Map<Symbol, Type> types)
        {
            this.lookup = lookup;
            this.session = session;
            this.types = ImmutableMap.copyOf(types);
        }

        private PlanNodeCost lookupStats(PlanNode sourceNode)
        {
            return lookup.getCost(sourceNode, session, types);
        }

        @Override
        protected PlanNodeCost visitPlan(PlanNode node, Void context)
        {
            // TODO: Explicitly visit GroupIdNode and throw an IllegalArgumentException
            // this can only be done once we get rid of the StatelessLookup
            return UNKNOWN_COST;
        }

        @Override
        public PlanNodeCost visitOutput(OutputNode node, Void context)
        {
            return lookupStats(node.getSource());
        }

        @Override
        public PlanNodeCost visitFilter(FilterNode node, Void context)
        {
            PlanNodeCost sourceCost = lookupStats(node.getSource());
            return sourceCost.mapOutputRowCount(value -> value * FILTER_COEFFICIENT);
        }

        @Override
        public PlanNodeCost visitProject(ProjectNode node, Void context)
        {
            return lookupStats(node.getSource());
        }

        @Override
        public PlanNodeCost visitJoin(JoinNode node, Void context)
        {
            PlanNodeCost leftCost = lookupStats(node.getLeft());
            PlanNodeCost rightCost = lookupStats(node.getRight());

            PlanNodeCost.Builder joinCost = PlanNodeCost.builder();
            if (!leftCost.getOutputRowCount().isValueUnknown() && !rightCost.getOutputRowCount().isValueUnknown()) {
                double rowCount = Math.max(leftCost.getOutputRowCount().getValue(), rightCost.getOutputRowCount().getValue()) * JOIN_MATCHING_COEFFICIENT;
                joinCost.setOutputRowCount(new Estimate(rowCount));
            }
            return joinCost.build();
        }

        @Override
        public PlanNodeCost visitExchange(ExchangeNode node, Void context)
        {
            Estimate rowCount = new Estimate(0);
            for (int i = 0; i < node.getSources().size(); i++) {
                PlanNodeCost childCost = lookupStats(node.getSources().get(i));
                if (childCost.getOutputRowCount().isValueUnknown()) {
                    rowCount = Estimate.unknownValue();
                }
                else {
                    rowCount = rowCount.map(value -> value + childCost.getOutputRowCount().getValue());
                }
            }

            return PlanNodeCost.builder()
                    .setOutputRowCount(rowCount)
                    .build();
        }

        @Override
        public PlanNodeCost visitTableScan(TableScanNode node, Void context)
        {
            Constraint<ColumnHandle> constraint = getConstraint(node, BooleanLiteral.TRUE_LITERAL);

            TableStatistics tableStatistics = metadata.getTableStatistics(session, node.getTable(), constraint);
            return PlanNodeCost.builder()
                    .setOutputRowCount(tableStatistics.getRowCount())
                    .build();
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
            return PlanNodeCost.builder()
                    .setOutputRowCount(valuesCount)
                    .build();
        }

        @Override
        public PlanNodeCost visitEnforceSingleRow(EnforceSingleRowNode node, Void context)
        {
            return PlanNodeCost.builder()
                    .setOutputRowCount(new Estimate(1.0))
                    .build();
        }

        @Override
        public PlanNodeCost visitSemiJoin(SemiJoinNode node, Void context)
        {
            PlanNodeCost sourceStats = lookupStats(node.getSource());
            return sourceStats.mapOutputRowCount(rowCount -> rowCount * JOIN_MATCHING_COEFFICIENT);
        }

        @Override
        public PlanNodeCost visitLimit(LimitNode node, Void context)
        {
            PlanNodeCost sourceStats = lookupStats(node.getSource());
            PlanNodeCost.Builder limitCost = PlanNodeCost.builder();
            if (sourceStats.getOutputRowCount().getValue() < node.getCount()) {
                limitCost.setOutputRowCount(sourceStats.getOutputRowCount());
            }
            else {
                limitCost.setOutputRowCount(new Estimate(node.getCount()));
            }
            return limitCost.build();
        }
    }
}
