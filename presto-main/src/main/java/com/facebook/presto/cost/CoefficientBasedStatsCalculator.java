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

import static com.facebook.presto.cost.PlanNodeStatsEstimate.UNKNOWN_STATS;

/**
 * Simple implementation of StatsCalculator. It make many arbitrary decisions (e.g filtering selectivity, join matching).
 * It serves POC purpose. To be replaced with more advanced implementation.
 */
@ThreadSafe
public class CoefficientBasedStatsCalculator
        implements StatsCalculator
{
    private static final Double FILTER_COEFFICIENT = 0.5;
    private static final Double JOIN_MATCHING_COEFFICIENT = 2.0;

    // todo some computation for outputSizeInBytes

    private final Metadata metadata;

    @Inject
    public CoefficientBasedStatsCalculator(Metadata metadata)
    {
        this.metadata = metadata;
    }

    @Override
    public PlanNodeStatsEstimate calculateStats(
            PlanNode planNode,
            Lookup lookup,
            Session session,
            Map<Symbol, Type> types)
    {
        Visitor visitor = new Visitor(lookup, session, types);
        return planNode.accept(visitor, null);
    }

    private class Visitor
            extends PlanVisitor<PlanNodeStatsEstimate, Void>
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

        private PlanNodeStatsEstimate lookupStats(PlanNode sourceNode)
        {
            return lookup.getStats(sourceNode, session, types);
        }

        @Override
        protected PlanNodeStatsEstimate visitPlan(PlanNode node, Void context)
        {
            // TODO: Explicitly visit GroupIdNode and throw an IllegalArgumentException
            // this can only be done once we get rid of the StatelessLookup
            return UNKNOWN_STATS;
        }

        @Override
        public PlanNodeStatsEstimate visitOutput(OutputNode node, Void context)
        {
            return lookupStats(node.getSource());
        }

        @Override
        public PlanNodeStatsEstimate visitFilter(FilterNode node, Void context)
        {
            PlanNodeStatsEstimate sourceStats = lookupStats(node.getSource());
            return sourceStats.mapOutputRowCount(value -> value * FILTER_COEFFICIENT);
        }

        @Override
        public PlanNodeStatsEstimate visitProject(ProjectNode node, Void context)
        {
            return lookupStats(node.getSource());
        }

        @Override
        public PlanNodeStatsEstimate visitJoin(JoinNode node, Void context)
        {
            PlanNodeStatsEstimate leftStats = lookupStats(node.getLeft());
            PlanNodeStatsEstimate rightStats = lookupStats(node.getRight());

            PlanNodeStatsEstimate.Builder joinStats = PlanNodeStatsEstimate.builder();
            double rowCount = Math.max(leftStats.getOutputRowCount(), rightStats.getOutputRowCount()) * JOIN_MATCHING_COEFFICIENT;
            joinStats.setOutputRowCount(rowCount);
            return joinStats.build();
        }

        @Override
        public PlanNodeStatsEstimate visitExchange(ExchangeNode node, Void context)
        {
            double rowCount = 0;
            for (int i = 0; i < node.getSources().size(); i++) {
                PlanNodeStatsEstimate childStats = lookupStats(node.getSources().get(i));
                rowCount = rowCount + childStats.getOutputRowCount();
            }

            return PlanNodeStatsEstimate.builder()
                    .setOutputRowCount(rowCount)
                    .build();
        }

        @Override
        public PlanNodeStatsEstimate visitTableScan(TableScanNode node, Void context)
        {
            Constraint<ColumnHandle> constraint = getConstraint(node, BooleanLiteral.TRUE_LITERAL);

            TableStatistics tableStatistics = metadata.getTableStatistics(session, node.getTable(), constraint);
            return PlanNodeStatsEstimate.builder()
                    .setOutputRowCount(tableStatistics.getRowCount().getValue())
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
        public PlanNodeStatsEstimate visitValues(ValuesNode node, Void context)
        {
            double valuesCount = node.getRows().size();
            return PlanNodeStatsEstimate.builder()
                    .setOutputRowCount(valuesCount)
                    .build();
        }

        @Override
        public PlanNodeStatsEstimate visitEnforceSingleRow(EnforceSingleRowNode node, Void context)
        {
            return PlanNodeStatsEstimate.builder()
                    .setOutputRowCount(1.0)
                    .build();
        }

        @Override
        public PlanNodeStatsEstimate visitSemiJoin(SemiJoinNode node, Void context)
        {
            PlanNodeStatsEstimate sourceStats = lookupStats(node.getSource());
            return sourceStats.mapOutputRowCount(rowCount -> rowCount * JOIN_MATCHING_COEFFICIENT);
        }

        @Override
        public PlanNodeStatsEstimate visitLimit(LimitNode node, Void context)
        {
            PlanNodeStatsEstimate sourceStats = lookupStats(node.getSource());
            PlanNodeStatsEstimate.Builder limitStats = PlanNodeStatsEstimate.builder();
            if (sourceStats.getOutputRowCount() < node.getCount()) {
                limitStats.setOutputRowCount(sourceStats.getOutputRowCount());
            }
            else {
                limitStats.setOutputRowCount(node.getCount());
            }
            return limitStats.build();
        }
    }
}
