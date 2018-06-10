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
import com.facebook.presto.spi.statistics.TableStatistics;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.iterative.GroupReference;
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

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import static com.facebook.presto.cost.PlanNodeStatsEstimate.UNKNOWN_STATS;
import static java.util.Objects.requireNonNull;

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
    public PlanNodeStatsEstimate calculateStats(PlanNode node, StatsProvider sourceStats, Lookup lookup, Session session, TypeProvider types)
    {
        Visitor visitor = new Visitor(sourceStats, session);
        return node.accept(visitor, null);
    }

    private class Visitor
            extends PlanVisitor<PlanNodeStatsEstimate, Void>
    {
        private final StatsProvider sourceStats;
        private final Session session;

        public Visitor(StatsProvider sourceStats, Session session)
        {
            this.sourceStats = requireNonNull(sourceStats, "sourceStats is null");
            this.session = requireNonNull(session, "session is null");
        }

        private PlanNodeStatsEstimate getStats(PlanNode sourceNode)
        {
            return sourceStats.getStats(sourceNode);
        }

        @Override
        protected PlanNodeStatsEstimate visitPlan(PlanNode node, Void context)
        {
            return UNKNOWN_STATS;
        }

        @Override
        public PlanNodeStatsEstimate visitGroupReference(GroupReference node, Void context)
        {
            // StatsCalculator should not be directly called on GroupReference
            throw new UnsupportedOperationException();
        }

        @Override
        public PlanNodeStatsEstimate visitOutput(OutputNode node, Void context)
        {
            return getStats(node.getSource());
        }

        @Override
        public PlanNodeStatsEstimate visitFilter(FilterNode node, Void context)
        {
            PlanNodeStatsEstimate sourceStats = getStats(node.getSource());
            return sourceStats.mapOutputRowCount(value -> value * FILTER_COEFFICIENT);
        }

        @Override
        public PlanNodeStatsEstimate visitProject(ProjectNode node, Void context)
        {
            return getStats(node.getSource());
        }

        @Override
        public PlanNodeStatsEstimate visitJoin(JoinNode node, Void context)
        {
            PlanNodeStatsEstimate leftStats = getStats(node.getLeft());
            PlanNodeStatsEstimate rightStats = getStats(node.getRight());

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
                PlanNodeStatsEstimate sourceStat = getStats(node.getSources().get(i));
                rowCount = rowCount + sourceStat.getOutputRowCount();
            }

            return PlanNodeStatsEstimate.builder()
                    .setOutputRowCount(rowCount)
                    .build();
        }

        @Override
        public PlanNodeStatsEstimate visitTableScan(TableScanNode node, Void context)
        {
            Constraint<ColumnHandle> constraint = new Constraint<>(node.getCurrentConstraint(), bindings -> true);
            TableStatistics tableStatistics = metadata.getTableStatistics(session, node.getTable(), constraint);
            return PlanNodeStatsEstimate.builder()
                    .setOutputRowCount(tableStatistics.getRowCount().getValue())
                    .build();
        }

        @Override
        public PlanNodeStatsEstimate visitValues(ValuesNode node, Void context)
        {
            int valuesCount = node.getRows().size();
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
            PlanNodeStatsEstimate sourceStats = getStats(node.getSource());
            return sourceStats.mapOutputRowCount(rowCount -> rowCount * JOIN_MATCHING_COEFFICIENT);
        }

        @Override
        public PlanNodeStatsEstimate visitLimit(LimitNode node, Void context)
        {
            PlanNodeStatsEstimate sourceStats = getStats(node.getSource());
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
