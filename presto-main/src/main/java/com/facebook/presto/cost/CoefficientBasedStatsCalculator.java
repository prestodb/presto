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
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.facebook.presto.spi.type.Type;
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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
    public Map<PlanNodeId, PlanNodeStatsEstimate> calculateStatsForPlan(Session session, Map<Symbol, Type> types, PlanNode node)
    {
        Visitor visitor = new Visitor(session, types);
        node.accept(visitor, null);
        return ImmutableMap.copyOf(visitor.getStats());
    }

    private class Visitor
            extends PlanVisitor<PlanNodeStatsEstimate, Void>
    {
        private final Session session;
        private final Map<Symbol, Type> types;
        private final Map<PlanNodeId, PlanNodeStatsEstimate> stats;

        public Visitor(Session session, Map<Symbol, Type> types)
        {
            this.session = requireNonNull(session, "session is null");
            this.types = ImmutableMap.copyOf(types);
            this.stats = new HashMap<>();
        }

        public Map<PlanNodeId, PlanNodeStatsEstimate> getStats()
        {
            return ImmutableMap.copyOf(stats);
        }

        @Override
        protected PlanNodeStatsEstimate visitPlan(PlanNode node, Void context)
        {
            visitSources(node);
            stats.put(node.getId(), UNKNOWN_STATS);
            return UNKNOWN_STATS;
        }

        @Override
        public PlanNodeStatsEstimate visitOutput(OutputNode node, Void context)
        {
            return copySourceStats(node);
        }

        @Override
        public PlanNodeStatsEstimate visitFilter(FilterNode node, Void context)
        {
            PlanNodeStatsEstimate sourceStats = visitSource(node);
            PlanNodeStatsEstimate filterStats = sourceStats.mapOutputRowCount(value -> value * (double) FILTER_COEFFICIENT);
            stats.put(node.getId(), filterStats);
            return filterStats;
        }

        @Override
        public PlanNodeStatsEstimate visitProject(ProjectNode node, Void context)
        {
            return copySourceStats(node);
        }

        @Override
        public PlanNodeStatsEstimate visitJoin(JoinNode node, Void context)
        {
            List<PlanNodeStatsEstimate> sourceStats = visitSources(node);
            PlanNodeStatsEstimate leftStats = sourceStats.get(0);
            PlanNodeStatsEstimate rightStats = sourceStats.get(1);

            PlanNodeStatsEstimate.Builder joinStats = PlanNodeStatsEstimate.builder();
            if (!leftStats.getOutputRowCount().isValueUnknown() && !rightStats.getOutputRowCount().isValueUnknown()) {
                double rowCount = Math.max(leftStats.getOutputRowCount().getValue(), rightStats.getOutputRowCount().getValue()) * JOIN_MATCHING_COEFFICIENT;
                joinStats.setOutputRowCount(new Estimate(rowCount));
            }

            stats.put(node.getId(), joinStats.build());
            return joinStats.build();
        }

        @Override
        public PlanNodeStatsEstimate visitExchange(ExchangeNode node, Void context)
        {
            List<PlanNodeStatsEstimate> sourceStats = visitSources(node);
            Estimate rowCount = new Estimate(0);
            for (PlanNodeStatsEstimate sourceStat : sourceStats) {
                if (sourceStat.getOutputRowCount().isValueUnknown()) {
                    rowCount = Estimate.unknownValue();
                }
                else {
                    rowCount = rowCount.map(value -> value + sourceStat.getOutputRowCount().getValue());
                }
            }

            PlanNodeStatsEstimate exchangeStats = PlanNodeStatsEstimate.builder()
                    .setOutputRowCount(rowCount)
                    .build();
            stats.put(node.getId(), exchangeStats);
            return exchangeStats;
        }

        @Override
        public PlanNodeStatsEstimate visitTableScan(TableScanNode node, Void context)
        {
            Constraint<ColumnHandle> constraint = new Constraint<>(node.getCurrentConstraint(), bindings -> true);
            TableStatistics tableStatistics = metadata.getTableStatistics(session, node.getTable(), constraint);
            PlanNodeStatsEstimate tableScanStats = PlanNodeStatsEstimate.builder()
                    .setOutputRowCount(tableStatistics.getRowCount())
                    .build();

            stats.put(node.getId(), tableScanStats);
            return tableScanStats;
        }

        @Override
        public PlanNodeStatsEstimate visitValues(ValuesNode node, Void context)
        {
            Estimate valuesCount = new Estimate(node.getRows().size());
            PlanNodeStatsEstimate valuesStats = PlanNodeStatsEstimate.builder()
                    .setOutputRowCount(valuesCount)
                    .build();
            stats.put(node.getId(), valuesStats);
            return valuesStats;
        }

        @Override
        public PlanNodeStatsEstimate visitEnforceSingleRow(EnforceSingleRowNode node, Void context)
        {
            visitSources(node);
            PlanNodeStatsEstimate nodeStats = PlanNodeStatsEstimate.builder()
                    .setOutputRowCount(new Estimate(1.0))
                    .build();
            stats.put(node.getId(), nodeStats);
            return nodeStats;
        }

        @Override
        public PlanNodeStatsEstimate visitSemiJoin(SemiJoinNode node, Void context)
        {
            visitSources(node);
            PlanNodeStatsEstimate sourceStatitics = stats.get(node.getSource().getId());
            PlanNodeStatsEstimate semiJoinStats = sourceStatitics.mapOutputRowCount(rowCount -> rowCount * JOIN_MATCHING_COEFFICIENT);
            stats.put(node.getId(), semiJoinStats);
            return semiJoinStats;
        }

        @Override
        public PlanNodeStatsEstimate visitLimit(LimitNode node, Void context)
        {
            PlanNodeStatsEstimate sourceStats = visitSource(node);
            PlanNodeStatsEstimate.Builder limitStats = PlanNodeStatsEstimate.builder();
            if (sourceStats.getOutputRowCount().getValue() < node.getCount()) {
                limitStats.setOutputRowCount(sourceStats.getOutputRowCount());
            }
            else {
                limitStats.setOutputRowCount(new Estimate(node.getCount()));
            }
            stats.put(node.getId(), limitStats.build());
            return limitStats.build();
        }

        private PlanNodeStatsEstimate copySourceStats(PlanNode node)
        {
            PlanNodeStatsEstimate sourceStats = visitSource(node);
            stats.put(node.getId(), sourceStats);
            return sourceStats;
        }

        private List<PlanNodeStatsEstimate> visitSources(PlanNode node)
        {
            return node.getSources().stream()
                    .map(source -> source.accept(this, null))
                    .collect(Collectors.toList());
        }

        private PlanNodeStatsEstimate visitSource(PlanNode node)
        {
            return Iterables.getOnlyElement(visitSources(node));
        }
    }
}
