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
package com.facebook.presto.statistics;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableLayoutHandle;
import com.facebook.presto.metadata.TableLayoutResult;
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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.statistics.PlanNodeStatistics.EMPTY_STATISTICS;

/**
 * Simple implementation of StatisticsCalculator. It make many arbitrary decisions (e.g filtering selectivity, join matching).
 * It serves POC purpose. To be replaced with more advanced implementation.
 */
@ThreadSafe
public class CoefficientBasedStatisticsCalculator
        implements StatisticsCalculator
{
    private static final Double FILTER_COEFFICIENT = 0.5;
    private static final Double JOIN_MATCHING_COEEFICIENT = 2.0;

    // todo some computation for outputSizeInBytes

    private final Metadata metadata;

    @Inject
    public CoefficientBasedStatisticsCalculator(Metadata metadata)
    {
        this.metadata = metadata;
    }

    @Override
    public Map<PlanNode, PlanNodeStatistics> calculateStatisticsForPlan(Session session, PlanNode planNode)
    {
        Visitor visitor = new Visitor(session);
        HashMap<PlanNode, PlanNodeStatistics> statisticsMap = new HashMap<>();
        planNode.accept(visitor, statisticsMap);
        return ImmutableMap.copyOf(statisticsMap);
    }

    private class Visitor
            extends PlanVisitor<Map<PlanNode, PlanNodeStatistics>, Void>
    {
        private final Session session;

        public Visitor(Session session)
        {
            this.session = session;
        }

        @Override
        protected Void visitPlan(PlanNode node, Map<PlanNode, PlanNodeStatistics> context)
        {
            visitChildren(node, context);
            context.put(node, EMPTY_STATISTICS);
            return null;
        }

        private void visitChildren(PlanNode node, Map<PlanNode, PlanNodeStatistics> context)
        {
            for (PlanNode source : node.getSources()) {
                source.accept(this, context);
            }
        }

        @Override
        public Void visitFilter(FilterNode node, Map<PlanNode, PlanNodeStatistics> context)
        {
            visitChildren(node, context);
            PlanNodeStatistics sourceStatistics = context.get(node.getSource());
            PlanNodeStatistics filterStatistics = sourceStatistics
                    .mapOutputRowCount(value -> value * FILTER_COEFFICIENT);
            context.put(node, filterStatistics);
            return null;
        }

        @Override
        public Void visitProject(ProjectNode node, Map<PlanNode, PlanNodeStatistics> context)
        {
            return copySourceStatistics(node, context);
        }

        @Override
        public Void visitJoin(JoinNode node, Map<PlanNode, PlanNodeStatistics> context)
        {
            visitChildren(node, context);
            PlanNodeStatistics leftStatistics = context.get(node.getLeft());
            PlanNodeStatistics rightStatistics = context.get(node.getRight());

            PlanNodeStatistics.Builder joinStatistics = PlanNodeStatistics.builder();
            if (!leftStatistics.getOutputRowCount().isValueUnknown() && !rightStatistics.getOutputRowCount().isValueUnknown()) {
                double joinOutputRowCount = Math.min(leftStatistics.getOutputRowCount().getValue(), rightStatistics.getOutputRowCount().getValue()) * JOIN_MATCHING_COEEFICIENT;
                joinStatistics.setOutputRowCount(new Estimate(joinOutputRowCount));
            }

            context.put(node, joinStatistics.build());
            return null;
        }

        @Override
        public Void visitExchange(ExchangeNode node, Map<PlanNode, PlanNodeStatistics> context)
        {
            visitChildren(node, context);
            Estimate exchangeOutputRowCount = new Estimate(0);
            for (PlanNode child : node.getSources()) {
                PlanNodeStatistics childStatistics = context.get(child);
                if (childStatistics.getOutputRowCount().isValueUnknown()) {
                    exchangeOutputRowCount = Estimate.unknownValue();
                }
                else {
                    exchangeOutputRowCount = exchangeOutputRowCount.map(value -> value + childStatistics.getOutputRowCount().getValue());
                }
            }

            PlanNodeStatistics exchangeStatistics = PlanNodeStatistics.builder()
                    .setOutputRowCount(exchangeOutputRowCount)
                    .build();
            context.put(node, exchangeStatistics);
            return null;
        }

        @Override
        public Void visitTableScan(TableScanNode node, Map<PlanNode, PlanNodeStatistics> context)
        {
            Optional<TableLayoutHandle> layout;
            if (node.getLayout().isPresent()) {
                layout = Optional.of(node.getLayout().get());
            }
            else {
                layout = getDefaultLayout(node);
            }
            PlanNodeStatistics.Builder tableScanStatistics = PlanNodeStatistics.builder();

            if (layout.isPresent()) {
                TableStatistics tableStatistics = metadata.getTableStatistics(session, node.getTable(), layout.get());
                tableScanStatistics.setOutputRowCount(tableStatistics.getRowCount());
            }
            else {
                tableScanStatistics.setOutputRowCount(new Estimate(0));
            }

            context.put(node, tableScanStatistics.build());
            return null;
        }

        private Optional<TableLayoutHandle> getDefaultLayout(TableScanNode node)
        {
            List<TableLayoutResult> layouts = metadata.getLayouts(
                    session, node.getTable(),
                    new Constraint<>(node.getCurrentConstraint(), bindings -> true),
                    Optional.of(ImmutableSet.copyOf(node.getAssignments().values())));
            if (layouts.isEmpty()) {
                return Optional.empty();
            }
            else {
                return Optional.of(layouts.get(0).getLayout().getHandle());
            }
        }

        @Override
        public Void visitValues(ValuesNode node, Map<PlanNode, PlanNodeStatistics> context)
        {
            Estimate valuesCount = new Estimate(node.getRows().size());
            PlanNodeStatistics valuesStatistics = PlanNodeStatistics.builder()
                    .setOutputRowCount(valuesCount)
                    .build();
            context.put(node, valuesStatistics);
            return null;
        }

        @Override
        public Void visitEnforceSingleRow(EnforceSingleRowNode node, Map<PlanNode, PlanNodeStatistics> context)
        {
            visitChildren(node, context);
            PlanNodeStatistics nodeStatistics = PlanNodeStatistics.builder()
                    .setOutputRowCount(new Estimate(1.0))
                    .build();
            context.put(node, nodeStatistics);
            return null;
        }

        @Override
        public Void visitSemiJoin(SemiJoinNode node, Map<PlanNode, PlanNodeStatistics> context)
        {
            visitChildren(node, context);
            PlanNodeStatistics sourceStatitics = context.get(node.getSource());
            PlanNodeStatistics semiJoinStatistics = sourceStatitics.mapOutputRowCount(rowCount -> rowCount * JOIN_MATCHING_COEEFICIENT);
            context.put(node, semiJoinStatistics);
            return null;
        }

        @Override
        public Void visitLimit(LimitNode node, Map<PlanNode, PlanNodeStatistics> context)
        {
            visitChildren(node, context);
            PlanNodeStatistics sourceStatistics = context.get(node.getSource());
            PlanNodeStatistics.Builder limitStatistics = PlanNodeStatistics.builder();
            if (sourceStatistics.getOutputRowCount().getValue() < node.getCount()) {
                limitStatistics.setOutputRowCount(sourceStatistics.getOutputRowCount());
            }
            else {
                limitStatistics.setOutputRowCount(new Estimate(node.getCount()));
            }
            return null;
        }

        private Void copySourceStatistics(PlanNode node, Map<PlanNode, PlanNodeStatistics> context)
        {
            visitChildren(node, context);
            PlanNode source = Iterables.getOnlyElement(node.getSources());
            context.put(node, context.get(source));
            return null;
        }
    }
}
