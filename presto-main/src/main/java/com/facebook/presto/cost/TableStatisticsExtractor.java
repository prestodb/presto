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

import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.facebook.presto.sql.planner.plan.InternalPlanVisitor;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.function.Function;

public class TableStatisticsExtractor
{
    private TableStatisticsExtractor() {}

    private static class Visitor
            extends InternalPlanVisitor<Void, Void>
    {
        private final ImmutableMap.Builder<PlanNodeId, TableStatistics> tableStatistics;
        private final Function<TableScanNode, TableStatistics> tableStatisticsProvider;

        private Visitor(Function<TableScanNode, TableStatistics> tableStatisticsProvider)
        {
            this.tableStatisticsProvider = tableStatisticsProvider;
            this.tableStatistics = ImmutableMap.builder();
        }

        @Override
        public Void visitPlan(PlanNode node, Void context)
        {
            for (PlanNode child : node.getSources()) {
                child.accept(this, context);
            }
            return null;
        }

        @Override
        public Void visitTableScan(TableScanNode node, Void context)
        {
            collectTableStatsForNodeId(node, node.getId()); // stats associated with self id
            return null;
        }

        @Override
        public Void visitProject(ProjectNode node, Void context)
        {
            PlanNode source = node.getSource();
            if (source instanceof FilterNode) {
                optionallyCollectStatsForNodeSource(node.getId(), ((FilterNode) source).getSource()); // potentially ScanFilterProject node
            }
            else {
                optionallyCollectStatsForNodeSource(node.getId(), source); // potentially ScanProject node
            }
            return super.visitProject(node, context);
        }

        @Override
        public Void visitFilter(FilterNode node, Void context)
        {
            optionallyCollectStatsForNodeSource(node.getId(), node.getSource());
            return super.visitFilter(node, context);
        }

        @Override
        public Void visitValues(ValuesNode node, Void context)
        {
            final int rowCount = node.getRows().size();
            tableStatistics.put(node.getId(), TableStatistics.builder().setRowCount(Estimate.of(rowCount))
                    .setTotalSize(Estimate.of(rowCount)).build()); // TODO temporary estimate, need to calculate actual size
            return null;
        }

        private void optionallyCollectStatsForNodeSource(PlanNodeId parentId, PlanNode node)
        {
            if (node instanceof TableScanNode) {
                collectTableStatsForNodeId((TableScanNode) node, parentId); // stats associated with parent node id
            }
        }

        private void collectTableStatsForNodeId(TableScanNode tableScanNode, PlanNodeId relatedNodeId)
        {
            final TableStatistics statistics = tableStatisticsProvider.apply(tableScanNode);
            tableStatistics.put(relatedNodeId, statistics);
        }

        public Map<PlanNodeId, TableStatistics> getTableStatistics()
        {
            return tableStatistics.build();
        }
    }

    /**
     * @return nodeId to TableStatistics mapping for every TableScanNode in node tree
     */
    public static Map<PlanNodeId, TableStatistics> extractTableStatistics(PlanNode node,
            Function<TableScanNode, TableStatistics> tableStatisticsProvider)
    {
        Visitor visitor = new Visitor(tableStatisticsProvider);
        node.accept(visitor, null);
        return visitor.getTableStatistics();
    }
}
