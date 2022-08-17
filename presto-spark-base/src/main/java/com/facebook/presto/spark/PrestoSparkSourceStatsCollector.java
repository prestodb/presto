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
package com.facebook.presto.spark;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.facebook.presto.sql.planner.plan.InternalPlanVisitor;
import com.google.common.collect.ImmutableList;

import java.util.Iterator;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class PrestoSparkSourceStatsCollector
{
    private final Metadata metadata;
    private final Session session;

    public PrestoSparkSourceStatsCollector(Metadata metadata, Session session)
    {
        this.metadata = metadata;
        this.session = session;
    }

    public double collectSourceStats(PlanNode root)
    {
        SourceTableStatsVisitor sourceTableStatsVisitor = new SourceTableStatsVisitor();
        root.accept(sourceTableStatsVisitor, null);

        List<TableStatistics> sourceStatistics = sourceTableStatsVisitor.getTableStatistics();

        if (sourceTableStatsVisitor.isSourceMissingData()) {
            return -1.0;
        }

        Iterator<TableStatistics> tableStatisticsIterator = sourceStatistics.iterator();
        double totalSourceDataSizeInBytes = 0.0;
        while (tableStatisticsIterator.hasNext()) {
            TableStatistics tableStatistics = tableStatisticsIterator.next();
            totalSourceDataSizeInBytes = totalSourceDataSizeInBytes + tableStatistics.getTotalSize().getValue();
        }

        return totalSourceDataSizeInBytes;
    }

    private class SourceTableStatsVisitor
            extends InternalPlanVisitor<Void, Void>
    {
        private final ImmutableList.Builder<TableStatistics> tableStatisticsBuilder = ImmutableList.builder();
        private boolean isSourceMissingData;

        public List<TableStatistics> getTableStatistics()
        {
            return tableStatisticsBuilder.build();
        }

        public boolean isSourceMissingData()
        {
            return isSourceMissingData;
        }

        @Override
        public Void visitTableScan(TableScanNode node, Void context)
        {
            TableHandle tableHandle = node.getTable();

            List<ColumnHandle> desiredColumns = node.getAssignments().values().stream().collect(toImmutableList());
            Constraint<ColumnHandle> constraint = new Constraint<>(node.getCurrentConstraint());
            TableStatistics statistics = metadata.getTableStatistics(session, tableHandle, desiredColumns, constraint);

            if ((null == statistics) || (statistics == TableStatistics.empty())) {
                isSourceMissingData = true;
            }
            else {
                tableStatisticsBuilder.add(statistics);
            }

            return null;
        }
        @Override
        public Void visitPlan(PlanNode node, Void context)
        {
            for (PlanNode source : node.getSources()) {
                source.accept(this, context);
            }
            return null;
        }
    }
}
