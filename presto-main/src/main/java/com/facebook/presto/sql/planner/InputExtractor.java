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
package com.facebook.presto.sql.planner;

import com.facebook.presto.Session;
import com.facebook.presto.execution.Column;
import com.facebook.presto.execution.Input;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.facebook.presto.sql.planner.plan.IndexSourceNode;
import com.facebook.presto.sql.planner.plan.InternalPlanVisitor;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.SpatialJoinNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class InputExtractor
{
    private final Metadata metadata;
    private final Session session;

    public InputExtractor(Metadata metadata, Session session)
    {
        this.metadata = metadata;
        this.session = session;
    }

    public List<Input> extractInputs(PlanNode root)
    {
        Visitor visitor = new Visitor();
        root.accept(visitor, new Context());

        return ImmutableList.copyOf(visitor.getInputs());
    }

    private static Column createColumn(ColumnMetadata columnMetadata)
    {
        return new Column(columnMetadata.getName(), columnMetadata.getType().toString());
    }

    private Input createInput(TableMetadata table, TableHandle tableHandle, Set<Column> columns, Optional<TableStatistics> statistics)
    {
        SchemaTableName schemaTable = table.getTable();
        Optional<Object> inputMetadata = metadata.getInfo(session, tableHandle);
        return new Input(table.getConnectorId(), schemaTable.getSchemaName(), schemaTable.getTableName(), inputMetadata, ImmutableList.copyOf(columns), statistics);
    }

    private class Visitor
            extends InternalPlanVisitor<Void, Context>
    {
        private final ImmutableSet.Builder<Input> inputs = ImmutableSet.builder();

        public Set<Input> getInputs()
        {
            return inputs.build();
        }

        @Override
        public Void visitJoin(JoinNode node, Context context)
        {
            context.setExtractStatistics(true);
            visitPlan(node, context);
            return null;
        }

        @Override
        public Void visitSemiJoin(SemiJoinNode node, Context context)
        {
            context.setExtractStatistics(true);
            visitPlan(node, context);
            return null;
        }

        @Override
        public Void visitSpatialJoin(SpatialJoinNode node, Context context)
        {
            context.setExtractStatistics(true);
            visitPlan(node, context);
            return null;
        }

        @Override
        public Void visitTableScan(TableScanNode node, Context context)
        {
            TableHandle tableHandle = node.getTable();

            Set<Column> columns = new HashSet<>();
            for (ColumnHandle columnHandle : node.getAssignments().values()) {
                columns.add(createColumn(metadata.getColumnMetadata(session, tableHandle, columnHandle)));
            }

            List<ColumnHandle> desiredColumns = node.getAssignments().values().stream().collect(toImmutableList());

            Optional<TableStatistics> statistics = Optional.empty();

            if (context.isExtractStatistics()) {
                Constraint<ColumnHandle> constraint = new Constraint<>(node.getCurrentConstraint());
                statistics = Optional.of(metadata.getTableStatistics(session, tableHandle, desiredColumns, constraint));
            }

            inputs.add(createInput(metadata.getTableMetadata(session, tableHandle), tableHandle, columns, statistics));

            return null;
        }

        @Override
        public Void visitIndexSource(IndexSourceNode node, Context context)
        {
            TableHandle tableHandle = node.getTableHandle();

            Set<Column> columns = new HashSet<>();
            for (ColumnHandle columnHandle : node.getAssignments().values()) {
                columns.add(createColumn(metadata.getColumnMetadata(session, tableHandle, columnHandle)));
            }

            List<ColumnHandle> desiredColumns = node.getAssignments().values().stream().collect(toImmutableList());

            Optional<TableStatistics> statistics = Optional.empty();

            if (context.isExtractStatistics()) {
                Constraint<ColumnHandle> constraint = new Constraint<>(node.getCurrentConstraint());
                statistics = Optional.of(metadata.getTableStatistics(session, tableHandle, desiredColumns, constraint));
            }

            inputs.add(createInput(metadata.getTableMetadata(session, tableHandle), tableHandle, columns, statistics));

            return null;
        }

        @Override
        public Void visitPlan(PlanNode node, Context context)
        {
            for (PlanNode child : node.getSources()) {
                child.accept(this, context);
            }
            return null;
        }
    }

    private static class Context
    {
        private boolean extractStatistics;

        public Context() {}

        public boolean isExtractStatistics()
        {
            return extractStatistics;
        }

        public void setExtractStatistics(boolean extractStatistics)
        {
            this.extractStatistics = extractStatistics;
        }
    }
}
