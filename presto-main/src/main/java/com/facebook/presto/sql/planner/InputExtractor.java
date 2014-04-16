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

import com.facebook.presto.execution.Column;
import com.facebook.presto.execution.Input;
import com.facebook.presto.execution.SimpleDomain;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.sql.planner.plan.IndexSourceNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSetMultimap;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class InputExtractor
{
    private final Metadata metadata;

    public InputExtractor(Metadata metadata)
    {
        this.metadata = metadata;
    }

    public List<Input> extract(PlanNode root)
    {
        ImmutableSetMultimap.Builder<TableEntry, Column> builder = ImmutableSetMultimap.builder();

        root.accept(new Visitor(builder), null);

        ImmutableList.Builder<Input> inputBuilder = ImmutableList.builder();
        for (Map.Entry<TableEntry, Collection<Column>> entry : builder.build().asMap().entrySet()) {
            Input input = new Input(entry.getKey().getConnectorId(), entry.getKey().getSchema(), entry.getKey().getTable(), ImmutableList.copyOf(entry.getValue()));
            inputBuilder.add(input);
        }

        return inputBuilder.build();
    }

    private class Visitor
            extends PlanVisitor<Void, Void>
    {
        private final ImmutableSetMultimap.Builder<TableEntry, Column> builder;

        public Visitor(ImmutableSetMultimap.Builder<TableEntry, Column> builder)
        {
            this.builder = builder;
        }

        @Override
        public Void visitTableScan(TableScanNode node, Void context)
        {
            TableHandle tableHandle = node.getTable();
            TableMetadata table = metadata.getTableMetadata(tableHandle);
            SchemaTableName schemaTable = table.getTable();

            TableEntry entry = new TableEntry(table.getConnectorId(), schemaTable.getSchemaName(), schemaTable.getTableName());
            Optional<ColumnHandle> sampleWeightColumn = metadata.getSampleWeightColumnHandle(tableHandle);

            for (ColumnHandle columnHandle : node.getAssignments().values()) {
                if (!columnHandle.equals(sampleWeightColumn.orNull())) {
                    ColumnMetadata columnMetadata = metadata.getColumnMetadata(tableHandle, columnHandle);
                    builder.put(entry, new Column(columnMetadata.getName(), columnMetadata.getType().toString(), Optional.<SimpleDomain>absent()));
                }
            }

            return null;
        }

        @Override
        public Void visitIndexSource(IndexSourceNode node, Void context)
        {
            TableHandle tableHandle = node.getTableHandle();
            TableMetadata table = metadata.getTableMetadata(tableHandle);
            SchemaTableName schemaTable = table.getTable();

            TableEntry entry = new TableEntry(table.getConnectorId(), schemaTable.getSchemaName(), schemaTable.getTableName());

            for (ColumnHandle columnHandle : node.getAssignments().values()) {
                ColumnMetadata columnMetadata = metadata.getColumnMetadata(tableHandle, columnHandle);
                builder.put(entry, new Column(columnMetadata.getName(), columnMetadata.getType().toString(), Optional.<SimpleDomain>absent()));
            }

            return null;
        }

        @Override
        protected Void visitPlan(PlanNode node, Void context)
        {
            for (PlanNode child : node.getSources()) {
                child.accept(this, context);
            }
            return null;
        }
    }

    private static final class TableEntry
    {
        private final String connectorId;
        private final String schema;
        private final String table;

        private TableEntry(String connectorId, String schema, String table)
        {
            this.connectorId = connectorId;
            this.schema = schema;
            this.table = table;
        }

        public String getConnectorId()
        {
            return connectorId;
        }

        public String getSchema()
        {
            return schema;
        }

        public String getTable()
        {
            return table;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(connectorId, schema, table);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            final TableEntry other = (TableEntry) obj;
            return Objects.equals(this.connectorId, other.connectorId) &&
                    Objects.equals(this.schema, other.schema) &&
                    Objects.equals(this.table, other.table);
        }
    }
}
