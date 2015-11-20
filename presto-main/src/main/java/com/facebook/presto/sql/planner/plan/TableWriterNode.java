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
package com.facebook.presto.sql.planner.plan;

import com.facebook.presto.metadata.InsertTableHandle;
import com.facebook.presto.metadata.OutputTableHandle;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.sql.planner.Symbol;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@Immutable
public class TableWriterNode
        extends PlanNode
{
    private final PlanNode source;
    private final WriterTarget target;
    private final List<Symbol> outputs;
    private final List<Symbol> columns;
    private final List<String> columnNames;
    private final Optional<Symbol> sampleWeightSymbol;

    @JsonCreator
    public TableWriterNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("target") WriterTarget target,
            @JsonProperty("columns") List<Symbol> columns,
            @JsonProperty("columnNames") List<String> columnNames,
            @JsonProperty("outputs") List<Symbol> outputs,
            @JsonProperty("sampleWeightSymbol") Optional<Symbol> sampleWeightSymbol)
    {
        super(id);

        requireNonNull(columns, "columns is null");
        requireNonNull(columnNames, "columnNames is null");
        checkArgument(columns.size() == columnNames.size(), "columns and columnNames sizes don't match");

        this.source = requireNonNull(source, "source is null");
        this.target = requireNonNull(target, "target is null");
        this.columns = ImmutableList.copyOf(columns);
        this.columnNames = ImmutableList.copyOf(columnNames);
        this.outputs = ImmutableList.copyOf(requireNonNull(outputs, "outputs is null"));
        this.sampleWeightSymbol = requireNonNull(sampleWeightSymbol, "sampleWeightSymbol is null");
    }

    @JsonProperty
    public Optional<Symbol> getSampleWeightSymbol()
    {
        return sampleWeightSymbol;
    }

    @JsonProperty
    public PlanNode getSource()
    {
        return source;
    }

    @JsonProperty
    public WriterTarget getTarget()
    {
        return target;
    }

    @JsonProperty
    public List<Symbol> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public List<String> getColumnNames()
    {
        return columnNames;
    }

    @JsonProperty("outputs")
    @Override
    public List<Symbol> getOutputSymbols()
    {
        return outputs;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context)
    {
        return visitor.visitTableWriter(this, context);
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "@type")
    @JsonSubTypes({
            @JsonSubTypes.Type(value = CreateHandle.class, name = "CreateHandle"),
            @JsonSubTypes.Type(value = InsertHandle.class, name = "InsertHandle"),
            @JsonSubTypes.Type(value = DeleteHandle.class, name = "DeleteHandle"),
    })
    @SuppressWarnings({"EmptyClass", "ClassMayBeInterface"})
    public abstract static class WriterTarget
    {
        @Override
        public abstract String toString();
    }

    // only used during planning -- will not be serialized
    public static class CreateName
            extends WriterTarget
    {
        private final String catalog;
        private final TableMetadata tableMetadata;

        public CreateName(String catalog, TableMetadata tableMetadata)
        {
            this.catalog = requireNonNull(catalog, "catalog is null");
            this.tableMetadata = requireNonNull(tableMetadata, "tableMetadata is null");
        }

        public String getCatalog()
        {
            return catalog;
        }

        public TableMetadata getTableMetadata()
        {
            return tableMetadata;
        }

        @Override
        public String toString()
        {
            return catalog + "." + tableMetadata.getTable();
        }
    }

    public static class CreateHandle
            extends WriterTarget
    {
        private final OutputTableHandle handle;

        @JsonCreator
        public CreateHandle(@JsonProperty("handle") OutputTableHandle handle)
        {
            this.handle = requireNonNull(handle, "handle is null");
        }

        @JsonProperty
        public OutputTableHandle getHandle()
        {
            return handle;
        }

        @Override
        public String toString()
        {
            return handle.toString();
        }
    }

    // only used during planning -- will not be serialized
    public static class InsertReference
            extends WriterTarget
    {
        private final TableHandle handle;

        public InsertReference(TableHandle handle)
        {
            this.handle = requireNonNull(handle, "handle is null");
        }

        public TableHandle getHandle()
        {
            return handle;
        }

        @Override
        public String toString()
        {
            return handle.toString();
        }
    }

    public static class InsertHandle
            extends WriterTarget
    {
        private final InsertTableHandle handle;

        @JsonCreator
        public InsertHandle(@JsonProperty("handle") InsertTableHandle handle)
        {
            this.handle = requireNonNull(handle, "handle is null");
        }

        @JsonProperty
        public InsertTableHandle getHandle()
        {
            return handle;
        }

        @Override
        public String toString()
        {
            return handle.toString();
        }
    }

    public static class DeleteHandle
            extends WriterTarget
    {
        private final TableHandle handle;

        @JsonCreator
        public DeleteHandle(@JsonProperty("handle") TableHandle handle)
        {
            this.handle = requireNonNull(handle, "handle is null");
        }

        @JsonProperty
        public TableHandle getHandle()
        {
            return handle;
        }

        @Override
        public String toString()
        {
            return handle.toString();
        }
    }
}
