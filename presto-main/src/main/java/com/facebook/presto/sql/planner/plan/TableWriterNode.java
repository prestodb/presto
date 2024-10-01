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

import com.facebook.presto.metadata.NewTableLayout;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SourceLocation;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.plan.PartitioningScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@Immutable
public class TableWriterNode
        extends InternalPlanNode
{
    private final PlanNode source;
    private final Optional<WriterTarget> target;
    private final VariableReferenceExpression rowCountVariable;
    private final VariableReferenceExpression fragmentVariable;
    private final VariableReferenceExpression tableCommitContextVariable;
    private final List<VariableReferenceExpression> columns;
    private final List<String> columnNames;
    private final Set<VariableReferenceExpression> notNullColumnVariables;
    private final Optional<PartitioningScheme> tablePartitioningScheme;
    private final Optional<PartitioningScheme> preferredShufflePartitioningScheme;
    private final Optional<StatisticAggregations> statisticsAggregation;
    private final List<VariableReferenceExpression> outputs;
    private final Optional<Integer> taskCountIfScaledWriter;
    private final Optional<Boolean> isTemporaryTableWriter;

    @JsonCreator
    public TableWriterNode(
            Optional<SourceLocation> sourceLocation,
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("target") Optional<WriterTarget> target,
            @JsonProperty("rowCountVariable") VariableReferenceExpression rowCountVariable,
            @JsonProperty("fragmentVariable") VariableReferenceExpression fragmentVariable,
            @JsonProperty("tableCommitContextVariable") VariableReferenceExpression tableCommitContextVariable,
            @JsonProperty("columns") List<VariableReferenceExpression> columns,
            @JsonProperty("columnNames") List<String> columnNames,
            @JsonProperty("notNullColumnVariables") Set<VariableReferenceExpression> notNullColumnVariables,
            @JsonProperty("partitioningScheme") Optional<PartitioningScheme> tablePartitioningScheme,
            @JsonProperty("preferredShufflePartitioningScheme") Optional<PartitioningScheme> preferredShufflePartitioningScheme,
            @JsonProperty("statisticsAggregation") Optional<StatisticAggregations> statisticsAggregation,
            @JsonProperty("taskCountIfScaledWriter") Optional<Integer> taskCountIfScaledWriter,
            @JsonProperty("isTemporaryTableWriter") Optional<Boolean> isTemporaryTableWriter)
    {
        this(sourceLocation, id, Optional.empty(), source, target, rowCountVariable, fragmentVariable, tableCommitContextVariable, columns, columnNames, notNullColumnVariables, tablePartitioningScheme, preferredShufflePartitioningScheme, statisticsAggregation, taskCountIfScaledWriter, isTemporaryTableWriter);
    }

    public TableWriterNode(
            Optional<SourceLocation> sourceLocation,
            PlanNodeId id,
            Optional<PlanNode> statsEquivalentPlanNode,
            PlanNode source,
            Optional<WriterTarget> target,
            VariableReferenceExpression rowCountVariable,
            VariableReferenceExpression fragmentVariable,
            VariableReferenceExpression tableCommitContextVariable,
            List<VariableReferenceExpression> columns,
            List<String> columnNames,
            Set<VariableReferenceExpression> notNullColumnVariables,
            Optional<PartitioningScheme> tablePartitioningScheme,
            Optional<PartitioningScheme> preferredShufflePartitioningScheme,
            Optional<StatisticAggregations> statisticsAggregation,
            Optional<Integer> taskCountIfScaledWriter,
            Optional<Boolean> isTemporaryTableWriter)
    {
        super(sourceLocation, id, statsEquivalentPlanNode);

        requireNonNull(columns, "columns is null");
        requireNonNull(columnNames, "columnNames is null");
        checkArgument(columns.size() == columnNames.size(), "columns and columnNames sizes don't match");
        checkArgument(
                !(tablePartitioningScheme.isPresent() && preferredShufflePartitioningScheme.isPresent()),
                "tablePartitioningScheme and preferredShufflePartitioningScheme cannot both exist");

        this.source = requireNonNull(source, "source is null");
        this.target = requireNonNull(target, "target is null");
        this.rowCountVariable = requireNonNull(rowCountVariable, "rowCountVariable is null");
        this.fragmentVariable = requireNonNull(fragmentVariable, "fragmentVariable is null");
        this.tableCommitContextVariable = requireNonNull(tableCommitContextVariable, "tableCommitContextVariable is null");
        this.columns = ImmutableList.copyOf(columns);
        this.columnNames = ImmutableList.copyOf(columnNames);
        this.notNullColumnVariables = ImmutableSet.copyOf(requireNonNull(notNullColumnVariables, "notNullColumns is null"));
        this.tablePartitioningScheme = requireNonNull(tablePartitioningScheme, "partitioningScheme is null");
        this.preferredShufflePartitioningScheme = requireNonNull(preferredShufflePartitioningScheme, "preferredShufflePartitioningScheme is null");
        this.statisticsAggregation = requireNonNull(statisticsAggregation, "statisticsAggregation is null");

        ImmutableList.Builder<VariableReferenceExpression> outputs = ImmutableList.<VariableReferenceExpression>builder()
                .add(rowCountVariable)
                .add(fragmentVariable)
                .add(tableCommitContextVariable);
        statisticsAggregation.ifPresent(aggregation -> {
            outputs.addAll(aggregation.getGroupingVariables());
            outputs.addAll(aggregation.getAggregations().keySet());
        });
        this.outputs = outputs.build();
        this.taskCountIfScaledWriter = requireNonNull(taskCountIfScaledWriter, "taskCountIfScaledWriter is null");
        this.isTemporaryTableWriter = requireNonNull(isTemporaryTableWriter, "isTemporaryTableWriter is null");
    }

    @JsonProperty
    public PlanNode getSource()
    {
        return source;
    }

    @JsonIgnore
    public Optional<WriterTarget> getTarget()
    {
        return target;
    }

    @JsonProperty
    public VariableReferenceExpression getRowCountVariable()
    {
        return rowCountVariable;
    }

    @JsonProperty
    public VariableReferenceExpression getFragmentVariable()
    {
        return fragmentVariable;
    }

    @JsonProperty
    public VariableReferenceExpression getTableCommitContextVariable()
    {
        return tableCommitContextVariable;
    }

    @JsonProperty
    public List<VariableReferenceExpression> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public List<String> getColumnNames()
    {
        return columnNames;
    }

    @JsonProperty
    public Set<VariableReferenceExpression> getNotNullColumnVariables()
    {
        return notNullColumnVariables;
    }

    @JsonProperty
    public Optional<PartitioningScheme> getTablePartitioningScheme()
    {
        return tablePartitioningScheme;
    }

    @JsonProperty
    public Optional<PartitioningScheme> getPreferredShufflePartitioningScheme()
    {
        return preferredShufflePartitioningScheme;
    }

    @JsonProperty
    public Optional<StatisticAggregations> getStatisticsAggregation()
    {
        return statisticsAggregation;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @Override
    public List<VariableReferenceExpression> getOutputVariables()
    {
        return outputs;
    }

    @JsonProperty
    public Optional<Integer> getTaskCountIfScaledWriter()
    {
        return taskCountIfScaledWriter;
    }

    @JsonProperty
    public Optional<Boolean> getIsTemporaryTableWriter()
    {
        return isTemporaryTableWriter;
    }

    @Override
    public <R, C> R accept(InternalPlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitTableWriter(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new TableWriterNode(
                getSourceLocation(),
                getId(),
                getStatsEquivalentPlanNode(),
                Iterables.getOnlyElement(newChildren),
                target,
                rowCountVariable,
                fragmentVariable,
                tableCommitContextVariable,
                columns,
                columnNames,
                notNullColumnVariables,
                tablePartitioningScheme,
                preferredShufflePartitioningScheme,
                statisticsAggregation,
                taskCountIfScaledWriter, isTemporaryTableWriter);
    }

    @Override
    public PlanNode assignStatsEquivalentPlanNode(Optional<PlanNode> statsEquivalentPlanNode)
    {
        return new TableWriterNode(
                getSourceLocation(),
                getId(),
                statsEquivalentPlanNode,
                source,
                target,
                rowCountVariable,
                fragmentVariable,
                tableCommitContextVariable,
                columns,
                columnNames,
                notNullColumnVariables,
                tablePartitioningScheme,
                preferredShufflePartitioningScheme,
                statisticsAggregation,
                taskCountIfScaledWriter, isTemporaryTableWriter);
    }

    // only used during planning -- will not be serialized
    @SuppressWarnings({"EmptyClass", "ClassMayBeInterface"})
    public abstract static class WriterTarget
    {
        public abstract ConnectorId getConnectorId();

        public abstract SchemaTableName getSchemaTableName();

        @Override
        public abstract String toString();
    }

    public static class CreateName
            extends WriterTarget
    {
        private final ConnectorId connectorId;
        private final ConnectorTableMetadata tableMetadata;
        private final Optional<NewTableLayout> layout;

        public CreateName(ConnectorId connectorId, ConnectorTableMetadata tableMetadata, Optional<NewTableLayout> layout)
        {
            this.connectorId = requireNonNull(connectorId, "connectorId is null");
            this.tableMetadata = requireNonNull(tableMetadata, "tableMetadata is null");
            this.layout = requireNonNull(layout, "layout is null");
        }

        @Override
        public ConnectorId getConnectorId()
        {
            return connectorId;
        }

        public ConnectorTableMetadata getTableMetadata()
        {
            return tableMetadata;
        }

        public Optional<NewTableLayout> getLayout()
        {
            return layout;
        }

        @Override
        public SchemaTableName getSchemaTableName()

        {
            return tableMetadata.getTable();
        }

        @Override
        public String toString()
        {
            return connectorId + "." + tableMetadata.getTable();
        }
    }

    public static class InsertReference
            extends WriterTarget
    {
        private final TableHandle handle;
        private final SchemaTableName schemaTableName;

        public InsertReference(TableHandle handle, SchemaTableName schemaTableName)
        {
            this.handle = requireNonNull(handle, "handle is null");
            this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        }

        public TableHandle getHandle()
        {
            return handle;
        }

        @Override
        public ConnectorId getConnectorId()
        {
            return handle.getConnectorId();
        }

        @Override
        public SchemaTableName getSchemaTableName()
        {
            return schemaTableName;
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
        private final SchemaTableName schemaTableName;

        public DeleteHandle(
                TableHandle handle,
                SchemaTableName schemaTableName)
        {
            this.handle = requireNonNull(handle, "handle is null");
            this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        }

        public TableHandle getHandle()
        {
            return handle;
        }

        @Override
        public ConnectorId getConnectorId()
        {
            return handle.getConnectorId();
        }

        @Override
        public SchemaTableName getSchemaTableName()
        {
            return schemaTableName;
        }

        public String toString()
        {
            return handle.toString();
        }
    }

    public static class RefreshMaterializedViewReference
            extends WriterTarget
    {
        private final TableHandle handle;
        private final SchemaTableName schemaTableName;

        public RefreshMaterializedViewReference(TableHandle handle, SchemaTableName schemaTableName)
        {
            this.handle = requireNonNull(handle, "handle is null");
            this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        }

        public TableHandle getHandle()
        {
            return handle;
        }

        @Override
        public ConnectorId getConnectorId()
        {
            return handle.getConnectorId();
        }

        @Override
        public SchemaTableName getSchemaTableName()
        {
            return schemaTableName;
        }

        @Override
        public String toString()
        {
            return handle.toString();
        }
    }

    public static class UpdateTarget
            extends WriterTarget
    {
        private final TableHandle handle;
        private final SchemaTableName schemaTableName;
        private final List<String> updatedColumns;
        private final List<ColumnHandle> updatedColumnHandles;

        public UpdateTarget(
                TableHandle handle,
                SchemaTableName schemaTableName,
                List<String> updatedColumns,
                List<ColumnHandle> updatedColumnHandles)
        {
            this.handle = requireNonNull(handle, "handle is null");
            this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
            checkArgument(updatedColumns.size() == updatedColumnHandles.size(), "updatedColumns size %s must equal updatedColumnHandles size %s", updatedColumns.size(), updatedColumnHandles.size());
            this.updatedColumns = requireNonNull(updatedColumns, "updatedColumns is null");
            this.updatedColumnHandles = requireNonNull(updatedColumnHandles, "updatedColumnHandles is null");
        }

        public TableHandle getHandle()
        {
            return handle;
        }

        public ConnectorId getConnectorId()
        {
            return handle.getConnectorId();
        }

        public SchemaTableName getSchemaTableName()
        {
            return schemaTableName;
        }

        public List<String> getUpdatedColumns()
        {
            return updatedColumns;
        }

        public List<ColumnHandle> getUpdatedColumnHandles()
        {
            return updatedColumnHandles;
        }

        @Override
        public String toString()
        {
            return handle.toString();
        }
    }
}
