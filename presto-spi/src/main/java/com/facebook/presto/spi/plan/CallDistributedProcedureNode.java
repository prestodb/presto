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
package com.facebook.presto.spi.plan;

import com.facebook.presto.spi.SourceLocation;
import com.facebook.presto.spi.plan.TableWriterNode.CallDistributedProcedureTarget;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.common.Utils.checkArgument;
import static java.util.Objects.requireNonNull;

public class CallDistributedProcedureNode
        extends PlanNode
{
    private final PlanNode source;
    private final Optional<CallDistributedProcedureTarget> target;
    private final VariableReferenceExpression rowCountVariable;
    private final VariableReferenceExpression fragmentVariable;
    private final VariableReferenceExpression tableCommitContextVariable;
    private final List<VariableReferenceExpression> columns;
    private final List<String> columnNames;
    private final Set<VariableReferenceExpression> notNullColumnVariables;
    private final Optional<PartitioningScheme> partitioningScheme;
    private final List<VariableReferenceExpression> outputs;

    @JsonCreator
    public CallDistributedProcedureNode(
            Optional<SourceLocation> sourceLocation,
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("target") Optional<CallDistributedProcedureTarget> target,
            @JsonProperty("rowCountVariable") VariableReferenceExpression rowCountVariable,
            @JsonProperty("fragmentVariable") VariableReferenceExpression fragmentVariable,
            @JsonProperty("tableCommitContextVariable") VariableReferenceExpression tableCommitContextVariable,
            @JsonProperty("columns") List<VariableReferenceExpression> columns,
            @JsonProperty("columnNames") List<String> columnNames,
            @JsonProperty("notNullColumnVariables") Set<VariableReferenceExpression> notNullColumnVariables,
            @JsonProperty("partitioningScheme") Optional<PartitioningScheme> partitioningScheme)
    {
        this(sourceLocation, id, Optional.empty(), source, target, rowCountVariable, fragmentVariable,
                tableCommitContextVariable, columns, columnNames, notNullColumnVariables, partitioningScheme);
    }

    public CallDistributedProcedureNode(
            Optional<SourceLocation> sourceLocation,
            PlanNodeId id,
            Optional<PlanNode> statsEquivalentPlanNode,
            PlanNode source,
            Optional<CallDistributedProcedureTarget> target,
            VariableReferenceExpression rowCountVariable,
            VariableReferenceExpression fragmentVariable,
            VariableReferenceExpression tableCommitContextVariable,
            List<VariableReferenceExpression> columns,
            List<String> columnNames,
            Set<VariableReferenceExpression> notNullColumnVariables,
            Optional<PartitioningScheme> partitioningScheme)
    {
        super(sourceLocation, id, statsEquivalentPlanNode);

        requireNonNull(columns, "columns is null");
        requireNonNull(columnNames, "columnNames is null");
        checkArgument(columns.size() == columnNames.size(), "columns and columnNames sizes don't match");

        this.source = requireNonNull(source, "source is null");
        this.target = requireNonNull(target, "target is null");
        this.rowCountVariable = requireNonNull(rowCountVariable, "rowCountVariable is null");
        this.fragmentVariable = requireNonNull(fragmentVariable, "fragmentVariable is null");
        this.tableCommitContextVariable = requireNonNull(tableCommitContextVariable, "tableCommitContextVariable is null");
        this.columns = Collections.unmodifiableList(columns);
        this.columnNames = Collections.unmodifiableList(columnNames);
        this.notNullColumnVariables = Collections.unmodifiableSet(requireNonNull(notNullColumnVariables, "notNullColumns is null"));
        this.partitioningScheme = requireNonNull(partitioningScheme, "partitioningScheme is null");
        this.outputs = getOutputs(rowCountVariable, fragmentVariable, tableCommitContextVariable);
    }

    @JsonProperty
    public PlanNode getSource()
    {
        return source;
    }

    @JsonIgnore
    public Optional<CallDistributedProcedureTarget> getTarget()
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
    public Optional<PartitioningScheme> getPartitioningScheme()
    {
        return partitioningScheme;
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
    public List<VariableReferenceExpression> getOutputs()
    {
        return outputs;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return Collections.singletonList(source);
    }

    @Override
    public List<VariableReferenceExpression> getOutputVariables()
    {
        return outputs;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitCallDistributedProcedure(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        checkArgument(newChildren.size() == 1, "expected exactly one child, got: %s", newChildren.size());
        return new CallDistributedProcedureNode(
                getSourceLocation(),
                getId(),
                getStatsEquivalentPlanNode(),
                newChildren.get(0),
                target,
                rowCountVariable,
                fragmentVariable,
                tableCommitContextVariable,
                columns,
                columnNames,
                notNullColumnVariables,
                partitioningScheme);
    }

    @Override
    public PlanNode assignStatsEquivalentPlanNode(Optional<PlanNode> statsEquivalentPlanNode)
    {
        return new CallDistributedProcedureNode(
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
                partitioningScheme);
    }

    private static List<VariableReferenceExpression> getOutputs(
            VariableReferenceExpression rowCountVariable,
            VariableReferenceExpression fragmentVariable,
            VariableReferenceExpression tableCommitContextVariable)
    {
        List<VariableReferenceExpression> outputsList = new ArrayList<>();
        outputsList.add(rowCountVariable);
        outputsList.add(fragmentVariable);
        outputsList.add(tableCommitContextVariable);
        return Collections.unmodifiableList(outputsList);
    }
}
