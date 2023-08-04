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
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.Immutable;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

@Immutable
public class OutputNode
        extends PlanNode
{
    private final PlanNode source;
    private final List<String> columnNames;
    private final List<VariableReferenceExpression> outputVariables; // column name = variable.name

    @JsonCreator
    public OutputNode(
            Optional<SourceLocation> sourceLocation,
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("columnNames") List<String> columnNames,
            @JsonProperty("outputVariables") List<VariableReferenceExpression> outputVariables)
    {
        this(sourceLocation, id, Optional.empty(), source, columnNames, outputVariables);
    }

    public OutputNode(
            Optional<SourceLocation> sourceLocation,
            PlanNodeId id,
            Optional<PlanNode> statsEquivalentPlanNode,
            PlanNode source,
            List<String> columnNames,
            List<VariableReferenceExpression> outputVariables)
    {
        super(sourceLocation, id, statsEquivalentPlanNode);

        requireNonNull(source, "source is null");
        requireNonNull(columnNames, "columnNames is null");
        if (columnNames.size() != outputVariables.size()) {
            throw new IllegalArgumentException("columnNames and assignments sizes don't match");
        }

        this.source = source;
        this.columnNames = columnNames;
        this.outputVariables = unmodifiableList(outputVariables);
    }

    @Override
    public List<PlanNode> getSources()
    {
        return unmodifiableList(Collections.singletonList(source));
    }

    @Override
    @JsonProperty
    public List<VariableReferenceExpression> getOutputVariables()
    {
        return outputVariables;
    }

    @JsonProperty
    public List<String> getColumnNames()
    {
        return columnNames;
    }

    @JsonProperty
    public PlanNode getSource()
    {
        return source;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitOutput(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        if (newChildren.size() != 1) {
            throw new IllegalArgumentException("new children size must be one.");
        }
        return new OutputNode(getSourceLocation(), getId(), getStatsEquivalentPlanNode(), newChildren.get(0), columnNames, outputVariables);
    }

    @Override
    public PlanNode assignStatsEquivalentPlanNode(Optional<PlanNode> statsEquivalentPlanNode)
    {
        return new OutputNode(getSourceLocation(), getId(), statsEquivalentPlanNode, source, columnNames, outputVariables);
    }
}
