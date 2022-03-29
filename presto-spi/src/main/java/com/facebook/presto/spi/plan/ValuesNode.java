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
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.Immutable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

@Immutable
public final class ValuesNode
        extends PlanNode
{
    private final List<VariableReferenceExpression> outputVariables;
    private final List<List<RowExpression>> rows;

    @JsonCreator
    public ValuesNode(
            Optional<SourceLocation> sourceLocation,
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("outputVariables") List<VariableReferenceExpression> outputVariables,
            @JsonProperty("rows") List<List<RowExpression>> rows)
    {
        super(sourceLocation, id);
        this.outputVariables = immutableListCopyOf(outputVariables);
        this.rows = immutableListCopyOf(requireNonNull(rows, "lists is null").stream().map(ValuesNode::immutableListCopyOf).collect(toList()));

        for (List<RowExpression> row : rows) {
            if (!(row.size() == outputVariables.size() || row.size() == 0)) {
                throw new IllegalArgumentException(format("Expected row to have %s values, but row has %s values", outputVariables.size(), row.size()));
            }
        }
    }

    @JsonProperty
    public List<List<RowExpression>> getRows()
    {
        return rows;
    }

    @Override
    @JsonProperty
    public List<VariableReferenceExpression> getOutputVariables()
    {
        return outputVariables;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return unmodifiableList(emptyList());
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitValues(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        if (!newChildren.isEmpty()) {
            throw new IllegalArgumentException("newChildren is not empty");
        }
        return this;
    }

    private static <T> List<T> immutableListCopyOf(List<T> list)
    {
        return unmodifiableList(new ArrayList<>(requireNonNull(list, "list is null")));
    }

    public ValuesNode deepCopy(
            PlanNodeIdAllocator planNodeIdAllocator,
            VariableAllocator variableAllocator,
            Map<VariableReferenceExpression, VariableReferenceExpression> variableMappings)
    {
        ImmutableList.Builder<VariableReferenceExpression> outputVariablesBuilder = ImmutableList.builder();
        for (VariableReferenceExpression variable : getOutputVariables()) {
            variableMappings.put(variable, variableAllocator.newVariable(variable.getSourceLocation(), variable.getName(), variable.getType()));
            outputVariablesBuilder.add(variableMappings.get(variable));
        }

        ImmutableList.Builder<List<RowExpression>> rowsBuilder = ImmutableList.builder();
        for (List<RowExpression> columns : getRows()) {
            ImmutableList.Builder<RowExpression> values = ImmutableList.builder();
            for (RowExpression item : columns) {
                values.add(item.deepCopy(variableMappings));
            }
            rowsBuilder.add(values.build());
        }

        return new ValuesNode(getSourceLocation(), planNodeIdAllocator.getNextId(), outputVariablesBuilder.build(), rowsBuilder.build());
    }
}
