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

import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.Immutable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

@Immutable
public final class MarkDistinctNode
        extends PlanNode
{
    private final PlanNode source;
    private final VariableReferenceExpression markerVariable;

    private final Optional<VariableReferenceExpression> hashVariable;
    private final List<VariableReferenceExpression> distinctVariables;
    private final List<VariableReferenceExpression> output;

    @JsonCreator
    public MarkDistinctNode(@JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("markerVariable") VariableReferenceExpression markerVariable,
            @JsonProperty("distinctVariables") List<VariableReferenceExpression> distinctVariables,
            @JsonProperty("hashVariable") Optional<VariableReferenceExpression> hashVariable)
    {
        super(id);
        this.source = source;
        this.markerVariable = markerVariable;
        this.hashVariable = requireNonNull(hashVariable, "hashVariable is null");
        requireNonNull(distinctVariables, "distinctVariables is null");
        checkArgument(!distinctVariables.isEmpty(), "distinctVariables cannot be empty");
        this.distinctVariables = unmodifiableList(new ArrayList<>(distinctVariables));
        List<VariableReferenceExpression> variableReferenceExpressions = new ArrayList<>(source.getOutputVariables());
        variableReferenceExpressions.add(markerVariable);
        this.output = unmodifiableList(variableReferenceExpressions);
    }

    @Override
    public List<VariableReferenceExpression> getOutputVariables()
    {
        return output;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return unmodifiableList(Collections.singletonList(source));
    }

    @JsonProperty
    public PlanNode getSource()
    {
        return source;
    }

    @JsonProperty
    public VariableReferenceExpression getMarkerVariable()
    {
        return markerVariable;
    }

    @JsonProperty
    public List<VariableReferenceExpression> getDistinctVariables()
    {
        return distinctVariables;
    }

    @JsonProperty
    public Optional<VariableReferenceExpression> getHashVariable()
    {
        return hashVariable;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitMarkDistinct(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        checkArgument(newChildren.size() == 1, "Unexpected number of elements in list newChildren");
        return new MarkDistinctNode(getId(), newChildren.get(0), markerVariable, distinctVariables, hashVariable);
    }

    private static void checkArgument(boolean condition, String message)
    {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }
}
