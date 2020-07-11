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
public final class DistinctLimitNode
        extends PlanNode
{
    private final PlanNode source;
    private final long limit;
    private final boolean partial;
    private final List<VariableReferenceExpression> distinctVariables;
    private final Optional<VariableReferenceExpression> hashVariable;

    @JsonCreator
    public DistinctLimitNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("limit") long limit,
            @JsonProperty("partial") boolean partial,
            @JsonProperty("distinctVariables") List<VariableReferenceExpression> distinctVariables,
            @JsonProperty("hashVariable") Optional<VariableReferenceExpression> hashVariable)
    {
        super(id);
        this.source = requireNonNull(source, "source is null");
        checkArgument(limit >= 0, "limit must be greater than or equal to zero");
        this.limit = limit;
        this.partial = partial;
        this.distinctVariables = unmodifiableList(distinctVariables);
        this.hashVariable = requireNonNull(hashVariable, "hashVariable is null");
        checkArgument(!hashVariable.isPresent() || !distinctVariables.contains(hashVariable.get()), "distinctVariables should not contain hash variable");
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
    public long getLimit()
    {
        return limit;
    }

    @JsonProperty
    public boolean isPartial()
    {
        return partial;
    }

    @JsonProperty
    public Optional<VariableReferenceExpression> getHashVariable()
    {
        return hashVariable;
    }

    @JsonProperty
    public List<VariableReferenceExpression> getDistinctVariables()
    {
        return distinctVariables;
    }

    @Override
    public List<VariableReferenceExpression> getOutputVariables()
    {
        ArrayList<VariableReferenceExpression> outputVariables = new ArrayList<>(distinctVariables);
        hashVariable.ifPresent(outputVariables::add);
        return unmodifiableList(outputVariables);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitDistinctLimit(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        checkArgument(newChildren.size() == 1, "Unexpected number of elements in list newChildren");
        return new DistinctLimitNode(getId(), newChildren.get(0), limit, partial, distinctVariables, hashVariable);
    }

    private static void checkArgument(boolean condition, String message)
    {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }
}
