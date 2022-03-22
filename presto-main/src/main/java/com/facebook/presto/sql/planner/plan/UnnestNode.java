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

import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@Immutable
public class UnnestNode
        extends InternalPlanNode
{
    private final PlanNode source;
    private final List<VariableReferenceExpression> replicateVariables;
    private final Map<VariableReferenceExpression, List<VariableReferenceExpression>> unnestVariables;
    private final Optional<VariableReferenceExpression> ordinalityVariable;

    @JsonCreator
    public UnnestNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("replicateVariables") List<VariableReferenceExpression> replicateVariables,
            @JsonProperty("unnestVariables") Map<VariableReferenceExpression, List<VariableReferenceExpression>> unnestVariables,
            @JsonProperty("ordinalityVariable") Optional<VariableReferenceExpression> ordinalityVariable)
    {
        super(id);
        this.source = requireNonNull(source, "source is null");
        this.replicateVariables = ImmutableList.copyOf(requireNonNull(replicateVariables, "replicateVariables is null"));
        checkArgument(source.getOutputVariables().containsAll(replicateVariables), "Source does not contain all replicateSymbols");
        requireNonNull(unnestVariables, "unnestVariables is null");
        checkArgument(!unnestVariables.isEmpty(), "unnestVariables is empty");
        ImmutableMap.Builder<VariableReferenceExpression, List<VariableReferenceExpression>> builder = ImmutableMap.builder();
        for (Map.Entry<VariableReferenceExpression, List<VariableReferenceExpression>> entry : unnestVariables.entrySet()) {
            builder.put(entry.getKey(), ImmutableList.copyOf(entry.getValue()));
        }
        this.unnestVariables = builder.build();
        this.ordinalityVariable = requireNonNull(ordinalityVariable, "ordinalityVariable is null");
    }

    @Override
    public List<VariableReferenceExpression> getOutputVariables()
    {
        ImmutableList.Builder<VariableReferenceExpression> outputSymbolsBuilder = ImmutableList.<VariableReferenceExpression>builder()
                .addAll(replicateVariables)
                .addAll(Iterables.concat(unnestVariables.values()));
        ordinalityVariable.ifPresent(outputSymbolsBuilder::add);
        return outputSymbolsBuilder.build();
    }

    @JsonProperty
    public PlanNode getSource()
    {
        return source;
    }

    @JsonProperty
    public List<VariableReferenceExpression> getReplicateVariables()
    {
        return replicateVariables;
    }

    @JsonProperty
    public Map<VariableReferenceExpression, List<VariableReferenceExpression>> getUnnestVariables()
    {
        return unnestVariables;
    }

    @JsonProperty
    public Optional<VariableReferenceExpression> getOrdinalityVariable()
    {
        return ordinalityVariable;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @Override
    public <R, C> R accept(InternalPlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitUnnest(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new UnnestNode(getId(), Iterables.getOnlyElement(newChildren), replicateVariables, unnestVariables, ordinalityVariable);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        UnnestNode that = (UnnestNode) o;
        return Objects.equals(source, that.source) &&
                Objects.equals(replicateVariables, that.replicateVariables) &&
                Objects.equals(unnestVariables, that.unnestVariables) &&
                Objects.equals(ordinalityVariable, that.ordinalityVariable);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(source, replicateVariables, unnestVariables, ordinalityVariable);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("source", source)
                .add("replicateVariables", replicateVariables)
                .add("unnestVariables", unnestVariables)
                .add("ordinalityVariable", ordinalityVariable)
                .toString();
    }
}
