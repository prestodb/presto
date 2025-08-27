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
import com.google.errorprone.annotations.Immutable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.common.Utils.checkArgument;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;

@Immutable
public final class UnnestNode
        extends PlanNode
{
    private final PlanNode source;
    private final List<VariableReferenceExpression> replicateVariables;
    private final Map<VariableReferenceExpression, List<VariableReferenceExpression>> unnestVariables;
    private final Optional<VariableReferenceExpression> ordinalityVariable;

    @JsonCreator
    public UnnestNode(
            Optional<SourceLocation> sourceLocation,
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("replicateVariables") List<VariableReferenceExpression> replicateVariables,
            @JsonProperty("unnestVariables") Map<VariableReferenceExpression, List<VariableReferenceExpression>> unnestVariables,
            @JsonProperty("ordinalityVariable") Optional<VariableReferenceExpression> ordinalityVariable)
    {
        this(sourceLocation, id, Optional.empty(), source, replicateVariables, unnestVariables, ordinalityVariable);
    }

    public UnnestNode(
            Optional<SourceLocation> sourceLocation,
            PlanNodeId id,
            Optional<PlanNode> statsEquivalentPlanNode,
            PlanNode source,
            List<VariableReferenceExpression> replicateVariables,
            Map<VariableReferenceExpression, List<VariableReferenceExpression>> unnestVariables,
            Optional<VariableReferenceExpression> ordinalityVariable)
    {
        super(sourceLocation, id, statsEquivalentPlanNode);
        this.source = requireNonNull(source, "source is null");
        this.replicateVariables = unmodifiableList(new ArrayList<>(requireNonNull(replicateVariables, "replicateVariables is null")));
        checkArgument(source.getOutputVariables().containsAll(replicateVariables), "Source does not contain all replicateSymbols");
        requireNonNull(unnestVariables, "unnestVariables is null");
        checkArgument(!unnestVariables.isEmpty(), "unnestVariables is empty");
        Map<VariableReferenceExpression, List<VariableReferenceExpression>> unnestVariablesMap = new LinkedHashMap<>();
        for (Map.Entry<VariableReferenceExpression, List<VariableReferenceExpression>> entry : unnestVariables.entrySet()) {
            unnestVariablesMap.put(entry.getKey(), unmodifiableList(new ArrayList<>(entry.getValue())));
        }
        this.unnestVariables = unmodifiableMap(unnestVariablesMap);
        this.ordinalityVariable = requireNonNull(ordinalityVariable, "ordinalityVariable is null");
    }

    @Override
    public List<VariableReferenceExpression> getOutputVariables()
    {
        List<VariableReferenceExpression> outputSymbols = new ArrayList<>(replicateVariables);
        unnestVariables.values().forEach(outputSymbols::addAll);
        ordinalityVariable.ifPresent(outputSymbols::add);
        return unmodifiableList(outputSymbols);
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
        return singletonList(source);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitUnnest(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        checkArgument(newChildren.size() == 1);
        return new UnnestNode(getSourceLocation(), getId(), getStatsEquivalentPlanNode(), newChildren.get(0), replicateVariables, unnestVariables, ordinalityVariable);
    }

    @Override
    public PlanNode assignStatsEquivalentPlanNode(Optional<PlanNode> statsEquivalentPlanNode)
    {
        return new UnnestNode(getSourceLocation(), getId(), statsEquivalentPlanNode, source, replicateVariables, unnestVariables, ordinalityVariable);
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
        return format("%s {source=%s, replicateVariables=%s, unnestVariables=%s, ordinalityVariable=%s}", this.getClass().getSimpleName(), source, replicateVariables, unnestVariables, ordinalityVariable);
    }
}
