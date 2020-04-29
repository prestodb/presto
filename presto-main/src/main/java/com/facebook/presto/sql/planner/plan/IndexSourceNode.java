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

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.metadata.IndexHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class IndexSourceNode
        extends InternalPlanNode
{
    private final IndexHandle indexHandle;
    private final TableHandle tableHandle;
    private final Set<VariableReferenceExpression> lookupVariables;
    private final List<VariableReferenceExpression> outputVariables;
    private final Map<VariableReferenceExpression, ColumnHandle> assignments; // symbol -> column
    private final TupleDomain<ColumnHandle> currentConstraint; // constraint over the input data the operator will guarantee

    @JsonCreator
    public IndexSourceNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("indexHandle") IndexHandle indexHandle,
            @JsonProperty("tableHandle") TableHandle tableHandle,
            @JsonProperty("lookupVariables") Set<VariableReferenceExpression> lookupVariables,
            @JsonProperty("outputVariables") List<VariableReferenceExpression> outputVariables,
            @JsonProperty("assignments") Map<VariableReferenceExpression, ColumnHandle> assignments,
            @JsonProperty("currentConstraint") TupleDomain<ColumnHandle> currentConstraint)
    {
        super(id);
        this.indexHandle = requireNonNull(indexHandle, "indexHandle is null");
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        this.lookupVariables = ImmutableSet.copyOf(requireNonNull(lookupVariables, "lookupVariables is null"));
        this.outputVariables = ImmutableList.copyOf(requireNonNull(outputVariables, "outputVariables is null"));
        this.assignments = ImmutableMap.copyOf(requireNonNull(assignments, "assignments is null"));
        this.currentConstraint = requireNonNull(currentConstraint, "effectiveTupleDomain is null");
        checkArgument(!lookupVariables.isEmpty(), "lookupVariables is empty");
        checkArgument(!outputVariables.isEmpty(), "outputVariables is empty");
        checkArgument(assignments.keySet().containsAll(lookupVariables), "Assignments do not include all lookup variables");
        checkArgument(outputVariables.containsAll(lookupVariables), "Lookup variables need to be part of the output variables");
    }

    @JsonProperty
    public IndexHandle getIndexHandle()
    {
        return indexHandle;
    }

    @JsonProperty
    public TableHandle getTableHandle()
    {
        return tableHandle;
    }

    @JsonProperty
    public Set<VariableReferenceExpression> getLookupVariables()
    {
        return lookupVariables;
    }

    @Override
    @JsonProperty
    public List<VariableReferenceExpression> getOutputVariables()
    {
        return outputVariables;
    }

    @JsonProperty
    public Map<VariableReferenceExpression, ColumnHandle> getAssignments()
    {
        return assignments;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getCurrentConstraint()
    {
        return currentConstraint;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of();
    }

    @Override
    public <R, C> R accept(InternalPlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitIndexSource(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        checkArgument(newChildren.isEmpty(), "newChildren is not empty");
        return this;
    }
}
