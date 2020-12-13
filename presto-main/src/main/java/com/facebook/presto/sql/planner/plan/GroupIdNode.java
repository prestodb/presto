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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import javax.annotation.concurrent.Immutable;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.util.MoreLists.listOfListsCopy;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;

@Immutable
public class GroupIdNode
        extends InternalPlanNode
{
    private final PlanNode source;

    // in terms of output variables
    private final List<List<VariableReferenceExpression>> groupingSets;

    // tracks how each grouping set column is derived from an input column
    private final Map<VariableReferenceExpression, VariableReferenceExpression> groupingColumns;
    private final List<VariableReferenceExpression> aggregationArguments;

    private final VariableReferenceExpression groupIdVariable;

    @JsonCreator
    public GroupIdNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("groupingSets") List<List<VariableReferenceExpression>> groupingSets,
            @JsonProperty("groupingColumns") Map<VariableReferenceExpression, VariableReferenceExpression> groupingColumns,
            @JsonProperty("aggregationArguments") List<VariableReferenceExpression> aggregationArguments,
            @JsonProperty("groupIdVariable") VariableReferenceExpression groupIdVariable)
    {
        super(id);
        this.source = requireNonNull(source);
        this.groupingSets = listOfListsCopy(requireNonNull(groupingSets, "groupingSets is null"));
        this.groupingColumns = ImmutableMap.copyOf(requireNonNull(groupingColumns));
        this.aggregationArguments = ImmutableList.copyOf(aggregationArguments);
        this.groupIdVariable = requireNonNull(groupIdVariable);

        checkArgument(Sets.intersection(groupingColumns.keySet(), ImmutableSet.copyOf(aggregationArguments)).isEmpty(), "aggregation columns and grouping set columns must be a disjoint set");
    }

    @Override
    public List<VariableReferenceExpression> getOutputVariables()
    {
        return ImmutableList.<VariableReferenceExpression>builder()
                .addAll(groupingSets.stream()
                        .flatMap(Collection::stream)
                        .collect(toSet()))
                .addAll(aggregationArguments)
                .add(groupIdVariable)
                .build();
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @JsonProperty
    public PlanNode getSource()
    {
        return source;
    }

    @JsonProperty
    public List<List<VariableReferenceExpression>> getGroupingSets()
    {
        return groupingSets;
    }

    @JsonProperty
    public Map<VariableReferenceExpression, VariableReferenceExpression> getGroupingColumns()
    {
        return groupingColumns;
    }

    @JsonProperty
    public List<VariableReferenceExpression> getAggregationArguments()
    {
        return aggregationArguments;
    }

    @JsonProperty
    public VariableReferenceExpression getGroupIdVariable()
    {
        return groupIdVariable;
    }

    @Override
    public <R, C> R accept(InternalPlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitGroupId(this, context);
    }

    public Set<VariableReferenceExpression> getInputVariables()
    {
        return ImmutableSet.<VariableReferenceExpression>builder()
                .addAll(aggregationArguments)
                .addAll(groupingSets.stream()
                        .map(set -> set.stream()
                                .map(groupingColumns::get).collect(Collectors.toList()))
                        .flatMap(Collection::stream)
                        .collect(toSet()))
                .build();
    }

    // returns the common grouping columns in terms of output symbols
    public Set<VariableReferenceExpression> getCommonGroupingColumns()
    {
        Set<VariableReferenceExpression> intersection = new HashSet<>(groupingSets.get(0));
        for (int i = 1; i < groupingSets.size(); i++) {
            intersection.retainAll(groupingSets.get(i));
        }
        return ImmutableSet.copyOf(intersection);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new GroupIdNode(getId(), Iterables.getOnlyElement(newChildren), groupingSets, groupingColumns, aggregationArguments, groupIdVariable);
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
        GroupIdNode that = (GroupIdNode) o;
        return Objects.equals(source, that.source) &&
                Objects.equals(groupingSets, that.groupingSets) &&
                Objects.equals(groupingColumns, that.groupingColumns) &&
                Objects.equals(aggregationArguments, that.aggregationArguments) &&
                Objects.equals(groupIdVariable, that.groupIdVariable);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(source, groupingSets, groupingColumns, aggregationArguments, groupIdVariable);
    }
}
