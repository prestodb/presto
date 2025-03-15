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

import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.CanonicalJoinNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.expressions.LogicalRowExpressions.extractConjuncts;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * This class represents a set of inner joins that can be executed in any order.
 */
public class MultiJoinNode
{
    // Use a linked hash set to ensure optimizer is deterministic
    protected CanonicalJoinNode node;
    protected Assignments assignments;
    private final boolean containsCombinedSources;
    private final Optional<RowExpression> joinFilter;

    public MultiJoinNode(LinkedHashSet<PlanNode> sources, RowExpression filter, List<VariableReferenceExpression> outputVariables,
            Assignments assignments, boolean containsCombinedSources, Optional<RowExpression> joinFilter)
    {
        requireNonNull(sources, "sources is null");
        requireNonNull(filter, "filter is null");
        requireNonNull(outputVariables, "outputVariables is null");
        requireNonNull(assignments, "assignments is null");

        this.assignments = assignments;
        // Plan node id doesn't matter here as we don't use this in planner
        this.node = new CanonicalJoinNode(
                new PlanNodeId(""),
                sources.stream().collect(toImmutableList()),
                INNER,
                ImmutableSet.of(),
                ImmutableSet.of(filter),
                outputVariables);
        this.containsCombinedSources = containsCombinedSources;
        this.joinFilter = joinFilter;
    }

    public RowExpression getFilter()
    {
        return node.getFilters().stream().findAny().get();
    }

    public LinkedHashSet<PlanNode> getSources()
    {
        return new LinkedHashSet<>(node.getSources());
    }

    public List<VariableReferenceExpression> getOutputVariables()
    {
        return node.getOutputVariables();
    }

    public Assignments getAssignments()
    {
        return assignments;
    }

    public Optional<RowExpression> getJoinFilter()
    {
        return joinFilter;
    }

    public boolean getContainsCombinedSources()
    {
        return containsCombinedSources;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getSources(), ImmutableSet.copyOf(extractConjuncts(getFilter())), getOutputVariables());
    }

    @Override
    public boolean equals(Object obj)
    {
        if (!(obj instanceof MultiJoinNode)) {
            return false;
        }

        MultiJoinNode other = (MultiJoinNode) obj;
        return getSources().equals(other.getSources())
                && ImmutableSet.copyOf(extractConjuncts(getFilter())).equals(ImmutableSet.copyOf(extractConjuncts(other.getFilter())))
                && getOutputVariables().equals(other.getOutputVariables())
                && getAssignments().equals(other.getAssignments());
    }

    @Override
    public String toString()
    {
        return "MultiJoinNode{" +
                "node=" + node +
                ", assignments=" + assignments +
                '}';
    }

    public static class Builder
    {
        private List<PlanNode> sources;
        private RowExpression filter;
        private List<VariableReferenceExpression> outputVariables;
        private Assignments assignments = Assignments.of();
        private boolean containsCombinedSources;
        private Optional<RowExpression> joinFilter;

        public MultiJoinNode.Builder setSources(PlanNode... sources)
        {
            this.sources = ImmutableList.copyOf(sources);
            return this;
        }

        public MultiJoinNode.Builder setFilter(RowExpression filter)
        {
            this.filter = filter;
            return this;
        }

        public MultiJoinNode.Builder setAssignments(Assignments assignments)
        {
            this.assignments = assignments;
            return this;
        }

        public MultiJoinNode.Builder setOutputVariables(VariableReferenceExpression... outputVariables)
        {
            this.outputVariables = ImmutableList.copyOf(outputVariables);
            return this;
        }

        public MultiJoinNode.Builder setContainsCombinedSources(boolean containsCombinedSources)
        {
            this.containsCombinedSources = containsCombinedSources;
            return this;
        }

        public MultiJoinNode.Builder setJoinFilter(Optional<RowExpression> joinFilter)
        {
            this.joinFilter = joinFilter;
            return this;
        }
        public MultiJoinNode build()
        {
            return new MultiJoinNode(new LinkedHashSet<>(sources), filter, outputVariables, assignments, containsCombinedSources, joinFilter);
        }
    }
}
