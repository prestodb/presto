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

package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;

import static com.facebook.presto.sql.ExpressionUtils.and;
import static com.facebook.presto.sql.ExpressionUtils.extractConjuncts;
import static com.facebook.presto.sql.planner.DeterminismEvaluator.isDeterministic;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * This class represents a set of inner joins that can be executed in any order.
 */
class MultiJoinNode
{
    // Use a linked hash set to ensure optimizer is deterministic
    private final LinkedHashSet<PlanNode> sources;
    private final Expression filter;
    private final List<Symbol> outputSymbols;

    public MultiJoinNode(LinkedHashSet<PlanNode> sources, Expression filter, List<Symbol> outputSymbols)
    {
        requireNonNull(sources, "sources is null");
        checkArgument(sources.size() > 1);
        requireNonNull(filter, "filter is null");
        requireNonNull(outputSymbols, "outputSymbols is null");

        this.sources = sources;
        this.filter = filter;
        this.outputSymbols = ImmutableList.copyOf(outputSymbols);

        List<Symbol> inputSymbols = sources.stream().flatMap(source -> source.getOutputSymbols().stream()).collect(toImmutableList());
        checkArgument(inputSymbols.containsAll(outputSymbols), "inputs do not contain all output symbols");
    }

    public Expression getFilter()
    {
        return filter;
    }

    public LinkedHashSet<PlanNode> getSources()
    {
        return sources;
    }

    public List<Symbol> getOutputSymbols()
    {
        return outputSymbols;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(sources, ImmutableSet.copyOf(extractConjuncts(filter)), outputSymbols);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (!(obj instanceof MultiJoinNode)) {
            return false;
        }

        MultiJoinNode other = (MultiJoinNode) obj;
        return this.sources.equals(other.sources)
                && ImmutableSet.copyOf(extractConjuncts(this.filter)).equals(ImmutableSet.copyOf(extractConjuncts(other.filter)))
                && this.outputSymbols.equals(other.outputSymbols);
    }

    static MultiJoinNode toMultiJoinNode(JoinNode joinNode, Lookup lookup, int joinLimit)
    {
        return new JoinNodeFlattener(joinNode, lookup, joinLimit).toMultiJoinNode();
    }

    private static class JoinNodeFlattener
    {
        private final LinkedHashSet<PlanNode> sources = new LinkedHashSet<>();
        private final List<Expression> filters = new ArrayList<>();
        private final List<Symbol> outputSymbols;
        private final Lookup lookup;

        JoinNodeFlattener(JoinNode node, Lookup lookup, int joinLimit)
        {
            requireNonNull(node, "node is null");
            checkState(node.getType() == INNER, "join type must be INNER");
            this.outputSymbols = node.getOutputSymbols();
            this.lookup = requireNonNull(lookup, "lookup is null");
            flattenNode(node, joinLimit);
        }

        private void flattenNode(PlanNode node, int limit)
        {
            PlanNode resolved = lookup.resolve(node);

            // (limit - 2) because you need to account for adding left and right side
            if (!(resolved instanceof JoinNode) || (sources.size() > (limit - 2))) {
                sources.add(node);
                return;
            }

            JoinNode joinNode = (JoinNode) resolved;
            if (joinNode.getType() != INNER || !isDeterministic(joinNode.getFilter().orElse(TRUE_LITERAL)) || joinNode.getDistributionType().isPresent()) {
                sources.add(node);
                return;
            }

            // we set the left limit to limit - 1 to account for the node on the right
            flattenNode(joinNode.getLeft(), limit - 1);
            flattenNode(joinNode.getRight(), limit);
            joinNode.getCriteria().stream()
                    .map(JoinNode.EquiJoinClause::toExpression)
                    .forEach(filters::add);
            joinNode.getFilter().ifPresent(filters::add);
        }

        MultiJoinNode toMultiJoinNode()
        {
            return new MultiJoinNode(sources, and(filters), outputSymbols);
        }
    }
}
