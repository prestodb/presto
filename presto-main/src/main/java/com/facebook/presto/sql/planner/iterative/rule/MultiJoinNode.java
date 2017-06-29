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

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.sql.ExpressionUtils.and;
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
    private static final int JOIN_LIMIT = 10;

    private final List<PlanNode> sources;
    private final Expression filter;
    private final List<Symbol> outputSymbols;

    public MultiJoinNode(List<PlanNode> sources, Expression filter, List<Symbol> outputSymbols)
    {
        this.sources = ImmutableList.copyOf(requireNonNull(sources, "sources is null"));
        this.filter = requireNonNull(filter, "filter is null");
        this.outputSymbols = ImmutableList.copyOf(requireNonNull(outputSymbols, "outputSymbols is null"));

        List<Symbol> inputSymbols = sources.stream().flatMap(source -> source.getOutputSymbols().stream()).collect(toImmutableList());
        checkArgument(inputSymbols.containsAll(outputSymbols), "inputs do not contain all output symbols");
    }

    public Expression getFilter()
    {
        return filter;
    }

    public List<PlanNode> getSources()
    {
        return sources;
    }

    public List<Symbol> getOutputSymbols()
    {
        return outputSymbols;
    }

    static MultiJoinNode toMultiJoinNode(JoinNode joinNode, Lookup lookup)
    {
        return new MultiJoinNodeBuilder(joinNode, lookup).toMultiJoinNode();
    }

    private static class MultiJoinNodeBuilder
    {
        private final List<PlanNode> sources = new ArrayList<>();
        private final List<Expression> filters = new ArrayList<>();
        private final List<Symbol> outputSymbols;
        private final Lookup lookup;

        MultiJoinNodeBuilder(JoinNode node, Lookup lookup)
        {
            requireNonNull(node, "node is null");
            checkState(node.getType() == INNER, "join type must be INNER");
            this.outputSymbols = node.getOutputSymbols();
            this.lookup = requireNonNull(lookup, "lookup is null");
            flattenNode(node);
        }

        private void flattenNode(PlanNode node)
        {
            PlanNode resolved = lookup.resolve(node);
            if (resolved instanceof JoinNode && sources.size() < JOIN_LIMIT) {
                JoinNode joinNode = (JoinNode) resolved;
                if (joinNode.getType() == INNER && isDeterministic(joinNode.getFilter().orElse(TRUE_LITERAL))) {
                    flattenNode(joinNode.getLeft());
                    flattenNode(joinNode.getRight());
                    joinNode.getCriteria().stream()
                            .map(JoinNode.EquiJoinClause::toExpression)
                            .forEach(filters::add);
                    joinNode.getFilter().ifPresent(filters::add);
                    return;
                }
            }
            sources.add(node);
        }

        MultiJoinNode toMultiJoinNode()
        {
            return new MultiJoinNode(sources, and(filters), outputSymbols);
        }
    }
}
