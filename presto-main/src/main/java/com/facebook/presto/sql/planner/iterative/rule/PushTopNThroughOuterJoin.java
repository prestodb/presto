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

import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.sql.planner.OrderingScheme;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.TopNNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.sql.planner.optimizations.QueryCardinalityUtil.extractCardinality;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.FULL;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.LEFT;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.RIGHT;
import static com.facebook.presto.sql.planner.plan.Patterns.Join.type;
import static com.facebook.presto.sql.planner.plan.Patterns.join;
import static com.facebook.presto.sql.planner.plan.Patterns.project;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.facebook.presto.sql.planner.plan.Patterns.topN;
import static com.facebook.presto.sql.planner.plan.TopNNode.Step;

public class PushTopNThroughOuterJoin
        implements Rule<TopNNode>
{
    private static final Capture<ProjectNode> PROJECT_CHILD = newCapture();
    private static final Capture<JoinNode> JOIN_CHILD = newCapture();

    private static final Pattern<TopNNode> PATTERN =
            topN().with(source().matching(
                    project().capturedAs(PROJECT_CHILD).with(source().matching(
                            join().capturedAs(JOIN_CHILD).with(type().matching(type -> isLeftOuter(type) || isRightOuter(type)))))));

    @Override
    public Pattern<TopNNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(TopNNode parent, Captures captures, Context context)
    {
        ProjectNode projectNode = captures.get(PROJECT_CHILD);
        JoinNode joinNode = captures.get(JOIN_CHILD);

        Optional<OrderingScheme> pushdownOrderingScheme = remapOrderingScheme(parent.getOrderingScheme(), projectNode.getAssignments());
        if (!pushdownOrderingScheme.isPresent()) {
            return Result.empty();
        }

        Set<Symbol> pushdownOrderBySymbols = ImmutableSet.copyOf(pushdownOrderingScheme.get().getOrderBy());

        Set<Symbol> leftSymbols = ImmutableSet.copyOf(joinNode.getLeft().getOutputSymbols());
        if (leftSymbols.containsAll(pushdownOrderBySymbols)
                && isLeftOuter(joinNode.getType())
                && !isLimited(joinNode.getLeft(), context.getLookup(), parent.getCount())) {
            TopNNode left = new TopNNode(context.getIdAllocator().getNextId(), joinNode.getLeft(), parent.getCount(), pushdownOrderingScheme.get(), Step.PARTIAL);

            return Result.ofPlanNode(
                    parent.replaceChildren(ImmutableList.of(
                            projectNode.replaceChildren(ImmutableList.of(
                                    joinNode.replaceChildren(ImmutableList.of(
                                            left,
                                            joinNode.getRight())))))));
        }

        Set<Symbol> rightSymbols = ImmutableSet.copyOf(joinNode.getRight().getOutputSymbols());
        if (rightSymbols.containsAll(pushdownOrderBySymbols)
                && isRightOuter(joinNode.getType())
                && !isLimited(joinNode.getRight(), context.getLookup(), parent.getCount())) {
            TopNNode right = new TopNNode(context.getIdAllocator().getNextId(), joinNode.getRight(), parent.getCount(), pushdownOrderingScheme.get(), Step.PARTIAL);

            return Result.ofPlanNode(
                    parent.replaceChildren(ImmutableList.of(
                            projectNode.replaceChildren(ImmutableList.of(
                                    joinNode.replaceChildren(ImmutableList.of(
                                            joinNode.getLeft(),
                                            right)))))));
        }

        return Result.empty();
    }

    private static Optional<OrderingScheme> remapOrderingScheme(OrderingScheme orderingScheme, Assignments assignments)
    {
        ImmutableList.Builder<Symbol> newOrderBy = ImmutableList.builder();
        ImmutableMap.Builder<Symbol, SortOrder> newOrderings = ImmutableMap.builder();
        for (Map.Entry<Symbol, SortOrder> ordering : orderingScheme.getOrderings().entrySet()) {
            Expression expression = assignments.get(ordering.getKey());
            if (!(expression instanceof SymbolReference)) {
                return Optional.empty();
            }
            Symbol remappedSymbol = Symbol.from(expression);
            newOrderBy.add(remappedSymbol);
            newOrderings.put(remappedSymbol, ordering.getValue());
        }
        return Optional.of(new OrderingScheme(newOrderBy.build(), newOrderings.build()));
    }

    private static boolean isLimited(PlanNode node, Lookup lookup, long limit)
    {
        Range<Long> cardinality = extractCardinality(node, lookup);
        return cardinality.hasUpperBound() && cardinality.upperEndpoint() <= limit;
    }

    private static boolean isLeftOuter(JoinNode.Type type)
    {
        return type == LEFT || type == FULL;
    }

    private static boolean isRightOuter(JoinNode.Type type)
    {
        return type == RIGHT || type == FULL;
    }
}
