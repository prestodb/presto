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
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;

import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.spi.plan.TopNNode.Step.PARTIAL;
import static com.facebook.presto.sql.planner.optimizations.QueryCardinalityUtil.isAtMost;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.LEFT;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.RIGHT;
import static com.facebook.presto.sql.planner.plan.Patterns.Join.type;
import static com.facebook.presto.sql.planner.plan.Patterns.TopN.step;
import static com.facebook.presto.sql.planner.plan.Patterns.join;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.facebook.presto.sql.planner.plan.Patterns.topN;

/**
 * Pushes a partial top N operation through a left or right outer join onto the preserved input.
 * The ordering columns of the top N must be rewriteable to refer to only columns of
 * the preserved input. Note that full outer joins are excluded as they
 * can inject order affecting null rows into the join result.
 */
public class PushTopNThroughOuterJoin
        implements Rule<TopNNode>
{
    private static final Capture<JoinNode> JOIN_CHILD = newCapture();

    private static final Pattern<TopNNode> PATTERN =
            topN().with(step().equalTo(PARTIAL))
                    .with(source().matching(
                            join().capturedAs(JOIN_CHILD).with(type().matching(type -> type == LEFT || type == RIGHT))));

    @Override
    public Pattern<TopNNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(TopNNode parent, Captures captures, Context context)
    {
        JoinNode joinNode = captures.get(JOIN_CHILD);

        List<VariableReferenceExpression> orderBySymbols = parent.getOrderingScheme().getOrderByVariables();

        PlanNode left = joinNode.getLeft();
        PlanNode right = joinNode.getRight();
        JoinNode.Type type = joinNode.getType();

        if ((type == LEFT)
                && ImmutableSet.copyOf(left.getOutputVariables()).containsAll(orderBySymbols)
                && !isAtMost(left, context.getLookup(), parent.getCount())) {
            return Result.ofPlanNode(
                    joinNode.replaceChildren(ImmutableList.of(
                            parent.replaceChildren(ImmutableList.of(left)),
                            right)));
        }

        if ((type == RIGHT)
                && ImmutableSet.copyOf(right.getOutputVariables()).containsAll(orderBySymbols)
                && !isAtMost(right, context.getLookup(), parent.getCount())) {
            return Result.ofPlanNode(
                    joinNode.replaceChildren(ImmutableList.of(
                            left,
                            parent.replaceChildren(ImmutableList.of(right)))));
        }

        return Result.empty();
    }
}
