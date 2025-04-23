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

import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.google.common.collect.ImmutableList;

import java.util.Optional;

import static com.facebook.presto.sql.planner.optimizations.QueryCardinalityUtil.isEmpty;
import static com.facebook.presto.sql.planner.plan.Patterns.join;

public class RemoveRedundantJoin
        implements Rule<JoinNode>
{
    private static final Pattern<JoinNode> PATTERN = join();

    @Override
    public Pattern<JoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(JoinNode node, Captures captures, Context context)
    {
        if (canRemoveJoin(node, context.getLookup())) {
            return Result.ofPlanNode(new ValuesNode(node.getSourceLocation(), node.getId(), node.getOutputVariables(), ImmutableList.of(), Optional.empty()));
        }

        return Result.empty();
    }

    private boolean canRemoveJoin(JoinNode joinNode, Lookup lookup)
    {
        PlanNode left = joinNode.getLeft();
        PlanNode right = joinNode.getRight();
        switch (joinNode.getType()) {
            case INNER:
                return isEmpty(left, lookup) || isEmpty(right, lookup);
            case LEFT:
                return isEmpty(left, lookup);
            case RIGHT:
                return isEmpty(right, lookup);
            case FULL:
                return isEmpty(left, lookup) && isEmpty(right, lookup);
            default:
                throw new IllegalArgumentException();
        }
    }
}
