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
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.JoinNode;

import java.util.Collections;

import static com.facebook.presto.SystemSessionProperties.isEmptyJoinOptimization;
import static com.facebook.presto.sql.planner.optimizations.QueryCardinalityUtil.isAtMost;
import static com.facebook.presto.sql.planner.plan.Patterns.join;

public class EliminateEmptyJoins
        implements Rule<JoinNode>
{
    private static final Pattern<JoinNode> PATTERN = join();

    @Override
    public Pattern<JoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(JoinNode joinNode, Captures captures, Context context)
    {
        if (!isEmptyJoinOptimization(context.getSession())) {
            return Result.empty();
        }

        boolean leftChildEmpty;
        boolean rightChildEmpty;
        leftChildEmpty = isAtMost(context.getLookup().resolve(joinNode.getLeft()), context.getLookup(), 0);
        rightChildEmpty = isAtMost(context.getLookup().resolve(joinNode.getRight()), context.getLookup(), 0);

        if ((!leftChildEmpty && !rightChildEmpty) || joinNode.getType() != JoinNode.Type.INNER) {
            return Result.empty();
        }

        return Result.ofPlanNode(
                new ValuesNode(joinNode.getId(), joinNode.getOutputVariables(), Collections.emptyList()));
    }
}
