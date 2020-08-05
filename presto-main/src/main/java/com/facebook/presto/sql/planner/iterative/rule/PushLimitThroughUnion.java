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
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.UnionNode;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.google.common.collect.ImmutableList;

import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.sql.planner.optimizations.QueryCardinalityUtil.isAtMost;
import static com.facebook.presto.sql.planner.plan.Patterns.limit;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.facebook.presto.sql.planner.plan.Patterns.union;

/**
 * Transforms:
 * <pre>
 * - Limit
 *    - Union
 *       - relation1
 *       - relation2
 *       ..
 * </pre>
 * Into:
 * <pre>
 * - Limit
 *    - Union
 *       - Limit
 *          - relation1
 *       - Limit
 *          - relation2
 *       ..
 * </pre>
 * Applies to LimitNode without ties only to avoid optimizer loop.
 */
public class PushLimitThroughUnion
        implements Rule<LimitNode>
{
    private static final Capture<UnionNode> CHILD = newCapture();

    private static final Pattern<LimitNode> PATTERN = limit()
            .with(source().matching(union().capturedAs(CHILD)));

    @Override
    public Pattern<LimitNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(LimitNode parent, Captures captures, Context context)
    {
        UnionNode unionNode = captures.get(CHILD);
        if (unionNode.getSources().stream().allMatch(source -> isAtMost(source, context.getLookup(), parent.getCount()))) {
            return Result.empty();
        }
        ImmutableList.Builder<PlanNode> builder = ImmutableList.builder();
        for (PlanNode source : unionNode.getSources()) {
            // This check is to ensure that we don't fire the optimizer if it was previously applied.
            if (isAtMost(source, context.getLookup(), parent.getCount())) {
                builder.add(source);
            }
            else {
                builder.add(new LimitNode(context.getIdAllocator().getNextId(), source, parent.getCount(), LimitNode.Step.PARTIAL));
            }
        }

        return Result.ofPlanNode(
                parent.replaceChildren(ImmutableList.of(
                        unionNode.replaceChildren(builder.build()))));
    }
}
