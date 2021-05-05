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
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.sql.planner.iterative.Rule;

import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.sql.planner.plan.Patterns.limit;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.facebook.presto.sql.planner.plan.Patterns.topN;

/**
 * This rule handles both LimitNode with ties and LimitNode without ties.
 * In the case of LimitNode with ties, the LimitNode is removed
 * if only its row count equals or exceeds TopNNode's row count:
 * <pre>
 *     - Limit (5, with ties)
 *        - TopN (3)
 * </pre>
 * is transformed into:
 * <pre>
 *     - TopN (3)
 * </pre>
 * In the case of LimitNode without ties, the LimitNode is removed,
 * and the TopNNode's row count is updated to minimum row count
 * of both nodes:
 * <pre>
 *     - Limit (3)
 *        - TopN (5)
 * </pre>
 * is transformed into:
 * <pre>
 *     - TopN (3)
 * </pre>
 */
public class MergeLimitWithTopN
        implements Rule<LimitNode>
{
    private static final Capture<TopNNode> CHILD = newCapture();

    private static final Pattern<LimitNode> PATTERN = limit()
            .with(source().matching(topN().capturedAs(CHILD)));

    @Override
    public Pattern<LimitNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(LimitNode parent, Captures captures, Context context)
    {
        TopNNode child = captures.get(CHILD);

        if (parent.isWithTies()) {
            if (parent.getCount() < child.getCount()) {
                return Result.empty();
            }
            else {
                return Result.ofPlanNode(child);
            }
        }

        return Result.ofPlanNode(
                new TopNNode(
                        parent.getId(),
                        child.getSource(),
                        Math.min(parent.getCount(), child.getCount()),
                        child.getOrderingScheme(),
                        parent.isPartial() ? TopNNode.Step.PARTIAL : TopNNode.Step.SINGLE));
    }
}
