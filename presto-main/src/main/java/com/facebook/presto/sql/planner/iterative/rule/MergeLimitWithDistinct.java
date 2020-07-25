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
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.DistinctLimitNode;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.sql.planner.iterative.Rule;

import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.sql.planner.plan.Patterns.aggregation;
import static com.facebook.presto.sql.planner.plan.Patterns.limit;
import static com.facebook.presto.sql.planner.plan.Patterns.source;

public class MergeLimitWithDistinct
        implements Rule<LimitNode>
{
    private static final Capture<AggregationNode> CHILD = newCapture();

    private static final Pattern<LimitNode> PATTERN = limit()
            .with(source().matching(aggregation().capturedAs(CHILD)
                    .matching(MergeLimitWithDistinct::isDistinct)));

    /**
     * Whether this node corresponds to a DISTINCT operation in SQL
     */
    private static boolean isDistinct(AggregationNode node)
    {
        return node.getAggregations().isEmpty() &&
                node.getOutputVariables().size() == node.getGroupingKeys().size() &&
                node.getOutputVariables().containsAll(node.getGroupingKeys());
    }

    @Override
    public Pattern<LimitNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(LimitNode parent, Captures captures, Context context)
    {
        AggregationNode child = captures.get(CHILD);

        return Result.ofPlanNode(
                new DistinctLimitNode(
                        parent.getId(),
                        child.getSource(),
                        parent.getCount(),
                        false,
                        child.getGroupingKeys(),
                        child.getHashVariable()));
    }
}
