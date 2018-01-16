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
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.JoinNode.DistributionType;

import static com.facebook.presto.SystemSessionProperties.isDistributedJoinEnabled;
import static com.facebook.presto.sql.planner.optimizations.QueryCardinalityUtil.isAtMostScalar;
import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.PARTITIONED;
import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.REPLICATED;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.FULL;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.LEFT;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.RIGHT;
import static com.facebook.presto.sql.planner.plan.Patterns.join;

public class DetermineJoinDistributionType
        implements Rule<JoinNode>
{
    private static final Pattern<JoinNode> PATTERN = join().matching(joinNode -> !joinNode.getDistributionType().isPresent());

    @Override
    public Pattern<JoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(JoinNode node, Captures captures, Context context)
    {
        DistributionType distributionType = determineDistributionType(node, context);
        return Result.ofPlanNode(node.withDistributionType(distributionType));
    }

    private static DistributionType determineDistributionType(JoinNode node, Context context)
    {
        JoinNode.Type type = node.getType();
        if (type == RIGHT || type == FULL) {
            // With REPLICATED, the unmatched rows from right-side would be duplicated.
            return PARTITIONED;
        }

        if (node.getCriteria().isEmpty() && (type == INNER || type == LEFT)) {
            // There is nothing to partition on
            return REPLICATED;
        }

        if (isAtMostScalar(node.getRight(), context.getLookup())) {
            return REPLICATED;
        }

        if (isDistributedJoinEnabled(context.getSession())) {
            return PARTITIONED;
        }

        return REPLICATED;
    }
}
