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

import com.facebook.presto.Session;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.spi.plan.Ordering;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.MergeJoinNode;
import com.facebook.presto.sql.planner.plan.SortNode;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.preferSortMergeJoin;
import static com.facebook.presto.common.block.SortOrder.ASC_NULLS_FIRST;
import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.PARTITIONED;
import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.REPLICATED;
import static com.facebook.presto.sql.planner.plan.Patterns.join;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class ConvertHashJoinToSortMergeJoin
        implements Rule<JoinNode>
{
    private static final Pattern<JoinNode> PATTERN = join().matching(
            joinNode -> MergeJoinNode.isMergeJoinEligible(joinNode) &&
                    // it's not worth doing a sort merge join for a broadcast join which will have a small build side
                    !joinNode.getDistributionType().orElse(PARTITIONED).equals(REPLICATED));

    @Override
    public Pattern<JoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return preferSortMergeJoin(session);
    }

    @Override
    public Result apply(JoinNode node, Captures captures, Context context)
    {
        PlanNode left = node.getLeft();
        PlanNode right = node.getRight();

        List<Ordering> leftOrdering = node.getCriteria().stream()
                .map(criterion -> new Ordering(criterion.getLeft(), ASC_NULLS_FIRST))
                .collect(toImmutableList());
        PlanNode sortedLeft = new SortNode(
                Optional.empty(),
                context.getIdAllocator().getNextId(),
                left,
                new OrderingScheme(leftOrdering),
                true);

        List<Ordering> rightOrdering = node.getCriteria().stream()
                .map(criterion -> new Ordering(criterion.getRight(), ASC_NULLS_FIRST))
                .collect(toImmutableList());
        PlanNode sortedRight = new SortNode(
                Optional.empty(),
                context.getIdAllocator().getNextId(),
                right,
                new OrderingScheme(rightOrdering),
                true);

        return Result.ofPlanNode(
                new MergeJoinNode(
                        Optional.empty(),
                        node.getId(),
                        node.getType(),
                        sortedLeft,
                        sortedRight,
                        node.getCriteria(),
                        node.getOutputVariables(),
                        node.getFilter(),
                        node.getLeftHashVariable(),
                        node.getRightHashVariable()));
    }
}
