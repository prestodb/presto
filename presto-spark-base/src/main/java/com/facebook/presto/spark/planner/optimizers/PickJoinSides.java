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

package com.facebook.presto.spark.planner.optimizers;

import com.facebook.presto.Session;
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.JoinNode;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.isSizeBasedJoinDistributionTypeEnabled;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.isAdaptiveJoinSideSwitchingEnabled;
import static com.facebook.presto.sql.planner.iterative.rule.DetermineJoinDistributionType.isBelowBroadcastLimit;
import static com.facebook.presto.sql.planner.iterative.rule.DetermineJoinDistributionType.isSmallerThanThreshold;
import static com.facebook.presto.sql.planner.iterative.rule.JoinSwappingUtils.createRuntimeSwappedJoinNode;
import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.PARTITIONED;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.LEFT;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.RIGHT;
import static com.facebook.presto.sql.planner.plan.Patterns.join;
import static java.util.Objects.requireNonNull;

/**
 * This optimizer chooses the build and probe side of the join based on the size of the
 * join inputs, putting the smaller table on the right side/ build side of the join.
 * <p>
 * for example, consider a join with two children, nodeA and nodeB
 * if  nodeA is smaller than nodeB, we will convert
 * <pre>
 *     JOIN                     JOIN
 *    /    \        = >        /    \
 * nodeA   nodeB            nodeB   nodeA
 * </pre>
 */
public class PickJoinSides
        implements Rule<JoinNode>
{
    private static final Pattern<JoinNode> PATTERN = join().matching(joinNode ->
            joinNode.getDistributionType().isPresent()
                    && joinNode.getDistributionType().get() == PARTITIONED
                    // Flipping right/left non-equality joins is only supported when
                    // changing the distribution type too
                    && !(joinNode.getCriteria().isEmpty() && (joinNode.getType() == LEFT || joinNode.getType() == RIGHT)));

    private Metadata metadata;
    private SqlParser sqlParser;

    public PickJoinSides(Metadata metadata, SqlParser sqlParser)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
    }

    @Override
    public Pattern<JoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isAdaptiveJoinSideSwitchingEnabled(session);
    }

    @Override
    public Result apply(JoinNode joinNode, Captures captures, Context context)
    {
        StatsProvider statsProvider = context.getStatsProvider();
        double leftSize = statsProvider.getStats(joinNode.getLeft()).getOutputSizeInBytes();
        double rightSize = statsProvider.getStats(joinNode.getRight()).getOutputSizeInBytes();

        Optional<JoinNode> rewrittenNode = Optional.empty();
        // if we don't have exact costs for the join, but based on source tables we think the left side
        // is very small or much smaller than the right, then flip the join.
        if (rightSize > leftSize || (isSizeBasedJoinDistributionTypeEnabled(context.getSession()) && (Double.isNaN(leftSize) || Double.isNaN(rightSize)) && isLeftSideSmall(joinNode, context))) {
            rewrittenNode = createRuntimeSwappedJoinNode(joinNode, metadata, sqlParser, context.getLookup(), context.getSession(), context.getVariableAllocator(), context.getIdAllocator());
        }

        return rewrittenNode.map(Result::ofPlanNode).orElseGet(Result::empty);
    }

    // This logic is based on DetermineJoinDistributionType.getSizeBasedJoin(),
    // but does not include switching the join distribution type.
    private boolean isLeftSideSmall(JoinNode joinNode, Context context)
    {
        // the source table of the left side of the join is below the broadcast limit
        boolean isRightSideSmall = isBelowBroadcastLimit(joinNode.getRight(), context);
        boolean isLeftSideSmall = isBelowBroadcastLimit(joinNode.getLeft(), context);
        if (isLeftSideSmall && !isRightSideSmall) {
            return true;
        }

        // the last estimatable stats on the left are much smaller than those on the right.
        return isSmallerThanThreshold(joinNode.getLeft(), joinNode.getRight(), context);
    }
}
