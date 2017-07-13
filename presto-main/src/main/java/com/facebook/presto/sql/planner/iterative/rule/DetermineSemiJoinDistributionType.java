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

import com.facebook.presto.sql.analyzer.FeaturesConfig.JoinDistributionType;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.DeleteNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.getJoinDistributionType;
import static com.facebook.presto.sql.planner.plan.SemiJoinNode.DistributionType.PARTITIONED;
import static com.facebook.presto.sql.planner.plan.SemiJoinNode.DistributionType.REPLICATED;

public class DetermineSemiJoinDistributionType
        implements Rule
{
    private boolean isDeleteQuery;

    @Override
    public Optional<PlanNode> apply(PlanNode node, Context context)
    {
        if (node instanceof DeleteNode) {
            isDeleteQuery = true;
            return Optional.empty();
        }
        if (!(node instanceof SemiJoinNode)) {
            return Optional.empty();
        }

        SemiJoinNode semiJoinNode = (SemiJoinNode) node;

        if (semiJoinNode.getDistributionType().isPresent()) {
            return Optional.empty();
        }

        JoinDistributionType joinDistributionType = getJoinDistributionType(context.getSession());

        if (joinDistributionType.canRepartition() && !isDeleteQuery) {
            return Optional.of(semiJoinNode.withDistributionType(PARTITIONED));
        }
        return Optional.of(semiJoinNode.withDistributionType(REPLICATED));
    }
}
