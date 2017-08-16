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
import com.facebook.presto.sql.planner.iterative.GroupTraitSet;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.iterative.trait.CardinalityGroupTrait;
import com.facebook.presto.sql.planner.plan.LateralJoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;

import java.util.Optional;

import static com.facebook.presto.sql.planner.iterative.GroupTrait.Type.CARDINALITY;
import static com.facebook.presto.sql.planner.plan.Patterns.lateralJoin;
import static java.util.Optional.empty;

public class RemoveUnreferencedScalarLateralNodes
        implements Rule<LateralJoinNode>
{
    private static final Pattern<LateralJoinNode> PATTERN = lateralJoin();

    @Override
    public Pattern<LateralJoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<PlanNode> apply(LateralJoinNode lateralJoinNode, Captures captures, Context context)
    {
        PlanNode input = lateralJoinNode.getInput();
        PlanNode subquery = lateralJoinNode.getSubquery();

        if (isUnreferencedScalar(input, context.getLookup())) {
            return Optional.of(subquery);
        }

        if (isUnreferencedScalar(subquery, context.getLookup())) {
            return Optional.of(input);
        }

        return empty();
    }

    private boolean isUnreferencedScalar(PlanNode planNode, Lookup lookup)
    {
        return planNode.getOutputSymbols().isEmpty() && isScalar(planNode, lookup);
    }

    private boolean isScalar(PlanNode planNode, Lookup lookup)
    {
        GroupTraitSet groupTraitSet = lookup.resolveGroupTraitSet(planNode);
        CardinalityGroupTrait cardinalityGroupTrait = (CardinalityGroupTrait) groupTraitSet.getTrait(CARDINALITY);
        return cardinalityGroupTrait.isScalar();
    }
}
