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
package io.prestosql.sql.planner.iterative.rule;

import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.sql.planner.iterative.Lookup;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.LateralJoinNode;
import io.prestosql.sql.planner.plan.PlanNode;

import static io.prestosql.sql.planner.optimizations.QueryCardinalityUtil.isScalar;
import static io.prestosql.sql.planner.plan.Patterns.lateralJoin;

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
    public Result apply(LateralJoinNode lateralJoinNode, Captures captures, Context context)
    {
        PlanNode input = lateralJoinNode.getInput();
        PlanNode subquery = lateralJoinNode.getSubquery();

        if (isUnreferencedScalar(input, context.getLookup())) {
            return Result.ofPlanNode(subquery);
        }

        if (isUnreferencedScalar(subquery, context.getLookup())) {
            return Result.ofPlanNode(input);
        }

        return Result.empty();
    }

    private boolean isUnreferencedScalar(PlanNode planNode, Lookup lookup)
    {
        return planNode.getOutputSymbols().isEmpty() && isScalar(planNode, lookup);
    }
}
