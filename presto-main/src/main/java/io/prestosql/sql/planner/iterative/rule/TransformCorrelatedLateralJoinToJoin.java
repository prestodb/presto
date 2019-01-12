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

import com.google.common.collect.ImmutableList;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.optimizations.PlanNodeDecorrelator;
import io.prestosql.sql.planner.optimizations.PlanNodeDecorrelator.DecorrelatedNode;
import io.prestosql.sql.planner.plan.JoinNode;
import io.prestosql.sql.planner.plan.LateralJoinNode;
import io.prestosql.sql.planner.plan.PlanNode;

import java.util.Optional;

import static io.prestosql.matching.Pattern.nonEmpty;
import static io.prestosql.sql.planner.plan.Patterns.LateralJoin.correlation;
import static io.prestosql.sql.planner.plan.Patterns.lateralJoin;

/**
 * Tries to decorrelate subquery and rewrite it using normal join.
 * Decorrelated predicates are part of join condition.
 */
public class TransformCorrelatedLateralJoinToJoin
        implements Rule<LateralJoinNode>
{
    private static final Pattern<LateralJoinNode> PATTERN = lateralJoin()
            .with(nonEmpty(correlation()));

    @Override
    public Pattern<LateralJoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(LateralJoinNode lateralJoinNode, Captures captures, Context context)
    {
        PlanNode subquery = lateralJoinNode.getSubquery();

        PlanNodeDecorrelator planNodeDecorrelator = new PlanNodeDecorrelator(context.getIdAllocator(), context.getLookup());
        Optional<DecorrelatedNode> decorrelatedNodeOptional = planNodeDecorrelator.decorrelateFilters(subquery, lateralJoinNode.getCorrelation());

        return decorrelatedNodeOptional.map(decorrelatedNode ->
                Result.ofPlanNode(new JoinNode(
                        context.getIdAllocator().getNextId(),
                        lateralJoinNode.getType().toJoinNodeType(),
                        lateralJoinNode.getInput(),
                        decorrelatedNode.getNode(),
                        ImmutableList.of(),
                        lateralJoinNode.getOutputSymbols(),
                        decorrelatedNode.getCorrelatedPredicates(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()))).orElseGet(Result::empty);
    }
}
