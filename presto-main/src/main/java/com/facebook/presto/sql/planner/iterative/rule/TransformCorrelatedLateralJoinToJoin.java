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
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.optimizations.PlanNodeDecorrelator;
import com.facebook.presto.sql.planner.optimizations.PlanNodeDecorrelator.DecorrelatedNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LateralJoinNode;
import com.facebook.presto.sql.relational.OriginalExpressionUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Optional;

import static com.facebook.presto.matching.Pattern.nonEmpty;
import static com.facebook.presto.sql.planner.plan.Patterns.LateralJoin.correlation;
import static com.facebook.presto.sql.planner.plan.Patterns.lateralJoin;

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

        PlanNodeDecorrelator planNodeDecorrelator = new PlanNodeDecorrelator(context.getIdAllocator(), context.getVariableAllocator(), context.getLookup());
        Optional<DecorrelatedNode> decorrelatedNodeOptional = planNodeDecorrelator.decorrelateFilters(subquery, lateralJoinNode.getCorrelation());

        return decorrelatedNodeOptional.map(decorrelatedNode ->
                Result.ofPlanNode(new JoinNode(
                        context.getIdAllocator().getNextId(),
                        lateralJoinNode.getType().toJoinNodeType(),
                        lateralJoinNode.getInput(),
                        decorrelatedNode.getNode(),
                        ImmutableList.of(),
                        lateralJoinNode.getOutputVariables(),
                        decorrelatedNode.getCorrelatedPredicates().map(OriginalExpressionUtils::castToRowExpression),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableMap.of()))).orElseGet(Result::empty);
    }
}
