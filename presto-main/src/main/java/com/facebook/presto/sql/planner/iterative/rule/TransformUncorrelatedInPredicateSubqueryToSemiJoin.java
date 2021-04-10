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
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.InPredicate;
import com.google.common.collect.ImmutableMap;

import java.util.Optional;

import static com.facebook.presto.matching.Pattern.empty;
import static com.facebook.presto.sql.planner.plan.Patterns.Apply.correlation;
import static com.facebook.presto.sql.planner.plan.Patterns.applyNode;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToExpression;
import static com.google.common.collect.Iterables.getOnlyElement;

/**
 * This optimizers looks for InPredicate expressions in ApplyNodes and replaces the nodes with SemiJoin nodes.
 * <p/>
 * Plan before optimizer:
 * <pre>
 * Filter(a IN b):
 *   Apply
 *     - correlation: []  // empty
 *     - input: some plan A producing symbol a
 *     - subquery: some plan B producing symbol b
 * </pre>
 * <p/>
 * Plan after optimizer:
 * <pre>
 * Filter(semijoinresult):
 *   SemiJoin
 *     - source: plan A
 *     - filteringSource: symbol a
 *     - sourceJoinSymbol: plan B
 *     - filteringSourceJoinSymbol: symbol b
 *     - semiJoinOutput: semijoinresult
 * </pre>
 */
public class TransformUncorrelatedInPredicateSubqueryToSemiJoin
        implements Rule<ApplyNode>
{
    private static final Pattern<ApplyNode> PATTERN = applyNode()
            .with(empty(correlation()));

    @Override
    public Pattern<ApplyNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(ApplyNode applyNode, Captures captures, Context context)
    {
        if (applyNode.getSubqueryAssignments().size() != 1) {
            return Result.empty();
        }

        Expression expression = castToExpression(getOnlyElement(applyNode.getSubqueryAssignments().getExpressions()));
        if (!(expression instanceof InPredicate)) {
            return Result.empty();
        }

        InPredicate inPredicate = (InPredicate) expression;
        VariableReferenceExpression semiJoinVariable = getOnlyElement(applyNode.getSubqueryAssignments().getVariables());

        SemiJoinNode replacement = new SemiJoinNode(context.getIdAllocator().getNextId(),
                applyNode.getInput(),
                applyNode.getSubquery(),
                context.getVariableAllocator().toVariableReference(inPredicate.getValue()),
                context.getVariableAllocator().toVariableReference(inPredicate.getValueList()),
                semiJoinVariable,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of());

        return Result.ofPlanNode(replacement);
    }
}
