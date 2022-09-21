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
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.GroupReference;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.AssignUniqueId;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.JoinNode.EquiJoinClause;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.getJoinReorderingStrategy;
import static com.facebook.presto.SystemSessionProperties.isExploitConstraints;
import static com.facebook.presto.SystemSessionProperties.isInPredicatesAsInnerJoinsEnabled;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.matching.Pattern.empty;
import static com.facebook.presto.spi.plan.AggregationNode.Step.SINGLE;
import static com.facebook.presto.spi.plan.AggregationNode.singleGroupingSet;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinReorderingStrategy.AUTOMATIC;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.identitiesAsSymbolReferences;
import static com.facebook.presto.sql.planner.plan.Patterns.Apply.correlation;
import static com.facebook.presto.sql.planner.plan.Patterns.applyNode;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToExpression;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToRowExpression;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;

/**
 * This optimizer looks for InPredicate expressions in ApplyNodes and replaces the nodes with Distinct + Inner Joins.
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
 * Aggregate (Distinct unique, a):
 *   InnerJoin (a=b)
 *     -source AssignUniqueId (plan A) -> producing uniqueId, a
 *     - plan B producing symbol b
 * </pre>
 */
public class TransformUncorrelatedInPredicateSubqueryToDistinctInnerJoin
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
    public boolean isEnabled(Session session)
    {
        return isExploitConstraints(session) &&
                getJoinReorderingStrategy(session) == AUTOMATIC &&
                isInPredicatesAsInnerJoinsEnabled(session);
    }

    @Override
    public Result apply(ApplyNode applyNode, Captures captures, Context context)
    {
        if (applyNode.getMayParticipateInAntiJoin()) {
            return Result.empty();
        }

        Assignments subqueryAssignments = applyNode.getSubqueryAssignments();
        if (subqueryAssignments.size() != 1) {
            return Result.empty();
        }

        Expression expression = castToExpression(getOnlyElement(subqueryAssignments.getExpressions()));
        if (!(expression instanceof InPredicate)) {
            return Result.empty();
        }

        InPredicate inPredicate = (InPredicate) expression;
        VariableReferenceExpression inPredicateOutputVariable = getOnlyElement(subqueryAssignments.getVariables());

        PlanNode leftInput = applyNode.getInput();
        // Add unique id column if the set of columns do not form a unique key already
        if (!((GroupReference) leftInput).getLogicalProperties().isPresent() ||
                !((GroupReference) leftInput).getLogicalProperties().get().isDistinct(ImmutableSet.copyOf(leftInput.getOutputVariables()))) {
            VariableReferenceExpression uniqueKeyVariable = context.getVariableAllocator().newVariable("unique", BIGINT);
            leftInput = new AssignUniqueId(
                    applyNode.getSourceLocation(),
                    context.getIdAllocator().getNextId(),
                    leftInput,
                    uniqueKeyVariable);
        }

        checkArgument(inPredicate.getValue() instanceof SymbolReference, "Unexpected expression: %s", inPredicate.getValue());
        VariableReferenceExpression leftVariableReference = context.getVariableAllocator().toVariableReference(inPredicate.getValue());
        checkArgument(inPredicate.getValueList() instanceof SymbolReference, "Unexpected expression: %s", inPredicate.getValueList());
        VariableReferenceExpression rightVariableReference = context.getVariableAllocator().toVariableReference(inPredicate.getValueList());

        JoinNode innerJoin = new JoinNode(
                applyNode.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                JoinNode.Type.INNER,
                leftInput,
                applyNode.getSubquery(),
                ImmutableList.of(new EquiJoinClause(
                        leftVariableReference,
                        rightVariableReference)),
                ImmutableList.<VariableReferenceExpression>builder()
                        .addAll(leftInput.getOutputVariables())
                        .build(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of());

        AggregationNode distinctNode = new AggregationNode(
                innerJoin.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                innerJoin,
                ImmutableMap.of(),
                singleGroupingSet(ImmutableList.<VariableReferenceExpression>builder()
                        .addAll(innerJoin.getOutputVariables())
                        .build()),
                ImmutableList.of(),
                SINGLE,
                Optional.empty(),
                Optional.empty());

        ImmutableList<VariableReferenceExpression> referencedOutputs = ImmutableList.<VariableReferenceExpression>builder()
                .addAll(applyNode.getInput().getOutputVariables())
                .add(inPredicateOutputVariable)
                .build();

        ProjectNode finalProjectNdde = new ProjectNode(
                context.getIdAllocator().getNextId(),
                distinctNode,
                Assignments.builder()
                        .putAll(identitiesAsSymbolReferences(distinctNode.getOutputVariables()))
                        .put(inPredicateOutputVariable, castToRowExpression(TRUE_LITERAL))
                        .build()
                        .filter(referencedOutputs));

        return Result.ofPlanNode(finalProjectNdde);
    }
}
