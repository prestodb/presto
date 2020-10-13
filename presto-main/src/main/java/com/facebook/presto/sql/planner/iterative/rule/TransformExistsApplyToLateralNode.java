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
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.AggregationNode.Aggregation;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.optimizations.PlanNodeDecorrelator;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.LateralJoinNode;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.CoalesceExpression;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.ExistsPredicate;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.plan.AggregationNode.globalAggregation;
import static com.facebook.presto.spi.plan.LimitNode.Step.FINAL;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.identitiesAsSymbolReferences;
import static com.facebook.presto.sql.planner.plan.LateralJoinNode.Type.INNER;
import static com.facebook.presto.sql.planner.plan.LateralJoinNode.Type.LEFT;
import static com.facebook.presto.sql.planner.plan.Patterns.applyNode;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.asSymbolReference;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToExpression;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToRowExpression;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.GREATER_THAN;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

/**
 * EXISTS is modeled as (if correlated predicates are equality comparisons):
 * <pre>
 *     - Project(exists := COALESCE(subqueryTrue, false))
 *       - LateralJoin(LEFT)
 *         - input
 *         - Project(subqueryTrue := true)
 *           - Limit(count=1)
 *             - subquery
 * </pre>
 * or:
 * <pre>
 *     - LateralJoin(LEFT)
 *       - input
 *       - Project($0 > 0)
 *         - Aggregation(COUNT(*))
 *           - subquery
 * </pre>
 * otherwise
 */
public class TransformExistsApplyToLateralNode
        implements Rule<ApplyNode>
{
    private static final Pattern<ApplyNode> PATTERN = applyNode();

    private final StandardFunctionResolution functionResolution;

    public TransformExistsApplyToLateralNode(FunctionAndTypeManager functionAndTypeManager)
    {
        requireNonNull(functionAndTypeManager, "functionManager is null");
        this.functionResolution = new FunctionResolution(functionAndTypeManager);
    }

    @Override
    public Pattern<ApplyNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(ApplyNode parent, Captures captures, Context context)
    {
        if (parent.getSubqueryAssignments().size() != 1) {
            return Result.empty();
        }

        Expression expression = castToExpression(getOnlyElement(parent.getSubqueryAssignments().getExpressions()));
        if (!(expression instanceof ExistsPredicate)) {
            return Result.empty();
        }

        Optional<PlanNode> nonDefaultAggregation = rewriteToNonDefaultAggregation(parent, context);
        return nonDefaultAggregation
                .map(Result::ofPlanNode)
                .orElseGet(() -> Result.ofPlanNode(rewriteToDefaultAggregation(parent, context)));
    }

    private Optional<PlanNode> rewriteToNonDefaultAggregation(ApplyNode applyNode, Context context)
    {
        checkState(applyNode.getSubquery().getOutputVariables().isEmpty(), "Expected subquery output variables to be pruned");

        VariableReferenceExpression exists = getOnlyElement(applyNode.getSubqueryAssignments().getVariables());
        VariableReferenceExpression subqueryTrue = context.getVariableAllocator().newVariable("subqueryTrue", BOOLEAN);

        Assignments.Builder assignments = Assignments.builder();
        assignments.putAll(identitiesAsSymbolReferences(applyNode.getInput().getOutputVariables()));
        assignments.put(exists, castToRowExpression(new CoalesceExpression(ImmutableList.of(new SymbolReference(subqueryTrue.getName()), BooleanLiteral.FALSE_LITERAL))));

        PlanNode subquery = new ProjectNode(
                context.getIdAllocator().getNextId(),
                new LimitNode(
                        context.getIdAllocator().getNextId(),
                        applyNode.getSubquery(),
                        1L,
                        FINAL),
                Assignments.of(subqueryTrue, castToRowExpression(TRUE_LITERAL)));

        PlanNodeDecorrelator decorrelator = new PlanNodeDecorrelator(context.getIdAllocator(), context.getVariableAllocator(), context.getLookup());
        if (!decorrelator.decorrelateFilters(subquery, applyNode.getCorrelation()).isPresent()) {
            return Optional.empty();
        }

        return Optional.of(new ProjectNode(context.getIdAllocator().getNextId(),
                new LateralJoinNode(
                        applyNode.getId(),
                        applyNode.getInput(),
                        subquery,
                        applyNode.getCorrelation(),
                        LEFT,
                        applyNode.getOriginSubqueryError()),
                assignments.build()));
    }

    private PlanNode rewriteToDefaultAggregation(ApplyNode parent, Context context)
    {
        VariableReferenceExpression count = context.getVariableAllocator().newVariable("count", BIGINT);
        VariableReferenceExpression exists = getOnlyElement(parent.getSubqueryAssignments().getVariables());

        return new LateralJoinNode(
                parent.getId(),
                parent.getInput(),
                new ProjectNode(
                        context.getIdAllocator().getNextId(),
                        new AggregationNode(
                                context.getIdAllocator().getNextId(),
                                parent.getSubquery(),
                                ImmutableMap.of(count, new Aggregation(
                                        new CallExpression(
                                                "count",
                                                functionResolution.countFunction(),
                                                BIGINT,
                                                ImmutableList.of()),
                                        Optional.empty(),
                                        Optional.empty(),
                                        false,
                                        Optional.empty())),
                                globalAggregation(),
                                ImmutableList.of(),
                                AggregationNode.Step.SINGLE,
                                Optional.empty(),
                                Optional.empty()),
                        Assignments.of(exists, castToRowExpression(new ComparisonExpression(GREATER_THAN, asSymbolReference(count), new Cast(new LongLiteral("0"), BIGINT.toString()))))),
                parent.getCorrelation(),
                INNER,
                parent.getOriginSubqueryError());
    }
}
