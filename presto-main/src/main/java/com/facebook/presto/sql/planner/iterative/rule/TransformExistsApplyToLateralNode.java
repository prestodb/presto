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
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.optimizations.PlanNodeDecorrelator;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.AggregationNode.Aggregation;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.LateralJoinNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.CoalesceExpression;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.ExistsPredicate;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Optional;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.sql.planner.plan.LateralJoinNode.Type.INNER;
import static com.facebook.presto.sql.planner.plan.LateralJoinNode.Type.LEFT;
import static com.facebook.presto.sql.planner.plan.Patterns.applyNode;
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

    private static final QualifiedName COUNT = QualifiedName.of("count");
    private static final FunctionCall COUNT_CALL = new FunctionCall(COUNT, ImmutableList.of());
    private final Signature countSignature;

    public TransformExistsApplyToLateralNode(FunctionRegistry functionRegistry)
    {
        requireNonNull(functionRegistry, "functionRegistry is null");
        countSignature = functionRegistry.resolveFunction(COUNT, ImmutableList.of());
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

        Expression expression = getOnlyElement(parent.getSubqueryAssignments().getExpressions());
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
        checkState(applyNode.getSubquery().getOutputSymbols().isEmpty(), "Expected subquery output symbols to be pruned");

        Symbol exists = getOnlyElement(applyNode.getSubqueryAssignments().getSymbols());
        Symbol subqueryTrue = context.getSymbolAllocator().newSymbol("subqueryTrue", BOOLEAN);

        Assignments.Builder assignments = Assignments.builder();
        assignments.putIdentities(applyNode.getInput().getOutputSymbols());
        assignments.put(exists, new CoalesceExpression(ImmutableList.of(subqueryTrue.toSymbolReference(), BooleanLiteral.FALSE_LITERAL)));

        PlanNode subquery = new ProjectNode(
                context.getIdAllocator().getNextId(),
                new LimitNode(
                        context.getIdAllocator().getNextId(),
                        applyNode.getSubquery(),
                        1L,
                        false),
                Assignments.of(subqueryTrue, TRUE_LITERAL));

        PlanNodeDecorrelator decorrelator = new PlanNodeDecorrelator(context.getIdAllocator(), context.getLookup());
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
                        applyNode.getOriginSubquery()),
                assignments.build()));
    }

    private PlanNode rewriteToDefaultAggregation(ApplyNode parent, Context context)
    {
        Symbol count = context.getSymbolAllocator().newSymbol(COUNT.toString(), BIGINT);
        Symbol exists = getOnlyElement(parent.getSubqueryAssignments().getSymbols());

        return new LateralJoinNode(
                parent.getId(),
                parent.getInput(),
                new ProjectNode(
                        context.getIdAllocator().getNextId(),
                        new AggregationNode(
                                context.getIdAllocator().getNextId(),
                                parent.getSubquery(),
                                ImmutableMap.of(count, new Aggregation(COUNT_CALL, countSignature, Optional.empty())),
                                ImmutableList.of(ImmutableList.of()),
                                ImmutableList.of(),
                                AggregationNode.Step.SINGLE,
                                Optional.empty(),
                                Optional.empty()),
                        Assignments.of(exists, new ComparisonExpression(GREATER_THAN, count.toSymbolReference(), new Cast(new LongLiteral("0"), BIGINT.toString())))),
                parent.getCorrelation(),
                INNER,
                parent.getOriginSubquery());
    }
}
