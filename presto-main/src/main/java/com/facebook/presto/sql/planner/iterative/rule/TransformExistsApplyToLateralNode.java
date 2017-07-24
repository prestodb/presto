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
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.AggregationNode.Aggregation;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.LateralJoinNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.tree.Cast;
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
import static com.facebook.presto.sql.planner.plan.LateralJoinNode.Type.INNER;
import static com.facebook.presto.sql.planner.plan.Patterns.applyNode;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.GREATER_THAN;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

/**
 * Exists is modeled as:
 * <pre>
 *     - Project($0 > 0)
 *       - Aggregation(COUNT(*))
 *         - Limit(1)
 *           -- subquery
 * </pre>
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

        Symbol count = context.getSymbolAllocator().newSymbol(COUNT.toString(), BIGINT);
        Symbol exists = getOnlyElement(parent.getSubqueryAssignments().getSymbols());

        return Result.ofPlanNode(
                new LateralJoinNode(
                        parent.getId(),
                        parent.getInput(),
                        new ProjectNode(
                                context.getIdAllocator().getNextId(),
                                new AggregationNode(
                                        context.getIdAllocator().getNextId(),
                                        parent.getSubquery(),
                                        ImmutableMap.of(count, new Aggregation(COUNT_CALL, countSignature, Optional.empty(), ImmutableList.of(), ImmutableList.of())),
                                        ImmutableList.of(ImmutableList.of()),
                                        AggregationNode.Step.SINGLE,
                                        Optional.empty(),
                                        Optional.empty()),
                                Assignments.of(exists, new ComparisonExpression(GREATER_THAN, count.toSymbolReference(), new Cast(new LongLiteral("0"), BIGINT.toString())))),
                        parent.getCorrelation(),
                        INNER,
                        parent.getOriginSubquery()));
    }
}
