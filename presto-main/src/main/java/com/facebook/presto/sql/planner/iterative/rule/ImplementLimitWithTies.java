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

import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.FrameBound;
import com.facebook.presto.sql.tree.GenericLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.SymbolReference;
import com.facebook.presto.sql.tree.WindowFrame;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.metadata.FunctionAndTypeManager.qualifyObjectName;
import static com.facebook.presto.sql.planner.optimizations.WindowNodeUtil.toBoundType;
import static com.facebook.presto.sql.planner.optimizations.WindowNodeUtil.toWindowType;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.identityAssignmentsAsSymbolReferences;
import static com.facebook.presto.sql.planner.plan.Patterns.limit;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToRowExpression;
import static java.util.Objects.requireNonNull;

/**
 * Transforms:
 * <pre>
 * - Limit (row count = x, tiesResolvingScheme(a,b,c))
 *    - source
 * </pre>
 * Into:
 * <pre>
 * - Project (prune rank symbol)
 *    - Filter (rank <= x)
 *       - Window (function: rank, order by a,b,c)
 *          - source
 * </pre>
 */
public class ImplementLimitWithTies
        implements Rule<LimitNode>
{
    private static final Capture<PlanNode> CHILD = newCapture();
    private static final Pattern<LimitNode> PATTERN = limit()
            .matching(LimitNode::isWithTies)
            .with(source().capturedAs(CHILD));

    private final Metadata metadata;

    public ImplementLimitWithTies(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public Pattern<LimitNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(LimitNode parent, Captures captures, Context context)
    {
        PlanNode child = captures.get(CHILD);
        VariableReferenceExpression rankSymbol = context.getVariableAllocator().newVariable("rank_num", BIGINT);

        FunctionHandle function = metadata.getFunctionAndTypeManager().resolveFunction(
                Optional.of(context.getSession().getSessionFunctions()),
                Optional.empty(),
                qualifyObjectName(QualifiedName.of("rank")),
                ImmutableList.of());

        WindowNode.Frame frame = new WindowNode.Frame(
                toWindowType(WindowFrame.Type.RANGE),
                toBoundType(FrameBound.Type.UNBOUNDED_PRECEDING),
                Optional.empty(),
                toBoundType(FrameBound.Type.CURRENT_ROW),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        WindowNode.Function rankFunction = new WindowNode.Function(
                call("rank", function, BIGINT, ImmutableList.of()),
                frame,
                false);

        WindowNode windowNode = new WindowNode(
                context.getIdAllocator().getNextId(),
                child,
                new WindowNode.Specification(ImmutableList.of(), parent.getTiesResolvingScheme()),
                ImmutableMap.of(rankSymbol, rankFunction),
                Optional.empty(),
                ImmutableSet.of(),
                0);

        FilterNode filterNode = new FilterNode(
                context.getIdAllocator().getNextId(),
                windowNode,
                castToRowExpression(new ComparisonExpression(
                        ComparisonExpression.Operator.LESS_THAN_OR_EQUAL,
                        new SymbolReference(rankSymbol.getName()),
                        new GenericLiteral("BIGINT", Long.toString(parent.getCount())))));

        ProjectNode projectNode = new ProjectNode(
                context.getIdAllocator().getNextId(),
                filterNode,
                identityAssignmentsAsSymbolReferences(parent.getOutputVariables()));

        return Rule.Result.ofPlanNode(projectNode);
    }
}
