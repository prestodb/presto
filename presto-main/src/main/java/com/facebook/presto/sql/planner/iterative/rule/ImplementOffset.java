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
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.OffsetNode;
import com.facebook.presto.sql.planner.plan.RowNumberNode;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.GenericLiteral;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.isOffsetClauseEnabled;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.identityAssignmentsAsSymbolReferences;
import static com.facebook.presto.sql.planner.plan.Patterns.offset;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToRowExpression;

/**
 * Transforms:
 * <pre>
 * - Offset (row count = x)
 *    - Source
 * </pre>
 * Into:
 * <pre>
 * - Project (prune rowNumber symbol)
 *    - Filter (rowNumber > x)
 *       - RowNumber
 *          - Source
 * </pre>
 * Relies on RowNumberNode's property of keeping order of its input.
 * If the query contains an ORDER BY clause, the sorted order
 * will be respected when leading rows are removed.
 */
public class ImplementOffset
        implements Rule<OffsetNode>
{
    private static final Pattern<OffsetNode> PATTERN = offset();

    @Override
    public Pattern<OffsetNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isOffsetClauseEnabled(session);
    }

    @Override
    public Result apply(OffsetNode parent, Captures captures, Context context)
    {
        VariableReferenceExpression rowNumberSymbol = context.getVariableAllocator().newVariable("row_number", BIGINT);

        RowNumberNode rowNumberNode = new RowNumberNode(
                context.getIdAllocator().getNextId(),
                parent.getSource(),
                ImmutableList.of(),
                rowNumberSymbol,
                Optional.empty(),
                Optional.empty());

        FilterNode filterNode = new FilterNode(
                context.getIdAllocator().getNextId(),
                rowNumberNode,
                castToRowExpression(new ComparisonExpression(
                        ComparisonExpression.Operator.GREATER_THAN,
                        new SymbolReference(rowNumberSymbol.getName()),
                        new GenericLiteral("BIGINT", Long.toString(parent.getCount())))));

        ProjectNode projectNode = new ProjectNode(
                context.getIdAllocator().getNextId(),
                filterNode,
                identityAssignmentsAsSymbolReferences(parent.getOutputVariables()));

        return Result.ofPlanNode(projectNode);
    }
}
