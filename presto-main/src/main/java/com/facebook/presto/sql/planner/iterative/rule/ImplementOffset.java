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
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.OffsetNode;
import com.facebook.presto.sql.planner.plan.RowNumberNode;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.common.collect.ImmutableList;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.isOffsetClauseEnabled;
import static com.facebook.presto.common.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.identityAssignments;
import static com.facebook.presto.sql.planner.plan.Patterns.offset;
import static com.facebook.presto.sql.relational.Expressions.comparisonExpression;
import static java.util.Objects.requireNonNull;

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

    private final StandardFunctionResolution functionResolution;

    public ImplementOffset(FunctionAndTypeManager functionAndTypeManager)
    {
        requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
        this.functionResolution = new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver());
    }

    @Override
    public Pattern<OffsetNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(OffsetNode parent, Captures captures, Context context)
    {
        if (!isOffsetClauseEnabled(context.getSession())) {
            throw new PrestoException(NOT_SUPPORTED, "Offset support is not enabled");
        }

        VariableReferenceExpression rowNumberSymbol = context.getVariableAllocator().newVariable("row_number", BIGINT);

        RowNumberNode rowNumberNode = new RowNumberNode(
                parent.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                parent.getSource(),
                ImmutableList.of(),
                rowNumberSymbol,
                Optional.empty(),
                false,
                Optional.empty());

        FilterNode filterNode = new FilterNode(
                parent.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                rowNumberNode,
                comparisonExpression(functionResolution, GREATER_THAN, rowNumberSymbol, new ConstantExpression(parent.getCount(), BIGINT)));

        ProjectNode projectNode = new ProjectNode(
                context.getIdAllocator().getNextId(),
                filterNode,
                identityAssignments(parent.getOutputVariables()));

        return Result.ofPlanNode(projectNode);
    }
}
