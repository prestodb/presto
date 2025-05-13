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
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.PlannerUtils;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.UnnestNode;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.facebook.presto.SystemSessionProperties.isRewriteCrossJoinArrayNotContainsToAntiJoinEnabled;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.expressions.LogicalRowExpressions.and;
import static com.facebook.presto.expressions.LogicalRowExpressions.extractConjuncts;
import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IS_NULL;
import static com.facebook.presto.sql.planner.PlannerUtils.isSupportedArrayContainsFilter;
import static com.facebook.presto.sql.planner.PlannerUtils.restrictOutput;
import static com.facebook.presto.sql.planner.plan.Patterns.filter;
import static com.facebook.presto.sql.planner.plan.Patterns.join;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.facebook.presto.sql.planner.plan.Patterns.sources;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.specialForm;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * Inner join with contains function inside join clause will be run as cross join with filter.
 * When the join condition has pattern of contains(array, element), we can rewrite it to a inner join. For example:
 * <pre>
 * - Filter not contains(r_array, l_key)
 *      - Cross join
 *          - scan l
 *          - single_row_table_scan r
 * </pre>
 * into:
 * <pre>
 *     - Filter (l_array_elem IS NULL)
 *       - LOJ (l_key = l_array_elem)
 *          - scan l
 *          - Unnest
 *              l_array_elem <- unnest distinct_array
 *              - project
 *                  distinct_array := array_distinct(remove_nulls(r_array))
 *                  - single_row_table_scan r
 * </pre>
 */
public class CrossJoinWithArrayNotContainsToAntiJoin
        implements Rule<FilterNode>
{
    private static final Capture<JoinNode> JOIN = newCapture();

    private static final Capture<List<PlanNode>> JOIN_CHILDREN = Capture.newCapture();

    private static final Pattern<FilterNode> PATTERN = filter()
            .with(source().matching(join().matching(x -> x.isCrossJoin()).capturedAs(JOIN).with(sources().capturedAs(JOIN_CHILDREN))));

    Metadata metadata;
    private final FunctionAndTypeManager functionAndTypeManager;

    public CrossJoinWithArrayNotContainsToAntiJoin(Metadata metadata, FunctionAndTypeManager functionAndTypeManager)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
    }

    public static RowExpression getCandidateArrayNotContainsExpression(FunctionResolution functionResolution, RowExpression filterPredicate, List<VariableReferenceExpression> leftInput, List<VariableReferenceExpression> rightInput)
    {
        List<RowExpression> conjuncts = extractConjuncts(filterPredicate);
        for (RowExpression conjunct : conjuncts) {
            if (PlannerUtils.isNegationExpression(functionResolution, conjunct) &&
                    isSupportedArrayContainsFilter(functionResolution, conjunct.getChildren().get(0), leftInput, rightInput)) {
                return conjunct;
            }
        }
        return null;
    }

    @Override
    public Pattern<FilterNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isRewriteCrossJoinArrayNotContainsToAntiJoinEnabled(session);
    }

    @Override
    public Result apply(FilterNode node, Captures captures, Context context)
    {
        JoinNode joinNode = captures.get(JOIN);

        if (!(joinNode.getType().equals(JoinType.INNER) && joinNode.getCriteria().isEmpty())) {
            return Result.empty();
        }
        List<VariableReferenceExpression> leftColumns = joinNode.getLeft().getOutputVariables();
        List<VariableReferenceExpression> rightColumns = joinNode.getRight().getOutputVariables();
        RowExpression filterExpression = node.getPredicate();
        FunctionResolution functionResolution = new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver());

        RowExpression arrayNotContainsExpression = getCandidateArrayNotContainsExpression(functionResolution, filterExpression, leftColumns, rightColumns);
        if (arrayNotContainsExpression == null) {
            return Result.empty();
        }
        List<RowExpression> allConjuncts = extractConjuncts(filterExpression);
        List<RowExpression> remainingConjuncts = allConjuncts.stream().filter(x -> !x.equals(arrayNotContainsExpression)).collect(Collectors.toList());

        RowExpression arrayContainsExpression = arrayNotContainsExpression.getChildren().get(0);
        RowExpression array = ((CallExpression) arrayContainsExpression).getArguments().get(0);
        RowExpression element = ((CallExpression) arrayContainsExpression).getArguments().get(1);

        checkState(element instanceof VariableReferenceExpression, "Argument to CONTAINS is not a column");
        checkState(array instanceof VariableReferenceExpression, "Argument to CONTAINS is not a column");

        boolean arrayAtLeftInput = leftColumns.contains(array);
        PlanNode inputWithArray = arrayAtLeftInput ? joinNode.getLeft() : joinNode.getRight();

        if (!isFromScalarSubquery(context, inputWithArray)) {
            // rewrite is incorrect if array input has more than 1 row or columns included in the output: if the source of CROSS JOIN was a subquery these conditions are guaranteed
            return Result.empty();
        }
        final Type type = element.getType();
        CallExpression arrayDistinct = call(functionAndTypeManager, "array_distinct", new ArrayType(type),
                call(functionAndTypeManager, "remove_nulls", new ArrayType(type), array));
        VariableReferenceExpression arrayDistinctVariable = context.getVariableAllocator().newVariable(arrayDistinct);
        PlanNode project = PlannerUtils.addProjections(inputWithArray, context.getIdAllocator(), ImmutableMap.of(arrayDistinctVariable, arrayDistinct));
        VariableReferenceExpression unnestVariable = context.getVariableAllocator().newVariable("field", type);
        UnnestNode unnest = new UnnestNode(inputWithArray.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                project,
                project.getOutputVariables(),
                ImmutableMap.of(arrayDistinctVariable, ImmutableList.of(unnestVariable)),
                Optional.empty());

        PlanNode newLeftNode;

        if (arrayAtLeftInput) {
            newLeftNode = joinNode.getRight();
        }
        else {
            newLeftNode = joinNode.getLeft();
        }

        // if element is not a VariableReferenceExpression, push the expression into a Project node so the variable can be used in equijoins
        checkState(element instanceof VariableReferenceExpression, "Argument to CONTAINS is not a column");

        EquiJoinClause equiJoinClause = new EquiJoinClause((VariableReferenceExpression) element, unnestVariable);

        List<VariableReferenceExpression> newOutputColumns = Stream.concat(newLeftNode.getOutputVariables().stream(), unnest.getOutputVariables().stream()).collect(toImmutableList());

        JoinNode newJoinNode = new JoinNode(joinNode.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                JoinType.LEFT,
                newLeftNode,
                unnest,
                ImmutableList.of(equiJoinClause),
                newOutputColumns,
                joinNode.getFilter(),
                Optional.empty(),
                Optional.empty(),
                joinNode.getDistributionType(),
                joinNode.getDynamicFilters());

        RowExpression isNull = specialForm(IS_NULL, BOOLEAN, ImmutableList.of(unnestVariable));
        remainingConjuncts.add(isNull);
        FilterNode filterNode = new FilterNode(node.getSourceLocation(), context.getIdAllocator().getNextId(), newJoinNode, and(remainingConjuncts));
        PlanNode result = restrictOutput(filterNode, context.getIdAllocator(), joinNode.getOutputVariables());
        return Result.ofPlanNode(result);
    }

    private boolean isFromScalarSubquery(Context context, PlanNode node)
    {
        // TODO: currently we only support EnforceSingleRow which guarantees the filter+cross join was generated from a subquery (so no other columns needed from the array side of the cross join)
        PlanNode extractedNode = context.getLookup().resolve(node);
        return extractedNode instanceof EnforceSingleRowNode ||
                (extractedNode instanceof ProjectNode && isFromScalarSubquery(context, extractedNode.getSources().get(0)));
    }
}
