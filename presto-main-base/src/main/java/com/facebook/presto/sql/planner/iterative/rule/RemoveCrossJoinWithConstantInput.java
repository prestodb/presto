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
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.RowExpressionVariableInliner;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.facebook.presto.SystemSessionProperties.isRemoveCrossJoinWithConstantSingleRowInputEnabled;
import static com.facebook.presto.sql.planner.PlannerUtils.addProjections;
import static com.facebook.presto.sql.planner.plan.Patterns.join;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableMap.toImmutableMap;

/**
 * When one side of a cross join is one single row of constant, we can remove the cross join and replace it with a project.
 * <pre>
 * - Cross Join
 *      - table scan
 *          left_field
 *      - values // only one row
 *          right_field := 1
 * </pre>
 * into
 * <pre>
 * - project
 *      left_field := left_field
 *      right_field := 1
 *      - table scan
 *          left_field
 * </pre>
 */
public class RemoveCrossJoinWithConstantInput
        implements Rule<JoinNode>
{
    private final RowExpressionDeterminismEvaluator rowExpressionDeterminismEvaluator;

    public RemoveCrossJoinWithConstantInput(FunctionAndTypeManager functionAndTypeManager)
    {
        this.rowExpressionDeterminismEvaluator = new RowExpressionDeterminismEvaluator(functionAndTypeManager);
    }

    @Override
    public Pattern<JoinNode> getPattern()
    {
        return join().matching(x -> x.getType().equals(JoinType.INNER) && x.getCriteria().isEmpty());
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isRemoveCrossJoinWithConstantSingleRowInputEnabled(session);
    }

    @Override
    public Result apply(JoinNode node, Captures captures, Context context)
    {
        PlanNode singleValueInput;
        PlanNode joinInput;
        PlanNode leftInput = context.getLookup().resolve(node.getLeft());
        PlanNode rightInput = context.getLookup().resolve(node.getRight());
        if (isOutputSingleConstantRow(rightInput, context)) {
            singleValueInput = rightInput;
            joinInput = leftInput;
        }
        else if (isOutputSingleConstantRow(leftInput, context)) {
            singleValueInput = leftInput;
            joinInput = rightInput;
        }
        else {
            return Result.empty();
        }
        Optional<Map<VariableReferenceExpression, RowExpression>> mapping = getConstantAssignments(singleValueInput, context);
        if (!mapping.isPresent()) {
            return Result.empty();
        }
        PlanNode resultNode = addProjections(joinInput, context.getIdAllocator(), mapping.get());
        if (node.getFilter().isPresent()) {
            resultNode = new FilterNode(node.getSourceLocation(), context.getIdAllocator().getNextId(), resultNode, node.getFilter().get());
        }
        return Result.ofPlanNode(resultNode);
    }

    private boolean isOutputSingleConstantRow(PlanNode planNode, Context context)
    {
        while (planNode instanceof ProjectNode) {
            planNode = context.getLookup().resolve(((ProjectNode) planNode).getSource());
        }
        if (planNode instanceof ValuesNode) {
            return ((ValuesNode) planNode).getRows().size() == 1;
        }
        return false;
    }

    private Optional<Map<VariableReferenceExpression, RowExpression>> getConstantAssignments(PlanNode planNode, Context context)
    {
        List<VariableReferenceExpression> outputVariables = planNode.getOutputVariables();
        Map<VariableReferenceExpression, RowExpression> mapping = outputVariables.stream().collect(toImmutableMap(Function.identity(), Function.identity()));
        while (planNode instanceof ProjectNode) {
            Map<VariableReferenceExpression, RowExpression> assignments = ((ProjectNode) planNode).getAssignments().getMap();
            mapping = updateAssignments(mapping, assignments);
            planNode = context.getLookup().resolve(((ProjectNode) planNode).getSource());
        }

        checkState(planNode instanceof ValuesNode);
        ValuesNode valuesNode = (ValuesNode) planNode;
        if (!valuesNode.getOutputVariables().isEmpty()) {
            Map<VariableReferenceExpression, RowExpression> assignments = IntStream.range(0, valuesNode.getOutputVariables().size()).boxed()
                    .collect(toImmutableMap(idx -> valuesNode.getOutputVariables().get(idx), idx -> valuesNode.getRows().get(0).get(idx)));
            mapping = updateAssignments(mapping, assignments);
        }
        boolean allDeterministic = mapping.values().stream().allMatch(rowExpressionDeterminismEvaluator::isDeterministic);
        if (allDeterministic) {
            return Optional.of(mapping);
        }
        return Optional.empty();
    }

    private static Map<VariableReferenceExpression, RowExpression> updateAssignments(Map<VariableReferenceExpression, RowExpression> mapping, Map<VariableReferenceExpression, RowExpression> newAssignments)
    {
        return mapping.entrySet().stream().collect(toImmutableMap(Map.Entry::getKey, entry -> RowExpressionVariableInliner.inlineVariables(newAssignments, entry.getValue())));
    }
}
