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
import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.UnionNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.SystemSessionProperties.isOptimizeUnionOverValues;
import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.sql.planner.plan.Patterns.sources;
import static com.facebook.presto.sql.planner.plan.Patterns.union;

public class MergeConstantValuesUnderUnion
        implements Rule<UnionNode>
{
    private static final Capture<List<PlanNode>> CHILDREN = newCapture();
    private static final Pattern<UnionNode> PATTERN = union()
            .with(sources().capturedAs(CHILDREN));

    @Override
    public Pattern<UnionNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isOptimizeUnionOverValues(session);
    }

    @Override
    public Result apply(UnionNode node, Captures captures, Context context)
    {
        List<PlanNode> values = captures.get(CHILDREN);
        int valueNodeCount = getValuesNodeCount(values, context);

        // Return if not union over 2 or more ValuesNodes
        if (valueNodeCount <= 1) {
            return Result.empty();
        }

        // Merge all rows for all ValuesNodes in the union
        List<ValuesNode> valuesNodes = new ArrayList<>();
        List<List<RowExpression>> valueRowsForUnionNode = new ArrayList<>();
        List<List<RowExpression>> valueRowsForValuesNode = new ArrayList<>();
        List<PlanNode> sources = new ArrayList<>(); // Collect sources for a possible new union node
        for (PlanNode child : values) {
            PlanNode resolvedChild = context.getLookup().resolve(child);
            if (resolvedChild instanceof ValuesNode) {
                ValuesNode valuesNode = (ValuesNode) resolvedChild;
                valuesNodes.add(valuesNode);
                valueRowsForValuesNode.addAll(valuesNode.getRows());
                List<List<RowExpression>> reorderedRows = rowColumnsByOutputVariableOrder(valuesNode, node.getOutputVariables(), node.getVariableMapping());
                valueRowsForUnionNode.addAll(reorderedRows);
            }
            else {
                sources.add(resolvedChild); // Collect sources for a possible new union node
            }
        }

        // If the union contains only ValuesNodes then we return a merged ValuesNode
        if (values.size() == valueNodeCount) {
            return Result.ofPlanNode(new ValuesNode(context.getIdAllocator().getNextId(), node.getOutputVariables(), valueRowsForValuesNode));
        }

        // Generate new output variables for value node to correspond to each of the union node output variables
        // and initialize the map for the union node output variables to receive their input variables
        List<VariableReferenceExpression> valueNodeOutputVariableList = new ArrayList<>();
        Map<VariableReferenceExpression, List<VariableReferenceExpression>> outputToInputsVariableMapping = new HashMap<>();
        int resultOutputVariableCount = 0;
        for (VariableReferenceExpression unionNodeOutputVariable : node.getOutputVariables()) {
            resultOutputVariableCount++;
            VariableReferenceExpression var = context.getVariableAllocator().newVariable("union_val_expr_" + resultOutputVariableCount, unionNodeOutputVariable.getType());
            valueNodeOutputVariableList.add(var);
            List<VariableReferenceExpression> inputsVariableList = new ArrayList<>();
            outputToInputsVariableMapping.put(unionNodeOutputVariable, inputsVariableList);
        }

        ValuesNode optimizedValuesNode = new ValuesNode(context.getIdAllocator().getNextId(), valueNodeOutputVariableList, valueRowsForUnionNode);
        sources.add(optimizedValuesNode);

        // Update the map for the union node output variables to their input variables
        int i = 0;
        for (VariableReferenceExpression unionNodeOutputVariable : node.getOutputVariables()) {
            for (PlanNode resolvedChild : sources) {
                outputToInputsVariableMapping.get(unionNodeOutputVariable).add(resolvedChild.getOutputVariables().get(i));
            }
            i++;
        }

        UnionNode unionNode = new UnionNode(context.getIdAllocator().getNextId(), sources, node.getOutputVariables(), outputToInputsVariableMapping);
        return Result.ofPlanNode(unionNode);
    }

    private int getValuesNodeCount(List<PlanNode> planNodes, Context context)
    {
        int valueNodeCount = 0;
        for (PlanNode planNode : planNodes) {
            if (context.getLookup().resolve(planNode) instanceof ValuesNode) {
                valueNodeCount++;
            }
        }
        return valueNodeCount;
    }

    private List<List<RowExpression>> rowColumnsByOutputVariableOrder(ValuesNode valuesNode,
            List<VariableReferenceExpression> outputVariables, Map<VariableReferenceExpression, List<VariableReferenceExpression>> outputToInputs)
    {
        List<List<RowExpression>> reorderedRows = new ArrayList<>();
        for (int i = 0; i < valuesNode.getRows().size(); i++) {
            reorderedRows.add(new ArrayList<>());
        }
        for (VariableReferenceExpression col : outputVariables) {
            for (int rowIndex = 0; rowIndex < reorderedRows.size(); rowIndex++) {
                int originalColIndex = originalColIndex(valuesNode.getOutputVariables(), outputToInputs.get(col));
                reorderedRows.get(rowIndex).add(valuesNode.getRows().get(rowIndex).get(originalColIndex));
            }
        }
        return reorderedRows;
    }

    private int originalColIndex(List<VariableReferenceExpression> inputsForNode, List<VariableReferenceExpression> inputsFromMap)
    {
        int index = 0;
        for (VariableReferenceExpression nodeInputCol : inputsForNode) {
            if (inputsFromMap.contains(nodeInputCol)) {
                return index;
            }
            index++;
        }
        throw new IllegalArgumentException("Invalid input variables: " + inputsForNode.toString() + " - from map inputs: " + inputsFromMap.toString());
    }
}
