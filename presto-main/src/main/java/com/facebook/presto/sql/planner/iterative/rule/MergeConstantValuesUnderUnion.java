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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
        //Return if not union over ValuesNode
        if (!(values.stream().map(x -> context.getLookup().resolve(x)).allMatch(x -> x instanceof ValuesNode))) {
            return Result.empty();
        }

        ImmutableMap<VariableReferenceExpression, List<RowExpression>> inputVariablesToValuesMap = getInputVariablesToValuesMap(context, values);

        List<List<RowExpression>> finalValues = generateRowsFromValues(node, context, values, inputVariablesToValuesMap);
        return Result.ofPlanNode(
                  new ValuesNode(
                  node.getId(),
                  node.getOutputVariables(),
                  finalValues));
    }

    private List<List<RowExpression>> generateRowsFromValues(UnionNode node, Context context, List<PlanNode> values, Map<VariableReferenceExpression, List<RowExpression>> columnToValuesMap)
    {
        Map<VariableReferenceExpression, List<RowExpression>> outputVarToValuesMap = getOutputVariablesToValuesMap(node, columnToValuesMap);
        List<Iterator> iterators = node.getOutputVariables().stream().map(x -> outputVarToValuesMap.get(x).iterator()).collect(Collectors.toList());

        int totalRows = values.stream().map(x -> (ValuesNode) context.getLookup().resolve(x)).mapToInt(x -> x.getRows().size()).sum();
        ImmutableList.Builder<List<RowExpression>> finalValues = ImmutableList.builder();

        for (int i = 0; i < totalRows; i++) {
            ImmutableList.Builder<RowExpression> row = ImmutableList.builder();
            for (Iterator<RowExpression> iterator : iterators) {
                row.add(iterator.next());
            }
            finalValues.add(row.build());
        }
        return finalValues.build();
    }

    private Map<VariableReferenceExpression, List<RowExpression>> getOutputVariablesToValuesMap(UnionNode node, Map<VariableReferenceExpression, List<RowExpression>> columnToValuesMap)
    {
        Map<VariableReferenceExpression, List<RowExpression>> outputVarToValuesMap = new HashMap<>();
        ImmutableMap.Builder<VariableReferenceExpression, List<RowExpression>> outputVarToValuesMapBuilder = ImmutableMap.builder();
        for (Map.Entry<VariableReferenceExpression, List<VariableReferenceExpression>> entry : node.getVariableMapping().entrySet()) {
            VariableReferenceExpression outputVar = entry.getKey();
            for (VariableReferenceExpression input : entry.getValue()) {
                if (outputVarToValuesMap.get(outputVar) == null) {
                    outputVarToValuesMap.put(outputVar, new ArrayList<>());
                }
                outputVarToValuesMap.get(outputVar).addAll(columnToValuesMap.get(input));
            }
        }
        return outputVarToValuesMap;
    }

    private ImmutableMap<VariableReferenceExpression, List<RowExpression>> getInputVariablesToValuesMap(Context context, List<PlanNode> values)
    {
        Map<VariableReferenceExpression, List<RowExpression>> columnToValuesMap = new HashMap<>();
        for (PlanNode child : values) {
            ValuesNode value = (ValuesNode) context.getLookup().resolve(child);
            for (List<RowExpression> row : value.getRows()) {
                for (int i = 0; i < row.size(); i++) {
                    List<RowExpression> colValues = columnToValuesMap.get(value.getOutputVariables().get(i));
                    if (colValues == null) {
                        colValues = new ArrayList<>();
                    }
                    colValues.add(row.get(i));
                    columnToValuesMap.put(value.getOutputVariables().get(i), colValues);
                }
            }
        }
        return ImmutableMap.copyOf(columnToValuesMap);
    }
}
