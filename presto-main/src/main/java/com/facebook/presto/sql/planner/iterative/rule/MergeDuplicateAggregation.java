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
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.facebook.presto.SystemSessionProperties.isMergeDuplicateAggregationsEnabled;
import static com.facebook.presto.spi.plan.ProjectNode.Locality.LOCAL;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.identityAssignments;
import static com.facebook.presto.sql.planner.plan.Patterns.aggregation;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;

public class MergeDuplicateAggregation
        implements Rule<AggregationNode>
{
    private static final Pattern<AggregationNode> PATTERN = aggregation();
    private final FunctionAndTypeManager functionAndTypeManager;

    public MergeDuplicateAggregation(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionAndTypeManager = functionAndTypeManager;
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(AggregationNode node, Captures captures, Context context)
    {
        if (!isMergeDuplicateAggregationsEnabled(context.getSession())) {
            return Result.empty();
        }
        Map<AggregationNode.Aggregation, List<VariableReferenceExpression>> aggregationToVariableList = node.getAggregations().entrySet().stream()
                .collect(Collectors.groupingBy(Map.Entry::getValue, Collectors.mapping(Map.Entry::getKey, Collectors.toList())));
        ImmutableMap.Builder<VariableReferenceExpression, VariableReferenceExpression> variablesToMergeBuilder = ImmutableMap.builder();
        for (Map.Entry<AggregationNode.Aggregation, List<VariableReferenceExpression>> entry : aggregationToVariableList.entrySet()) {
            List<VariableReferenceExpression> variableToSameAggregation = entry.getValue();
            if (variableToSameAggregation.size() <= 1 || !functionAndTypeManager.getFunctionMetadata(entry.getKey().getFunctionHandle()).isDeterministic()) {
                continue;
            }
            for (int i = 1; i < variableToSameAggregation.size(); ++i) {
                variablesToMergeBuilder.put(variableToSameAggregation.get(i), variableToSameAggregation.get(0));
            }
        }
        ImmutableMap<VariableReferenceExpression, VariableReferenceExpression> variablesToMerge = variablesToMergeBuilder.build();
        if (variablesToMerge.isEmpty()) {
            return Result.empty();
        }
        Map<VariableReferenceExpression, AggregationNode.Aggregation> aggregations = node.getAggregations().entrySet().stream()
                .filter(x -> !variablesToMerge.containsKey(x.getKey()))
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
        Assignments.Builder assignments = Assignments.builder();
        assignments.putAll(identityAssignments(node.getOutputVariables().stream().filter(x -> !variablesToMerge.containsKey(x)).collect(toImmutableList())));
        assignments.putAll(variablesToMerge.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
        return Result.ofPlanNode(
                new ProjectNode(
                        node.getSourceLocation(),
                        context.getIdAllocator().getNextId(),
                        new AggregationNode(
                                node.getSourceLocation(),
                                node.getId(),
                                node.getSource(),
                                aggregations,
                                node.getGroupingSets(),
                                node.getPreGroupedVariables(),
                                node.getStep(),
                                node.getHashVariable(),
                                node.getGroupIdVariable()),
                        assignments.build(),
                        LOCAL));
    }
}
