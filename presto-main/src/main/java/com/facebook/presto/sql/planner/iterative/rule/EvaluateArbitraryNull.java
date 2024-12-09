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
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.common.type.UnknownType.UNKNOWN;
import static com.facebook.presto.sql.planner.plan.Patterns.aggregation;
import static com.facebook.presto.sql.relational.Expressions.constant;

/**
 * Evaluates arbitrary(NULL) operations to NULL.
 * <p>
 * From:
 * <pre>
 * - Aggregation (arbitrary(NULL), ...otherAggregations)
 *   - Source
 * </pre>
 * To:
 * <pre>
 * - Project (arbitrary <- null)
 *   - Aggregation (...otherAggregations)
 *     - Source
 * </pre>
 * </p>
 */
public class EvaluateArbitraryNull
        implements Rule<AggregationNode>
{
    private static final String ARBITRARY = "arbitrary";
    private static final Pattern<AggregationNode> PATTERN = aggregation().matching(EvaluateArbitraryNull::hasArbitraryNull);

    private static boolean hasArbitraryNull(AggregationNode aggregation)
    {
        return aggregation.getAggregations().values().stream()
                .anyMatch(agg -> agg.getCall().getDisplayName().equals(ARBITRARY) && agg.getCall().getType() == UNKNOWN);
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(AggregationNode node, Captures captures, Context context)
    {
        ImmutableMap.Builder<VariableReferenceExpression, AggregationNode.Aggregation> existingAggregationsBuilder = ImmutableMap.builder();
        Set<String> arbitraryNullsInputs = new HashSet<>();
        Set<VariableReferenceExpression> arbitraryNulls = new HashSet<>();

        // Record arbitrary(NULL) expressions and their inputs
        node.getAggregations().forEach((key, value) -> {
            if (value.getCall().getDisplayName().equals(ARBITRARY) && value.getCall().getType() == UNKNOWN) {
                arbitraryNulls.add(key);
                arbitraryNullsInputs.addAll(value.getArguments().stream().map(RowExpression::toString).collect(Collectors.toList()));
            }
            else {
                existingAggregationsBuilder.put(key, value);
            }
        });

        // Keep only the variables that are not inputs to arbitrary(NULL) expressions
        Assignments.Builder sourceProjectAssignmentsBuilder = Assignments.builder();
        node.getSource().getOutputVariables().forEach(variable -> {
            if (!arbitraryNullsInputs.contains(variable.getName())) {
                sourceProjectAssignmentsBuilder.put(variable, variable);
            }
        });

        // Assign NULL for variables that were assigned the arbitrary(NULL) expressions, else keep as is
        Assignments.Builder outputProjectAssignmentsBuilder = Assignments.builder();
        node.getOutputVariables().forEach(variable -> {
            if (arbitraryNulls.contains(variable)) {
                outputProjectAssignmentsBuilder.put(variable, constant(null, UNKNOWN));
            }
            else {
                outputProjectAssignmentsBuilder.put(variable, variable);
            }
        });

        ImmutableMap<VariableReferenceExpression, AggregationNode.Aggregation> existingAggregations = existingAggregationsBuilder.build();
        Assignments sourceProjectAssignments = sourceProjectAssignmentsBuilder.build();
        Assignments outputProjectAssignments = outputProjectAssignmentsBuilder.build();

        // Return an empty ValuesNode if there are no other aggregations and no outputs to pass from the source
        if (existingAggregations.isEmpty() && sourceProjectAssignments.isEmpty()) {
            return Result.ofPlanNode(
                    new ProjectNode(context.getIdAllocator().getNextId(),
                            new ValuesNode(
                                    node.getSourceLocation(),
                                    context.getIdAllocator().getNextId(),
                                    ImmutableList.of(),
                                    ImmutableList.of(ImmutableList.of()),
                                    Optional.empty()),
                            outputProjectAssignments));
        }
        return Result.ofPlanNode(
                new ProjectNode(context.getIdAllocator().getNextId(),
                        new AggregationNode(
                                node.getSourceLocation(),
                                context.getIdAllocator().getNextId(),
                                new ProjectNode(context.getIdAllocator().getNextId(),
                                        node.getSource(), sourceProjectAssignments),
                                existingAggregations,
                                node.getGroupingSets(),
                                ImmutableList.of(),
                                node.getStep(),
                                node.getHashVariable(),
                                node.getGroupIdVariable(),
                                node.getAggregationId()),
                        outputProjectAssignments));
    }
}
