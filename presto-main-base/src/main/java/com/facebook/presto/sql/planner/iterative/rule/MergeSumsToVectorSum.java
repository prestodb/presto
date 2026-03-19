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
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.SystemSessionProperties.getMergeSumsToVectorSumThreshold;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.planner.plan.Patterns.aggregation;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static java.util.Objects.requireNonNull;

/**
 * Merges multiple SUM aggregations into a single vector_sum aggregation when the
 * number of eligible SUMs exceeds a configurable threshold.
 * <p>
 * This optimization is targeted at queries with many scalar SUM aggregations over
 * different columns of the same type (e.g., hundreds of REAL or BIGINT columns).
 * Instead of computing each SUM independently, the columns are packed into an array,
 * and a single vector_sum call computes all sums in one vectorized pass.
 * <p>
 * From:
 * <pre>
 * - Aggregation (sum(col1), sum(col2), ..., sum(colN), count(*))
 * </pre>
 * To:
 * <pre>
 * - Project (element_at(vec_result, 1) AS col1, element_at(vec_result, 2) AS col2, ..., count_result)
 *   - Aggregation (vec_result <- vector_sum(arr), count_result <- count(*))
 *     - Project (arr <- array_constructor(col1, col2, ..., colN), ...)
 * </pre>
 */
public class MergeSumsToVectorSum
        implements Rule<AggregationNode>
{
    private static final String SUM = "sum";
    private static final String VECTOR_SUM = "vector_sum";
    private static final String ARRAY_CONSTRUCTOR = "array_constructor";
    private static final String ELEMENT_AT = "element_at";
    private static final int ARRAY_SIZE_LIMIT = 254;

    private static final Pattern<AggregationNode> PATTERN = aggregation();

    private final FunctionAndTypeManager functionAndTypeManager;

    public MergeSumsToVectorSum(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return getMergeSumsToVectorSumThreshold(session) > 0;
    }

    @Override
    public Result apply(AggregationNode node, Captures captures, Context context)
    {
        int threshold = getMergeSumsToVectorSumThreshold(context.getSession());
        if (threshold <= 0) {
            return Result.empty();
        }

        // Only apply to SINGLE step aggregations (not partial/intermediate/final).
        // For multi-stage aggregations, the rewrite would need to handle
        // partial vector_sum state, which is not supported yet.
        if (node.getStep() != AggregationNode.Step.SINGLE) {
            return Result.empty();
        }

        // Find eligible SUM aggregations and group them by argument type.
        // Eligible means: simple SUM with exactly one argument, no DISTINCT,
        // no FILTER, no ORDER BY, no MASK.
        Map<Type, List<Map.Entry<VariableReferenceExpression, AggregationNode.Aggregation>>> byType =
                node.getAggregations().entrySet().stream()
                        .filter(e -> isEligibleSum(e.getValue()))
                        .collect(Collectors.groupingBy(
                                e -> e.getValue().getCall().getArguments().get(0).getType(),
                                LinkedHashMap::new,
                                Collectors.toList()));

        // Only merge type groups that meet the threshold and fit within array size limits
        Map<Type, List<Map.Entry<VariableReferenceExpression, AggregationNode.Aggregation>>> mergeableGroups =
                byType.entrySet().stream()
                        .filter(e -> e.getValue().size() >= threshold && e.getValue().size() <= ARRAY_SIZE_LIMIT)
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                Map.Entry::getValue,
                                (a, b) -> a,
                                LinkedHashMap::new));

        if (mergeableGroups.isEmpty()) {
            return Result.empty();
        }

        // Verify vector_sum function is available before proceeding
        if (!isVectorSumAvailable(mergeableGroups)) {
            return Result.empty();
        }

        Set<VariableReferenceExpression> mergedVars = new HashSet<>();
        mergeableGroups.values().stream().flatMap(List::stream).forEach(e -> mergedVars.add(e.getKey()));

        // Build source ProjectNode: pass through all source columns + pack SUM args into arrays
        Assignments.Builder sourceProjectAssignments = Assignments.builder();
        node.getSource().getOutputVariables().forEach(v -> sourceProjectAssignments.put(v, v));

        // Build new aggregations and output projections
        ImmutableMap.Builder<VariableReferenceExpression, AggregationNode.Aggregation> newAggregations = ImmutableMap.builder();
        Assignments.Builder outputProjectAssignments = Assignments.builder();

        for (Map.Entry<Type, List<Map.Entry<VariableReferenceExpression, AggregationNode.Aggregation>>> typeGroup :
                mergeableGroups.entrySet()) {
            Type elemType = typeGroup.getKey();
            List<Map.Entry<VariableReferenceExpression, AggregationNode.Aggregation>> entries = typeGroup.getValue();

            // Create array_constructor(col1, col2, ..., colN)
            List<RowExpression> arrayElements = entries.stream()
                    .map(e -> e.getValue().getCall().getArguments().get(0))
                    .collect(ImmutableList.toImmutableList());

            ArrayType arrayType = new ArrayType(elemType);
            CallExpression arrayConstructorCall = call(
                    functionAndTypeManager,
                    ARRAY_CONSTRUCTOR,
                    arrayType,
                    arrayElements);
            VariableReferenceExpression arrayVar = context.getVariableAllocator().newVariable(arrayConstructorCall);
            sourceProjectAssignments.put(arrayVar, arrayConstructorCall);

            // Create vector_sum(array_var) aggregation
            CallExpression vectorSumCall = call(
                    functionAndTypeManager,
                    VECTOR_SUM,
                    arrayType,
                    ImmutableList.of(arrayVar));
            VariableReferenceExpression vectorSumVar = context.getVariableAllocator().newVariable(vectorSumCall);
            newAggregations.put(vectorSumVar, new AggregationNode.Aggregation(
                    vectorSumCall,
                    Optional.empty(),
                    Optional.empty(),
                    false,
                    Optional.empty()));

            // Create element_at projections to extract individual results
            for (int i = 0; i < entries.size(); i++) {
                VariableReferenceExpression originalVar = entries.get(i).getKey();
                CallExpression elementAtCall = call(
                        functionAndTypeManager,
                        ELEMENT_AT,
                        elemType,
                        ImmutableList.of(vectorSumVar, constant((long) (i + 1), BIGINT)));
                outputProjectAssignments.put(originalVar, elementAtCall);
            }
        }

        // Add non-merged aggregations unchanged
        node.getAggregations().forEach((key, value) -> {
            if (!mergedVars.contains(key)) {
                newAggregations.put(key, value);
            }
        });

        // Add identity projections for non-merged output variables
        node.getOutputVariables().forEach(v -> {
            if (!mergedVars.contains(v)) {
                outputProjectAssignments.put(v, v);
            }
        });

        // Build the result plan:
        // ProjectNode (extract elements via element_at)
        //   └── AggregationNode (vector_sum + remaining aggregations)
        //       └── ProjectNode (array_constructor + identity)
        //           └── Original Source
        return Result.ofPlanNode(
                new ProjectNode(
                        context.getIdAllocator().getNextId(),
                        new AggregationNode(
                                node.getSourceLocation(),
                                context.getIdAllocator().getNextId(),
                                new ProjectNode(
                                        context.getIdAllocator().getNextId(),
                                        node.getSource(),
                                        sourceProjectAssignments.build()),
                                newAggregations.build(),
                                node.getGroupingSets(),
                                node.getPreGroupedVariables(),
                                node.getStep(),
                                node.getHashVariable(),
                                node.getGroupIdVariable(),
                                node.getAggregationId()),
                        outputProjectAssignments.build()));
    }

    private boolean isEligibleSum(AggregationNode.Aggregation aggregation)
    {
        CallExpression call = aggregation.getCall();
        return call.getDisplayName().equalsIgnoreCase(SUM)
                && call.getArguments().size() == 1
                && !aggregation.isDistinct()
                && !aggregation.getFilter().isPresent()
                && !aggregation.getOrderBy().isPresent()
                && !aggregation.getMask().isPresent();
    }

    private boolean isVectorSumAvailable(
            Map<Type, List<Map.Entry<VariableReferenceExpression, AggregationNode.Aggregation>>> mergeableGroups)
    {
        for (Type elemType : mergeableGroups.keySet()) {
            try {
                functionAndTypeManager.lookupFunction(VECTOR_SUM, fromTypes(new ArrayType(elemType)));
            }
            catch (PrestoException e) {
                return false;
            }
        }
        return true;
    }
}
