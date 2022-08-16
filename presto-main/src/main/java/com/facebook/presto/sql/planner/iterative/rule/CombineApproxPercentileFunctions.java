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
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.BuiltInFunctionHandle;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.facebook.presto.SystemSessionProperties.isCombineApproxPercentileEnabled;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.plan.Patterns.aggregation;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * For multiple approx_percentile() function calls on the same column with different percentile arguments, combine them to one call on an array of percentile arguments.
 * <p>
 * From:
 * <pre>
 * - Aggregation (approx_percentile(col, 0.2), approx_percentile(col, 0.8)
 * </pre>
 * To:
 * <pre>
 * - Project (approx_percentile_results[1], approx_percentile_results[2])
 *   - Aggregation (approx_percentile_results <- approx_percentile(col, array)
 *     - Project (col <- col, array <- [0.2, 0.8])
 * </pre>
 * <p>
 */

public class CombineApproxPercentileFunctions
        implements Rule<AggregationNode>
{
    private static final String APPROX_PERCENTILE = "approx_percentile";
    private static final String ARRAY_CONSTRUCTOR = "array_constructor";
    private static final String ELEMENT_AT = "element_at";
    // Limit specified in `ArrayConstructor` function.
    private static final int ARRAY_SIZE_LIMIT = 254;
    private final FunctionAndTypeManager functionAndTypeManager;

    public CombineApproxPercentileFunctions(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
    }

    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .matching(CombineApproxPercentileFunctions::hasMultipleApproxPercentile);

    private static boolean hasMultipleApproxPercentile(AggregationNode aggregation)
    {
        return aggregation.getAggregations().values().stream()
                .filter(agg -> agg.getCall().getDisplayName().equals(APPROX_PERCENTILE)).count() > 1;
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isCombineApproxPercentileEnabled(session);
    }

    // Get the position of the percentile argument in arguments
    private static int getPercentilePosition(FunctionHandle functionHandle)
    {
        // approx_percentile(x, percentage) -> arguments.size() == 2
        // approx_percentile(x, percentage, accuracy) -> arguments.size() == 3 && arguments.get(1) is Double
        // approx_percentile(x, w, percentage) -> arguments.size() == 3 && arguments.get(1) is BigInt
        // approx_percentile(x, w, percentage, accuracy) -> arguments.size() == 4
        checkState(functionHandle instanceof BuiltInFunctionHandle);
        List<TypeSignature> argumentTypes = ((BuiltInFunctionHandle) functionHandle).getSignature().getArgumentTypes();
        if (argumentTypes.size() == 2 || (argumentTypes.size() == 3 && argumentTypes.get(1).getBase().equals(StandardTypes.DOUBLE))) {
            return 1;
        }
        checkState(argumentTypes.size() == 4 || (argumentTypes.size() == 3 && argumentTypes.get(1).getBase().equals(StandardTypes.BIGINT)));
        return 2;
    }

    private static boolean aggregationCanMerge(AggregationNode.Aggregation aggregation1, AggregationNode.Aggregation aggregation2)
    {
        if (!aggregation1.getMask().equals(aggregation2.getMask())
                || !aggregation1.getOrderBy().equals(aggregation2.getOrderBy())
                || !aggregation1.getFilter().equals(aggregation2.getFilter())
                || aggregation1.isDistinct() != aggregation2.isDistinct()) {
            return false;
        }
        // Check call expression, the only difference should be percentile argument
        CallExpression expression1 = aggregation1.getCall();
        CallExpression expression2 = aggregation2.getCall();
        int percentilePosition = getPercentilePosition(expression1.getFunctionHandle());
        if (!expression1.getFunctionHandle().equals(expression2.getFunctionHandle()) || expression1.getArguments().size() != expression2.getArguments().size()) {
            return false;
        }
        List<RowExpression> arguments1 = expression1.getArguments();
        List<RowExpression> arguments2 = expression2.getArguments();
        for (int i = 0; i < arguments1.size(); ++i) {
            if (i != percentilePosition && !arguments1.get(i).equals(arguments2.get(i))) {
                return false;
            }
        }
        return true;
    }

    private static List<RowExpression> changePercentileArgument(List<RowExpression> arguments, RowExpression percentileArgument, int percentilePosition)
    {
        ImmutableList.Builder<RowExpression> newAggCallArguments = new ImmutableList.Builder<>();
        for (int i = 0; i < arguments.size(); ++i) {
            if (i == percentilePosition) {
                newAggCallArguments.add(percentileArgument);
            }
            else {
                newAggCallArguments.add(arguments.get(i));
            }
        }
        return newAggCallArguments.build();
    }

    // Split the aggregations in candidateAggregations into multiple lists, with each list containing aggregations which can be merged.
    private static List<List<AggregationNode.Aggregation>> createMergeableAggregations(List<AggregationNode.Aggregation> candidateAggregations)
    {
        ImmutableList.Builder<List<AggregationNode.Aggregation>> result = ImmutableList.builder();
        Set<AggregationNode.Aggregation> mergedAggregation = new HashSet<>();
        for (int i = 0; i < candidateAggregations.size(); ++i) {
            if (mergedAggregation.contains(candidateAggregations.get(i))) {
                continue;
            }
            ImmutableList.Builder<AggregationNode.Aggregation> aggregationCanBeMerged = ImmutableList.builder();
            mergedAggregation.add(candidateAggregations.get(i));
            aggregationCanBeMerged.add(candidateAggregations.get(i));
            for (int j = i + 1; j < candidateAggregations.size(); ++j) {
                if (mergedAggregation.contains(candidateAggregations.get(j))) {
                    continue;
                }
                if (aggregationCanMerge(candidateAggregations.get(i), candidateAggregations.get(j))) {
                    mergedAggregation.add(candidateAggregations.get(j));
                    aggregationCanBeMerged.add(candidateAggregations.get(j));
                }
            }
            result.add(aggregationCanBeMerged.build());
        }
        return result.build();
    }

    private CallExpression createArrayPercentile(List<AggregationNode.Aggregation> aggregations)
    {
        List<RowExpression> percentileArray = aggregations.stream().map(x -> x.getArguments().get(getPercentilePosition(x.getFunctionHandle()))).collect(Collectors.toList());

        return call(
                functionAndTypeManager,
                ARRAY_CONSTRUCTOR,
                new ArrayType(percentileArray.get(0).getType()),
                percentileArray);
    }

    private AggregationNode.Aggregation createArrayAggregation(List<AggregationNode.Aggregation> candidateList, VariableReferenceExpression arrayVariableReference)
    {
        AggregationNode.Aggregation aggregationBeforeMerge = candidateList.get(0);
        int percentilePosition = getPercentilePosition(aggregationBeforeMerge.getFunctionHandle());
        List<RowExpression> newAggCallArguments = changePercentileArgument(aggregationBeforeMerge.getCall().getArguments(), arrayVariableReference, percentilePosition);
        Type colType = aggregationBeforeMerge.getCall().getArguments().get(0).getType();
        CallExpression approxPercentileCall = call(
                functionAndTypeManager,
                APPROX_PERCENTILE,
                new ArrayType(colType),
                newAggCallArguments);

        return new AggregationNode.Aggregation(
                approxPercentileCall,
                aggregationBeforeMerge.getFilter(),
                aggregationBeforeMerge.getOrderBy(),
                aggregationBeforeMerge.isDistinct(),
                aggregationBeforeMerge.getMask());
    }

    @Override
    public Result apply(AggregationNode aggregationNode, Captures captures, Context context)
    {
        ImmutableMap.Builder<VariableReferenceExpression, AggregationNode.Aggregation> aggregations = ImmutableMap.builder();

        // Only approx_percentile which does not take array as percentile arguments
        List<AggregationNode.Aggregation> approxPercentile = aggregationNode.getAggregations().values().stream().filter(
                x -> x.getCall().getDisplayName().equals(APPROX_PERCENTILE) && !(x.getCall().getType() instanceof ArrayType)
        ).collect(Collectors.toList());

        // Group the aggregations on the same column and have the same function handle
        Map<RowExpression, Map<FunctionHandle, List<AggregationNode.Aggregation>>> sameColumnHandle =
                approxPercentile.stream().collect(Collectors.groupingBy(x -> x.getCall().getArguments().get(0), LinkedHashMap::new,
                        Collectors.groupingBy(x -> x.getFunctionHandle(), LinkedHashMap::new, Collectors.toList())));

        // Each list contains the aggregations which can be combined
        ImmutableList.Builder<List<AggregationNode.Aggregation>> candidateLists = ImmutableList.builder();
        sameColumnHandle.values().forEach(sameHandle -> {
            sameHandle.values().forEach(aggregationList -> {
                candidateLists.addAll(createMergeableAggregations(aggregationList));
            });
        });
        // ArrayConstructor does not support more than 254 elements
        List<List<AggregationNode.Aggregation>> candidateAggregationLists =
                candidateLists.build().stream().filter(x -> x.size() > 1 && x.size() < ARRAY_SIZE_LIMIT).collect(Collectors.toList());

        if (candidateAggregationLists.isEmpty()) {
            return Result.empty();
        }

        // Record which aggregations are combined
        Set<AggregationNode.Aggregation> combinedAggregations = candidateAggregationLists.stream().flatMap(List::stream).collect(Collectors.toSet());
        // Record mapping between aggregation and corresponding output variable reference
        Map<AggregationNode.Aggregation, VariableReferenceExpression> aggregationVariableMap = new HashMap<>();
        // Record which variable references are combined
        Set<VariableReferenceExpression> combinedVariableReference = new HashSet<>();
        aggregationNode.getAggregations().forEach((variable, aggregation) -> {
            if (combinedAggregations.contains(aggregation)) {
                aggregationVariableMap.put(aggregation, variable);
                combinedVariableReference.add(variable);
            }
        });

        // Build a project node as the source of the aggregation. This contains aggregation inputs and percentile arrays
        Assignments.Builder sourceProjectAssignments = Assignments.builder();
        // Build a project node as the output of the aggregation, do subscription for the combined approx_percentile, and keep the others
        Assignments.Builder outputProjectAssignments = Assignments.builder();
        for (List<AggregationNode.Aggregation> candidateList : candidateAggregationLists) {
            // Build array of percentile arguments
            RowExpression arrayExpression = createArrayPercentile(candidateList);
            VariableReferenceExpression arrayVariableReference = context.getVariableAllocator().newVariable(arrayExpression);
            sourceProjectAssignments.put(arrayVariableReference, arrayExpression);

            // Build aggregations taking percentile array as arguments
            AggregationNode.Aggregation newAggregation = createArrayAggregation(candidateList, arrayVariableReference);
            VariableReferenceExpression newVariableReference = context.getVariableAllocator().newVariable(newAggregation.getCall());
            aggregations.put(newVariableReference, newAggregation);

            // Build element_at expression
            Map<VariableReferenceExpression, RowExpression> elementAtMap =
                    IntStream.range(0, candidateList.size()).boxed().collect(ImmutableMap.toImmutableMap(
                            x -> aggregationVariableMap.get(candidateList.get(x)),
                            x -> call(
                                    functionAndTypeManager,
                                    ELEMENT_AT,
                                    candidateList.get(x).getArguments().get(0).getType(),
                                    ImmutableList.of(newVariableReference, constant((long) x + 1, BIGINT)))));
            outputProjectAssignments.putAll(elementAtMap);
        }

        // Add aggregations which are not combined to the new aggregation node.
        aggregationNode.getAggregations().forEach((key, value) -> {
            if (!combinedVariableReference.contains(key)) {
                aggregations.put(key, value);
            }
        });

        // Add output of the old aggregations which are not changed in the rewrite to the parent projection node.
        aggregationNode.getOutputVariables().forEach(variable -> {
            if (!combinedVariableReference.contains(variable)) {
                outputProjectAssignments.put(variable, variable);
            }
        });

        aggregationNode.getSource().getOutputVariables().forEach(variable -> sourceProjectAssignments.put(variable, variable));

        return Result.ofPlanNode(
                new ProjectNode(context.getIdAllocator().getNextId(),
                        new AggregationNode(
                                aggregationNode.getSourceLocation(),
                                context.getIdAllocator().getNextId(),
                                new ProjectNode(context.getIdAllocator().getNextId(),
                                        aggregationNode.getSource(), sourceProjectAssignments.build()),
                                aggregations.build(),
                                aggregationNode.getGroupingSets(),
                                ImmutableList.of(),
                                aggregationNode.getStep(),
                                aggregationNode.getHashVariable(),
                                aggregationNode.getGroupIdVariable()),
                        outputProjectAssignments.build()));
    }
}
