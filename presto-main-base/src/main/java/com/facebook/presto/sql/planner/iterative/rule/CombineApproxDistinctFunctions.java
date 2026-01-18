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
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
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

import static com.facebook.presto.SystemSessionProperties.isCombineApproxDistinctEnabled;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.COALESCE;
import static com.facebook.presto.sql.planner.plan.Patterns.aggregation;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

/**
 * For multiple approx_distinct() function calls on expressions of the same type, combine them using set_agg.
 * <p>
 * From:
 * <pre>
 *   Aggregation (approx_distinct(e1), approx_distinct(e2), approx_distinct(e3))
 * </pre>
 * To:
 * <pre>
 *   Project (coalesce(cardinality(array_distinct(remove_nulls(ads[1]))), 0),
 *            coalesce(cardinality(array_distinct(remove_nulls(ads[2]))), 0),
 *            coalesce(cardinality(array_distinct(remove_nulls(ads[3]))), 0))
 *   - Project (ads <- transpose(ads_array))
 *     - Aggregation (ads_array <- set_agg(array[e1, e2, e3]))
 * </pre>
 * <p>
 */
public class CombineApproxDistinctFunctions
        implements Rule<AggregationNode>
{
    private static final String APPROX_DISTINCT = "approx_distinct";
    private static final String SET_AGG = "set_agg";
    private static final String ARRAY_CONSTRUCTOR = "array_constructor";
    private static final String ARRAY_TRANSPOSE = "array_transpose";
    private static final String ARRAY_DISTINCT = "array_distinct";
    private static final String REMOVE_NULLS = "remove_nulls";
    private static final String CARDINALITY = "cardinality";
    private static final String ELEMENT_AT = "element_at";

    private final FunctionAndTypeManager functionAndTypeManager;

    public CombineApproxDistinctFunctions(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
    }

    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .matching(CombineApproxDistinctFunctions::hasMultipleApproxDistinct);

    private static boolean hasMultipleApproxDistinct(AggregationNode aggregation)
    {
        return aggregation.getAggregations().values().stream()
                .filter(agg -> agg.getCall().getDisplayName().equals(APPROX_DISTINCT)).count() > 1;
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isCombineApproxDistinctEnabled(session);
    }

    private static boolean aggregationCanMerge(AggregationNode.Aggregation aggregation1, AggregationNode.Aggregation aggregation2)
    {
        if (!aggregation1.getMask().equals(aggregation2.getMask())
                || !aggregation1.getOrderBy().equals(aggregation2.getOrderBy())
                || !aggregation1.getFilter().equals(aggregation2.getFilter())
                || aggregation1.isDistinct() != aggregation2.isDistinct()) {
            return false;
        }
        CallExpression expression1 = aggregation1.getCall();
        CallExpression expression2 = aggregation2.getCall();
        if (expression1.getArguments().size() != expression2.getArguments().size()) {
            return false;
        }
        boolean isSameType = expression1.getArguments().get(0).getType().equals(expression2.getArguments().get(0).getType());
        boolean isSameError = expression1.getArguments().size() == 1 || expression1.getArguments().get(1).equals(expression2.getArguments().get(1));
        return isSameType && isSameError;
    }

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

    private CallExpression createArrayExpression(List<AggregationNode.Aggregation> aggregations)
    {
        List<RowExpression> expressions = aggregations.stream()
                .map(x -> x.getCall().getArguments().get(0))
                .collect(Collectors.toList());

        return call(
                functionAndTypeManager,
                ARRAY_CONSTRUCTOR,
                new ArrayType(expressions.get(0).getType()),
                expressions);
    }

    private AggregationNode.Aggregation createSetAggAggregation(List<AggregationNode.Aggregation> candidateList, VariableReferenceExpression arrayVariableReference)
    {
        AggregationNode.Aggregation aggregationBeforeMerge = candidateList.get(0);
        Type elementType = aggregationBeforeMerge.getCall().getArguments().get(0).getType();
        Type arrayType = new ArrayType(elementType);
        Type setType = new ArrayType(arrayType);

        CallExpression setAggCall = call(
                functionAndTypeManager,
                SET_AGG,
                setType,
                ImmutableList.of(arrayVariableReference));

        return new AggregationNode.Aggregation(
                setAggCall,
                aggregationBeforeMerge.getFilter(),
                aggregationBeforeMerge.getOrderBy(),
                aggregationBeforeMerge.isDistinct(),
                aggregationBeforeMerge.getMask());
    }

    @Override
    public Result apply(AggregationNode aggregationNode, Captures captures, Context context)
    {
        ImmutableMap.Builder<VariableReferenceExpression, AggregationNode.Aggregation> aggregations = ImmutableMap.builder();

        List<AggregationNode.Aggregation> approxDistinct = aggregationNode.getAggregations().values().stream().filter(
                x -> x.getCall().getDisplayName().equals(APPROX_DISTINCT)).collect(Collectors.toList());

        Map<AggregationNode.Aggregation, Long> aggregationOccurrences = approxDistinct.stream().collect(Collectors.groupingBy(identity(), Collectors.counting()));
        ImmutableList<AggregationNode.Aggregation> candidateApproxDistinct = approxDistinct.stream().filter(x -> aggregationOccurrences.get(x) == 1).collect(toImmutableList());

        Map<Type, List<AggregationNode.Aggregation>> sameTypeAggregations =
                candidateApproxDistinct.stream().collect(Collectors.groupingBy(
                        x -> x.getCall().getArguments().get(0).getType(), LinkedHashMap::new, Collectors.toList()));

        ImmutableList.Builder<List<AggregationNode.Aggregation>> candidateLists = ImmutableList.builder();
        sameTypeAggregations.values().forEach(aggregationList -> {
            candidateLists.addAll(createMergeableAggregations(aggregationList));
        });

        List<List<AggregationNode.Aggregation>> candidateAggregationLists =
                candidateLists.build().stream().filter(x -> x.size() > 1).collect(Collectors.toList());

        if (candidateAggregationLists.isEmpty()) {
            return Result.empty();
        }

        Set<AggregationNode.Aggregation> combinedAggregations = candidateAggregationLists.stream().flatMap(List::stream).collect(Collectors.toSet());
        Map<AggregationNode.Aggregation, VariableReferenceExpression> aggregationVariableMap = new HashMap<>();
        Set<VariableReferenceExpression> combinedVariableReference = new HashSet<>();
        aggregationNode.getAggregations().forEach((variable, aggregation) -> {
            if (combinedAggregations.contains(aggregation)) {
                aggregationVariableMap.put(aggregation, variable);
                combinedVariableReference.add(variable);
            }
        });

        Assignments.Builder sourceProjectAssignments = Assignments.builder();
        Assignments.Builder intermediateProjectAssignments = Assignments.builder();
        Assignments.Builder outputProjectAssignments = Assignments.builder();

        for (List<AggregationNode.Aggregation> candidateList : candidateAggregationLists) {
            RowExpression arrayExpression = createArrayExpression(candidateList);
            VariableReferenceExpression arrayVariableReference = context.getVariableAllocator().newVariable(arrayExpression);
            sourceProjectAssignments.put(arrayVariableReference, arrayExpression);

            AggregationNode.Aggregation newAggregation = createSetAggAggregation(candidateList, arrayVariableReference);
            VariableReferenceExpression setAggVariableReference = context.getVariableAllocator().newVariable(newAggregation.getCall());
            aggregations.put(setAggVariableReference, newAggregation);

            Type elementType = candidateList.get(0).getCall().getArguments().get(0).getType();
            Type arrayType = new ArrayType(elementType);
            Type arrayArrayType = new ArrayType(arrayType);

            CallExpression transposeCall = call(
                    functionAndTypeManager,
                    ARRAY_TRANSPOSE,
                    arrayArrayType,
                    ImmutableList.of(setAggVariableReference));

            VariableReferenceExpression transposeVariableReference = context.getVariableAllocator().newVariable(transposeCall);
            intermediateProjectAssignments.put(transposeVariableReference, transposeCall);

            Map<VariableReferenceExpression, RowExpression> elementAtMap =
                    IntStream.range(0, candidateList.size()).boxed().collect(ImmutableMap.toImmutableMap(
                            x -> aggregationVariableMap.get(candidateList.get(x)),
                            x -> {
                                CallExpression elementAt = call(
                                        functionAndTypeManager,
                                        ELEMENT_AT,
                                        arrayType,
                                        ImmutableList.of(transposeVariableReference, constant((long) x + 1, BIGINT)));
                                CallExpression removeNullsCall = call(
                                        functionAndTypeManager,
                                        REMOVE_NULLS,
                                        arrayType,
                                        ImmutableList.of(elementAt));
                                CallExpression arrayDistinctCall = call(
                                        functionAndTypeManager,
                                        ARRAY_DISTINCT,
                                        arrayType,
                                        ImmutableList.of(removeNullsCall));
                                CallExpression cardinalityCall = call(
                                        functionAndTypeManager,
                                        CARDINALITY,
                                        BIGINT,
                                        ImmutableList.of(arrayDistinctCall));
                                return new SpecialFormExpression(
                                        COALESCE,
                                        BIGINT,
                                        ImmutableList.of(cardinalityCall, constant(0L, BIGINT)));
                            }));
            outputProjectAssignments.putAll(elementAtMap);
        }

        aggregationNode.getAggregations().forEach((key, value) -> {
            if (!combinedVariableReference.contains(key)) {
                aggregations.put(key, value);
            }
        });

        aggregationNode.getOutputVariables().forEach(variable -> {
            if (!combinedVariableReference.contains(variable)) {
                outputProjectAssignments.put(variable, variable);
                intermediateProjectAssignments.put(variable, variable);
            }
        });

        aggregationNode.getSource().getOutputVariables().forEach(variable -> sourceProjectAssignments.put(variable, variable));

        AggregationNode newAggregationNode = new AggregationNode(
                aggregationNode.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                new ProjectNode(context.getIdAllocator().getNextId(),
                        aggregationNode.getSource(), sourceProjectAssignments.build()),
                aggregations.build(),
                aggregationNode.getGroupingSets(),
                aggregationNode.getPreGroupedVariables(),
                aggregationNode.getStep(),
                aggregationNode.getHashVariable(),
                aggregationNode.getGroupIdVariable(),
                aggregationNode.getAggregationId());

        ProjectNode intermediateProjectNode = new ProjectNode(
                context.getIdAllocator().getNextId(),
                newAggregationNode,
                intermediateProjectAssignments.build());

        return Result.ofPlanNode(
                new ProjectNode(context.getIdAllocator().getNextId(),
                        intermediateProjectNode,
                        outputProjectAssignments.build()));
    }
}
