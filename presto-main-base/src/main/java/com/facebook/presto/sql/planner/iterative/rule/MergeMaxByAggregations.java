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
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.AggregationNode.Aggregation;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.common.collect.ImmutableList;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.SystemSessionProperties.isMergeMaxByAggregationsEnabled;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.plan.ProjectNode.Locality.LOCAL;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.DEREFERENCE;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.ROW_CONSTRUCTOR;
import static com.facebook.presto.sql.planner.plan.Patterns.aggregation;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * Merges multiple MAX_BY aggregations with the same comparison key into a single MAX_BY
 * with a ROW argument and extracts individual fields from the result.
 * <p>
 * For example:
 * <pre>
 * max_by(v1, k), max_by(v2, k), max_by(v3, k)
 * </pre>
 * is transformed to:
 * <pre>
 * max_by(ROW(v1, v2, v3), k)[1], max_by(ROW(v1, v2, v3), k)[2], max_by(ROW(v1, v2, v3), k)[3]
 * </pre>
 */
public class MergeMaxByAggregations
        implements Rule<AggregationNode>
{
    private static final String MAX_BY = "max_by";

    private final FunctionResolution functionResolution;

    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .matching(MergeMaxByAggregations::hasMultipleMaxBy);

    private static boolean hasMultipleMaxBy(AggregationNode aggregationNode)
    {
        return aggregationNode.getAggregations().values().stream()
                .filter(agg -> agg.getCall().getDisplayName().equals(MAX_BY)).count() > 1;
    }

    public MergeMaxByAggregations(FunctionAndTypeManager functionAndTypeManager)
    {
        requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
        this.functionResolution = new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver());
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isMergeMaxByAggregationsEnabled(session);
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(AggregationNode node, Captures captures, Context context)
    {
        Map<VariableReferenceExpression, Aggregation> maxByAggregations = node.getAggregations().entrySet().stream()
                .filter(entry -> isMaxByAggregationWithTwoArgs(entry.getValue()))
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (first, second) -> first,
                        LinkedHashMap::new));

        if (maxByAggregations.size() < 2) {
            return Result.empty();
        }

        Map<MaxByKey, List<Map.Entry<VariableReferenceExpression, Aggregation>>> groupedByKey = maxByAggregations.entrySet().stream()
                .collect(Collectors.groupingBy(
                        entry -> new MaxByKey(entry.getValue()),
                        LinkedHashMap::new,
                        Collectors.toList()));

        List<List<Map.Entry<VariableReferenceExpression, Aggregation>>> mergeableGroups = groupedByKey.values().stream()
                .filter(group -> group.size() >= 2)
                .collect(toImmutableList());

        if (mergeableGroups.isEmpty()) {
            return Result.empty();
        }

        Map<VariableReferenceExpression, Aggregation> newAggregations = new LinkedHashMap<>(node.getAggregations());
        Assignments.Builder topProjectionsBuilder = Assignments.builder();
        Assignments.Builder bottomProjectionsBuilder = Assignments.builder();

        Set<VariableReferenceExpression> mergedVariables = new HashSet<>();
        Map<VariableReferenceExpression, RowExpression> originalToProjection = new LinkedHashMap<>();

        for (VariableReferenceExpression sourceVar : node.getSource().getOutputVariables()) {
            bottomProjectionsBuilder.put(sourceVar, sourceVar);
        }

        for (List<Map.Entry<VariableReferenceExpression, Aggregation>> group : mergeableGroups) {
            for (Map.Entry<VariableReferenceExpression, Aggregation> entry : group) {
                newAggregations.remove(entry.getKey());
            }

            Aggregation firstAggregation = group.get(0).getValue();
            CallExpression firstCall = (CallExpression) firstAggregation.getCall();

            RowExpression comparisonKey = firstCall.getArguments().get(1);

            List<RowExpression> valueExpressions = group.stream()
                    .map(entry -> ((CallExpression) entry.getValue().getCall()).getArguments().get(0))
                    .collect(toImmutableList());

            List<Type> fieldTypes = valueExpressions.stream()
                    .map(RowExpression::getType)
                    .collect(toImmutableList());
            RowType rowType = RowType.anonymous(fieldTypes);

            RowExpression rowExpression = new SpecialFormExpression(
                    ROW_CONSTRUCTOR,
                    rowType,
                    valueExpressions);

            VariableReferenceExpression rowVariable = context.getVariableAllocator().newVariable("expr", rowType);
            bottomProjectionsBuilder.put(rowVariable, rowExpression);

            FunctionHandle maxByFunctionHandle = functionResolution.lookupBuiltInFunction("max_by", ImmutableList.of(rowType, comparisonKey.getType()));
            CallExpression mergedMaxByCall = new CallExpression(
                    "max_by",
                    maxByFunctionHandle,
                    rowType,
                    ImmutableList.of(rowVariable, comparisonKey));

            VariableReferenceExpression mergedVariable = context.getVariableAllocator().newVariable(mergedMaxByCall);
            mergedVariables.add(mergedVariable);

            Aggregation mergedAggregation = new Aggregation(
                    mergedMaxByCall,
                    firstAggregation.getFilter(),
                    firstAggregation.getOrderBy(),
                    firstAggregation.isDistinct(),
                    firstAggregation.getMask());
            newAggregations.put(mergedVariable, mergedAggregation);

            for (int i = 0; i < group.size(); i++) {
                VariableReferenceExpression originalVariable = group.get(i).getKey();
                Type fieldType = fieldTypes.get(i);

                SpecialFormExpression fieldAccess = new SpecialFormExpression(
                        DEREFERENCE,
                        fieldType,
                        mergedVariable,
                        constant((long) i, INTEGER));

                originalToProjection.put(originalVariable, fieldAccess);
            }
        }

        for (VariableReferenceExpression outputVariable : node.getOutputVariables()) {
            if (originalToProjection.containsKey(outputVariable)) {
                topProjectionsBuilder.put(outputVariable, originalToProjection.get(outputVariable));
            }
            else if (!mergedVariables.contains(outputVariable)) {
                topProjectionsBuilder.put(outputVariable, outputVariable);
            }
        }

        ProjectNode bottomProjectNode = new ProjectNode(
                node.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                node.getSource(),
                bottomProjectionsBuilder.build(),
                LOCAL);

        AggregationNode newAggregationNode = new AggregationNode(
                node.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                bottomProjectNode,
                newAggregations,
                node.getGroupingSets(),
                node.getPreGroupedVariables(),
                node.getStep(),
                node.getHashVariable(),
                node.getGroupIdVariable(),
                node.getAggregationId());

        ProjectNode topProjectNode = new ProjectNode(
                node.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                newAggregationNode,
                topProjectionsBuilder.build(),
                LOCAL);

        return Result.ofPlanNode(topProjectNode);
    }

    private boolean isMaxByAggregationWithTwoArgs(Aggregation aggregation)
    {
        if (!(aggregation.getCall() instanceof CallExpression)) {
            return false;
        }

        CallExpression call = (CallExpression) aggregation.getCall();

        if (!functionResolution.isMaxByFunction(call.getFunctionHandle())) {
            return false;
        }

        return call.getArguments().size() == 2;
    }

    /**
     * Key class for grouping MAX_BY aggregations by their comparison key
     */
    private static class MaxByKey
    {
        private final RowExpression comparisonKey;
        private final Optional<RowExpression> filter;
        private final Optional<VariableReferenceExpression> mask;
        private final boolean isDistinct;

        public MaxByKey(Aggregation aggregation)
        {
            checkState(aggregation.getCall() instanceof CallExpression, "Expected CallExpression");
            CallExpression call = (CallExpression) aggregation.getCall();
            checkState(call.getArguments().size() == 2, "MAX_BY should have exactly 2 arguments");

            this.comparisonKey = call.getArguments().get(1);
            this.filter = aggregation.getFilter();
            this.mask = aggregation.getMask();
            this.isDistinct = aggregation.isDistinct();
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            MaxByKey maxByKey = (MaxByKey) o;

            if (isDistinct != maxByKey.isDistinct) {
                return false;
            }
            if (!comparisonKey.equals(maxByKey.comparisonKey)) {
                return false;
            }
            if (!filter.equals(maxByKey.filter)) {
                return false;
            }
            return mask.equals(maxByKey.mask);
        }

        @Override
        public int hashCode()
        {
            int result = comparisonKey.hashCode();
            result = 31 * result + filter.hashCode();
            result = 31 * result + mask.hashCode();
            result = 31 * result + (isDistinct ? 1 : 0);
            return result;
        }
    }
}
