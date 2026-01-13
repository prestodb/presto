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

import static com.facebook.presto.SystemSessionProperties.isMergeMaxByMinByAggregationsEnabled;
import static com.facebook.presto.common.Utils.checkState;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.plan.ProjectNode.Locality.LOCAL;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.DEREFERENCE;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.ROW_CONSTRUCTOR;
import static com.facebook.presto.sql.planner.plan.Patterns.aggregation;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class MergeMinByAggregations
        implements Rule<AggregationNode>
{
    private static final String MIN_BY = "min_by";

    private final FunctionResolution functionResolution;

    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .matching(MergeMinByAggregations::hasMultipleMinBy);
    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    private static boolean hasMultipleMinBy(AggregationNode aggregationNode)
    {
        return aggregationNode.getAggregations().values().stream()
                .filter(agg -> agg.getCall().getDisplayName().equals(MIN_BY)).count() > 1;
    }

    public MergeMinByAggregations(FunctionAndTypeManager functionAndTypeManager)
    {
        requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
        this.functionResolution = new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver());
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isMergeMaxByMinByAggregationsEnabled(session);
    }

    @Override
    public Result apply(AggregationNode node, Captures captures, Context context)
    {
        Map<VariableReferenceExpression, AggregationNode.Aggregation> minByAggregations = node.getAggregations().entrySet().stream()
                .filter(entry -> isMinByAggregationWithTwoArgs(entry.getValue()))
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (first, second) -> first,
                        LinkedHashMap::new));

        if (minByAggregations.size() < 2) {
            return Result.empty();
        }

        Map<MinByKey, List<Map.Entry<VariableReferenceExpression, AggregationNode.Aggregation>>> groupedKey = minByAggregations.entrySet().stream()
                .collect(Collectors.groupingBy(
                        entry -> new MinByKey(entry.getValue()),
                        LinkedHashMap::new,
                        Collectors.toList()));

        List<List<Map.Entry<VariableReferenceExpression, AggregationNode.Aggregation>>> mergeableGroups = groupedKey.values().stream()
                .filter(group -> group.size() >= 2)
                .collect(toImmutableList());

        if (mergeableGroups.isEmpty()) {
            return Result.empty();
        }

        Map<VariableReferenceExpression, AggregationNode.Aggregation> newAggregations = new LinkedHashMap<>(node.getAggregations());
        Assignments.Builder topProjectionsBuilder = Assignments.builder();
        Assignments.Builder bottomProjectionsBuilder = Assignments.builder();

        Set<VariableReferenceExpression> mergedVariables = new HashSet<>();
        Map<VariableReferenceExpression, RowExpression> originalToProjection = new LinkedHashMap<>();

        for (VariableReferenceExpression sourceVar : node.getSource().getOutputVariables()) {
            bottomProjectionsBuilder.put(sourceVar, sourceVar);
        }

        for (List<Map.Entry<VariableReferenceExpression, AggregationNode.Aggregation>> group : mergeableGroups) {
            for (Map.Entry<VariableReferenceExpression, AggregationNode.Aggregation> entry : group) {
                newAggregations.remove(entry.getKey());
            }

            AggregationNode.Aggregation firstAggregation = group.get(0).getValue();
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

            FunctionHandle minByFunctionHandle = functionResolution.lookupBuiltInFunction("min_by", ImmutableList.of(rowType, comparisonKey.getType()));
            CallExpression mergedMinByCall = new CallExpression(
                    "min_by",
                    minByFunctionHandle,
                    rowType,
                    ImmutableList.of(rowVariable, comparisonKey));

            VariableReferenceExpression mergedVariable = context.getVariableAllocator().newVariable(mergedMinByCall);
            mergedVariables.add(mergedVariable);

            AggregationNode.Aggregation mergedAggregation = new AggregationNode.Aggregation(
                    mergedMinByCall,
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

    private boolean isMinByAggregationWithTwoArgs(AggregationNode.Aggregation aggregation)
    {
        if (aggregation.getCall() == null) {
            return false;
        }

        CallExpression call = aggregation.getCall();

        if (!functionResolution.isMinByFunction(call.getFunctionHandle())) {
            return false;
        }

        return call.getArguments().size() == 2;
    }

    private static class MinByKey
    {
        private final RowExpression comparisionKey;
        private final Optional<RowExpression> filter;
        private final Optional<VariableReferenceExpression> mask;
        private final boolean isDistinct;

        public MinByKey(AggregationNode.Aggregation aggregation)
        {
            checkState(aggregation.getCall() instanceof CallExpression, "Expected CallExpression");
            CallExpression call = aggregation.getCall();
            checkState(call.getArguments().size() == 2, "MIN_BY should have exactly 2 arguments");

            this.comparisionKey = call.getArguments().get(1);
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

            MinByKey minByKey = (MinByKey) o;

            if (isDistinct != minByKey.isDistinct) {
                return false;
            }
            if (!comparisionKey.equals(minByKey.comparisionKey)) {
                return false;
            }
            if (!filter.equals(minByKey.filter)) {
                return false;
            }
            return mask.equals(minByKey.mask);
        }

        @Override
        public int hashCode()
        {
            int result = comparisionKey.hashCode();
            result = 31 * result + filter.hashCode();
            result = 31 * result + mask.hashCode();
            result = 31 * result + (isDistinct ? 1 : 0);
            return result;
        }
    }
}
