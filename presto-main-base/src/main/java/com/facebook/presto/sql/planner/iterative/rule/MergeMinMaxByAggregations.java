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
import com.facebook.presto.spi.plan.OrderingScheme;
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
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.SystemSessionProperties.isMergeMaxByMinByAggregationsEnabled;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.plan.ProjectNode.Locality.LOCAL;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.DEREFERENCE;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.ROW_CONSTRUCTOR;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.planner.plan.Patterns.aggregation;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * Merges multiple MAX_BY or MIN_BY aggregations with the same comparison key
 * into a single aggregation with a ROW argument and extracts individual fields
 * from the result.
 * <p>
 * For example:
 * <pre>
 * SELECT max_by(v1, k), max_by(v2, k), max_by(v3, k) FROM t
 * </pre>
 * is transformed to:
 * <pre>
 * SELECT merged[1], merged[2], merged[3]
 * FROM (SELECT max_by(ROW(v1, v2, v3), k) AS merged FROM t)
 * </pre>
 * <p>
 * The same transformation applies to MIN_BY aggregations.
 */
public class MergeMinMaxByAggregations
        implements Rule<AggregationNode>
{
    private final FunctionResolution functionResolution;
    private final FunctionAndTypeManager functionAndTypeManager;
    private final Pattern<AggregationNode> pattern;

    public MergeMinMaxByAggregations(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
        this.functionResolution = new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver());
        this.pattern = aggregation()
                .matching(this::hasMultipleMergeableAggregations);
    }

    private boolean hasMultipleMergeableAggregations(AggregationNode aggregationNode)
    {
        long maxByCount = aggregationNode.getAggregations().values().stream()
                .filter(agg -> agg.getCall().getArguments().size() == 2
                        && functionResolution.isMaxByFunction(agg.getCall().getFunctionHandle()))
                .count();
        if (maxByCount > 1) {
            return true;
        }
        long minByCount = aggregationNode.getAggregations().values().stream()
                .filter(agg -> agg.getCall().getArguments().size() == 2
                        && functionResolution.isMinByFunction(agg.getCall().getFunctionHandle()))
                .count();
        return minByCount > 1;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isMergeMaxByMinByAggregationsEnabled(session);
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return pattern;
    }

    @Override
    public Result apply(AggregationNode node, Captures captures, Context context)
    {
        // Collect max_by and min_by aggregations with exactly 2 arguments
        Map<VariableReferenceExpression, Aggregation> maxByAggs = new LinkedHashMap<>();
        Map<VariableReferenceExpression, Aggregation> minByAggs = new LinkedHashMap<>();

        for (Map.Entry<VariableReferenceExpression, Aggregation> entry : node.getAggregations().entrySet()) {
            Aggregation agg = entry.getValue();
            if (agg.getCall().getArguments().size() == 2) {
                if (functionResolution.isMaxByFunction(agg.getCall().getFunctionHandle())) {
                    maxByAggs.put(entry.getKey(), agg);
                }
                else if (functionResolution.isMinByFunction(agg.getCall().getFunctionHandle())) {
                    minByAggs.put(entry.getKey(), agg);
                }
            }
        }

        // Group by comparison key (including filter, mask, distinct, orderBy)
        List<List<Map.Entry<VariableReferenceExpression, Aggregation>>> maxByGroups = findMergeableGroups(maxByAggs);
        List<List<Map.Entry<VariableReferenceExpression, Aggregation>>> minByGroups = findMergeableGroups(minByAggs);

        if (maxByGroups.isEmpty() && minByGroups.isEmpty()) {
            return Result.empty();
        }

        Map<VariableReferenceExpression, Aggregation> newAggregations = new LinkedHashMap<>(node.getAggregations());
        Assignments.Builder topProjectionsBuilder = Assignments.builder();
        Assignments.Builder bottomProjectionsBuilder = Assignments.builder();
        Map<VariableReferenceExpression, RowExpression> originalToProjection = new LinkedHashMap<>();
        Set<VariableReferenceExpression> mergedVariables = new HashSet<>();

        // Pass through all source variables
        for (VariableReferenceExpression sourceVar : node.getSource().getOutputVariables()) {
            bottomProjectionsBuilder.put(sourceVar, sourceVar);
        }

        // Process max_by groups
        for (List<Map.Entry<VariableReferenceExpression, Aggregation>> group : maxByGroups) {
            mergeGroup(group, "max_by", newAggregations, bottomProjectionsBuilder,
                    originalToProjection, mergedVariables, context);
        }

        // Process min_by groups
        for (List<Map.Entry<VariableReferenceExpression, Aggregation>> group : minByGroups) {
            mergeGroup(group, "min_by", newAggregations, bottomProjectionsBuilder,
                    originalToProjection, mergedVariables, context);
        }

        // Build top projection: dereferences for merged vars, identity for others
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

    private List<List<Map.Entry<VariableReferenceExpression, Aggregation>>> findMergeableGroups(
            Map<VariableReferenceExpression, Aggregation> aggregations)
    {
        if (aggregations.size() < 2) {
            return ImmutableList.of();
        }

        return aggregations.entrySet().stream()
                .collect(Collectors.groupingBy(
                        entry -> new AggregationKey(entry.getValue()),
                        LinkedHashMap::new,
                        Collectors.toList()))
                .values().stream()
                .filter(group -> group.size() >= 2)
                .collect(toImmutableList());
    }

    private void mergeGroup(
            List<Map.Entry<VariableReferenceExpression, Aggregation>> group,
            String functionName,
            Map<VariableReferenceExpression, Aggregation> newAggregations,
            Assignments.Builder bottomProjectionsBuilder,
            Map<VariableReferenceExpression, RowExpression> originalToProjection,
            Set<VariableReferenceExpression> mergedVariables,
            Context context)
    {
        // Remove individual aggregations
        for (Map.Entry<VariableReferenceExpression, Aggregation> entry : group) {
            newAggregations.remove(entry.getKey());
        }

        Aggregation firstAggregation = group.get(0).getValue();
        RowExpression comparisonKey = firstAggregation.getCall().getArguments().get(1);

        // Build ROW(v1, v2, ...) expression
        List<RowExpression> valueExpressions = group.stream()
                .map(entry -> entry.getValue().getCall().getArguments().get(0))
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

        // Build merged aggregation: max_by(ROW(...), key) or min_by(ROW(...), key)
        FunctionHandle functionHandle = functionAndTypeManager.getFunctionAndTypeResolver()
                .lookupFunction(functionName, fromTypes(rowType, comparisonKey.getType()));
        CallExpression mergedCall = new CallExpression(
                functionName,
                functionHandle,
                rowType,
                ImmutableList.of(rowVariable, comparisonKey));

        VariableReferenceExpression mergedVariable = context.getVariableAllocator().newVariable(mergedCall);
        mergedVariables.add(mergedVariable);

        Aggregation mergedAggregation = new Aggregation(
                mergedCall,
                firstAggregation.getFilter(),
                firstAggregation.getOrderBy(),
                firstAggregation.isDistinct(),
                firstAggregation.getMask());
        newAggregations.put(mergedVariable, mergedAggregation);

        // Build dereference projections: merged[0], merged[1], ...
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

    /**
     * Groups aggregations by comparison key, filter, mask, distinct, and orderBy.
     * Two aggregations can only be merged if all of these match.
     */
    private static class AggregationKey
    {
        private final RowExpression comparisonKey;
        private final Optional<RowExpression> filter;
        private final Optional<VariableReferenceExpression> mask;
        private final boolean isDistinct;
        private final Optional<OrderingScheme> orderBy;

        public AggregationKey(Aggregation aggregation)
        {
            CallExpression call = aggregation.getCall();
            this.comparisonKey = call.getArguments().get(1);
            this.filter = aggregation.getFilter();
            this.mask = aggregation.getMask();
            this.isDistinct = aggregation.isDistinct();
            this.orderBy = aggregation.getOrderBy();
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
            AggregationKey that = (AggregationKey) o;
            return isDistinct == that.isDistinct &&
                    comparisonKey.equals(that.comparisonKey) &&
                    filter.equals(that.filter) &&
                    mask.equals(that.mask) &&
                    orderBy.equals(that.orderBy);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(comparisonKey, filter, mask, isDistinct, orderBy);
        }
    }
}
