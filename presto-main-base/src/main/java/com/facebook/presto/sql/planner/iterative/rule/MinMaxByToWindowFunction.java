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
import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.DataOrganizationSpecification;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.Ordering;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.TopNRowNumberNode;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.isRewriteMinMaxByToTopNEnabled;
import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.identityAssignments;
import static com.facebook.presto.sql.planner.plan.Patterns.aggregation;
import static com.facebook.presto.sql.relational.Expressions.comparisonExpression;
import static com.google.common.collect.ImmutableMap.toImmutableMap;

/**
 * For queries with min_by/max_by functions on map, rewrite it with top n window functions.
 * For example, for query `select id, max(ds), max_by(feature, ds) from t group by id`,
 * it will be rewritten from:
 * <pre>
 * - Aggregation
 *      ds_0 := max(ds)
 *      feature_0 := max_by(feature, ds)
 *      group by id
 *      - scan t
 *          ds
 *          feature
 *          id
 * </pre>
 * into:
 * <pre>
 *     - Filter
 *          row_num = 1
 *          - TopNRow
 *              partition by id
 *              order by ds desc
 *              maxRowCountPerPartition = 1
 *              - scan t
 *                  ds
 *                  feature
 *                  id
 * </pre>
 */
public class MinMaxByToWindowFunction
        implements Rule<AggregationNode>
{
    private static final Pattern<AggregationNode> PATTERN = aggregation().matching(x -> !x.getHashVariable().isPresent() && !x.getGroupingKeys().isEmpty() && x.getGroupingSetCount() == 1 && x.getStep().equals(AggregationNode.Step.SINGLE));
    private final FunctionResolution functionResolution;

    public MinMaxByToWindowFunction(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionResolution = new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver());
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isRewriteMinMaxByToTopNEnabled(session);
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(AggregationNode node, Captures captures, Context context)
    {
        Map<VariableReferenceExpression, AggregationNode.Aggregation> maxByAggregations = node.getAggregations().entrySet().stream()
                .filter(x -> functionResolution.isMaxByFunction(x.getValue().getFunctionHandle()))
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
        Map<VariableReferenceExpression, AggregationNode.Aggregation> minByAggregations = node.getAggregations().entrySet().stream()
                .filter(x -> functionResolution.isMinByFunction(x.getValue().getFunctionHandle()))
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
        boolean isMaxByAggregation;
        Map<VariableReferenceExpression, AggregationNode.Aggregation> candidateAggregation;
        if (maxByAggregations.isEmpty() && !minByAggregations.isEmpty()) {
            isMaxByAggregation = false;
            candidateAggregation = minByAggregations;
        }
        else if (!maxByAggregations.isEmpty() && minByAggregations.isEmpty()) {
            isMaxByAggregation = true;
            candidateAggregation = maxByAggregations;
        }
        else {
            return Result.empty();
        }
        if (candidateAggregation.values().stream().noneMatch(x -> x.getArguments().get(0).getType() instanceof MapType || x.getArguments().get(0).getType() instanceof ArrayType)) {
            return Result.empty();
        }
        boolean allMaxOrMinByWithSameField = candidateAggregation.values().stream().map(x -> x.getArguments().get(1)).distinct().count() == 1;
        if (!allMaxOrMinByWithSameField) {
            return Result.empty();
        }
        VariableReferenceExpression orderByVariable = (VariableReferenceExpression) candidateAggregation.values().stream().findFirst().get().getArguments().get(1);
        Map<VariableReferenceExpression, AggregationNode.Aggregation> remainingAggregations = node.getAggregations().entrySet().stream().filter(x -> !candidateAggregation.containsKey(x.getKey()))
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
        boolean remainingEmptyOrMinOrMaxOnOrderBy = remainingAggregations.isEmpty() || (remainingAggregations.size() == 1
                && remainingAggregations.values().stream().allMatch(x -> (isMaxByAggregation ? functionResolution.isMaxFunction(x.getFunctionHandle()) : functionResolution.isMinFunction(x.getFunctionHandle())) && x.getArguments().size() == 1 && x.getArguments().get(0).equals(orderByVariable)));
        if (!remainingEmptyOrMinOrMaxOnOrderBy) {
            return Result.empty();
        }

        List<VariableReferenceExpression> partitionKeys = node.getGroupingKeys();
        OrderingScheme orderingScheme = new OrderingScheme(ImmutableList.of(new Ordering(orderByVariable, isMaxByAggregation ? SortOrder.DESC_NULLS_LAST : SortOrder.ASC_NULLS_LAST)));
        DataOrganizationSpecification dataOrganizationSpecification = new DataOrganizationSpecification(partitionKeys, Optional.of(orderingScheme));
        VariableReferenceExpression rowNumberVariable = context.getVariableAllocator().newVariable("row_number", BIGINT);
        TopNRowNumberNode topNRowNumberNode =
                new TopNRowNumberNode(node.getSourceLocation(),
                        context.getIdAllocator().getNextId(),
                        node.getStatsEquivalentPlanNode(),
                        node.getSource(),
                        dataOrganizationSpecification,
                        TopNRowNumberNode.RankingFunction.ROW_NUMBER,
                        rowNumberVariable,
                        1,
                        false,
                        Optional.empty());
        RowExpression equal = comparisonExpression(functionResolution, EQUAL, rowNumberVariable, new ConstantExpression(1L, BIGINT));
        FilterNode filterNode = new FilterNode(node.getSourceLocation(), context.getIdAllocator().getNextId(), node.getStatsEquivalentPlanNode(), topNRowNumberNode, equal);
        Map<VariableReferenceExpression, RowExpression> assignments = ImmutableMap.<VariableReferenceExpression, RowExpression>builder()
                .putAll(node.getAggregations().entrySet().stream().collect(toImmutableMap(Map.Entry::getKey, x -> x.getValue().getArguments().get(0)))).build();

        ProjectNode projectNode = new ProjectNode(node.getSourceLocation(), context.getIdAllocator().getNextId(), node.getStatsEquivalentPlanNode(), filterNode,
                Assignments.builder().putAll(assignments).putAll(identityAssignments(node.getGroupingKeys())).build(), ProjectNode.Locality.LOCAL);
        return Result.ofPlanNode(projectNode);
    }
}
