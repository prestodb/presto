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
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.Ordering;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.shouldPushAggregationThroughJoin;
import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.spi.plan.AggregationNode.globalAggregation;
import static com.facebook.presto.spi.plan.AggregationNode.singleGroupingSet;
import static com.facebook.presto.sql.planner.RowExpressionVariableInliner.inlineVariables;
import static com.facebook.presto.sql.planner.optimizations.DistinctOutputQueryUtil.isDistinct;
import static com.facebook.presto.sql.planner.plan.Patterns.aggregation;
import static com.facebook.presto.sql.planner.plan.Patterns.join;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.facebook.presto.sql.relational.Expressions.constantNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * This optimizer pushes aggregations below outer joins when: the aggregation
 * is on top of the outer join, it groups by all columns in the outer table, and
 * the outer rows are guaranteed to be distinct.
 * <p>
 * When the aggregation is pushed down, we still need to perform aggregations
 * on the null values that come out of the absent values in an outer
 * join. We add a cross join with a row of aggregations on null literals,
 * and coalesce the aggregation that results from the left outer join with
 * the result of the aggregation over nulls.
 * <p>
 * Example:
 * <pre>
 * - Filter ("nationkey" > "avg")
 *  - Aggregate(Group by: all columns from the left table, aggregation:
 *    avg("n2.nationkey"))
 *      - LeftJoin("regionkey" = "regionkey")
 *          - AssignUniqueId (nation)
 *              - Tablescan (nation)
 *          - Tablescan (nation)
 * </pre>
 * </p>
 * Is rewritten to:
 * <pre>
 * - Filter ("nationkey" > "avg")
 *  - project(regionkey, coalesce("avg", "avg_over_null")
 *      - CrossJoin
 *          - LeftJoin("regionkey" = "regionkey")
 *              - AssignUniqueId (nation)
 *                  - Tablescan (nation)
 *              - Aggregate(Group by: regionkey, aggregation:
 *                avg(nationkey))
 *                  - Tablescan (nation)
 *          - Aggregate
 *            avg(null_literal)
 *              - Values (null_literal)
 * </pre>
 */
public class PushAggregationThroughOuterJoin
        implements Rule<AggregationNode>
{
    private static final Capture<JoinNode> JOIN = newCapture();

    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .with(source().matching(join().capturedAs(JOIN)));
    private final FunctionAndTypeManager functionAndTypeManager;

    public PushAggregationThroughOuterJoin(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionManager is null");
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return shouldPushAggregationThroughJoin(session);
    }

    @Override
    public Result apply(AggregationNode aggregation, Captures captures, Context context)
    {
        JoinNode join = captures.get(JOIN);

        if (join.getFilter().isPresent()
                || !(join.getType() == JoinNode.Type.LEFT || join.getType() == JoinNode.Type.RIGHT)
                || !groupsOnAllColumns(aggregation, getOuterTable(join).getOutputVariables())
                || !isDistinct(context.getLookup().resolve(getOuterTable(join)), context.getLookup()::resolve)) {
            return Result.empty();
        }

        List<VariableReferenceExpression> groupingKeys = join.getCriteria().stream()
                .map(join.getType() == JoinNode.Type.RIGHT ? JoinNode.EquiJoinClause::getLeft : JoinNode.EquiJoinClause::getRight)
                .collect(toImmutableList());
        AggregationNode rewrittenAggregation = new AggregationNode(
                aggregation.getSourceLocation(),
                aggregation.getId(),
                getInnerTable(join),
                aggregation.getAggregations(),
                singleGroupingSet(groupingKeys),
                ImmutableList.of(),
                aggregation.getStep(),
                aggregation.getHashVariable(),
                aggregation.getGroupIdVariable());

        JoinNode rewrittenJoin;
        if (join.getType() == JoinNode.Type.LEFT) {
            rewrittenJoin = new JoinNode(
                    join.getSourceLocation(),
                    join.getId(),
                    join.getType(),
                    join.getLeft(),
                    rewrittenAggregation,
                    join.getCriteria(),
                    ImmutableList.<VariableReferenceExpression>builder()
                            .addAll(join.getLeft().getOutputVariables())
                            .addAll(rewrittenAggregation.getAggregations().keySet())
                            .build(),
                    join.getFilter(),
                    join.getLeftHashVariable(),
                    join.getRightHashVariable(),
                    join.getDistributionType(),
                    join.getDynamicFilters());
        }
        else {
            rewrittenJoin = new JoinNode(
                    join.getSourceLocation(),
                    join.getId(),
                    join.getType(),
                    rewrittenAggregation,
                    join.getRight(),
                    join.getCriteria(),
                    ImmutableList.<VariableReferenceExpression>builder()
                            .addAll(rewrittenAggregation.getAggregations().keySet())
                            .addAll(join.getRight().getOutputVariables())
                            .build(),
                    join.getFilter(),
                    join.getLeftHashVariable(),
                    join.getRightHashVariable(),
                    join.getDistributionType(),
                    join.getDynamicFilters());
        }

        Optional<PlanNode> resultNode = coalesceWithNullAggregation(rewrittenAggregation, rewrittenJoin, context.getVariableAllocator(), context.getIdAllocator(), context.getLookup());
        if (!resultNode.isPresent()) {
            return Result.empty();
        }

        return Result.ofPlanNode(resultNode.get());
    }

    private static PlanNode getInnerTable(JoinNode join)
    {
        checkState(join.getType() == JoinNode.Type.LEFT || join.getType() == JoinNode.Type.RIGHT, "expected LEFT or RIGHT JOIN");
        PlanNode innerNode;
        if (join.getType().equals(JoinNode.Type.LEFT)) {
            innerNode = join.getRight();
        }
        else {
            innerNode = join.getLeft();
        }
        return innerNode;
    }

    private static PlanNode getOuterTable(JoinNode join)
    {
        checkState(join.getType() == JoinNode.Type.LEFT || join.getType() == JoinNode.Type.RIGHT, "expected LEFT or RIGHT JOIN");
        PlanNode outerNode;
        if (join.getType().equals(JoinNode.Type.LEFT)) {
            outerNode = join.getLeft();
        }
        else {
            outerNode = join.getRight();
        }
        return outerNode;
    }

    private static boolean groupsOnAllColumns(AggregationNode node, List<VariableReferenceExpression> columns)
    {
        return new HashSet<>(node.getGroupingKeys()).equals(new HashSet<>(columns));
    }

    // When the aggregation is done after the join, there will be a null value that gets aggregated over
    // where rows did not exist in the inner table.  For some aggregate functions, such as count, the result
    // of an aggregation over a single null row is one or zero rather than null. In order to ensure correct results,
    // we add a coalesce function with the output of the new outer join and the aggregation performed over a single
    // null row.
    private Optional<PlanNode> coalesceWithNullAggregation(AggregationNode aggregationNode, PlanNode outerJoin, PlanVariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, Lookup lookup)
    {
        // Create an aggregation node over a row of nulls.
        Optional<MappedAggregationInfo> aggregationOverNullInfoResultNode = createAggregationOverNull(
                aggregationNode,
                variableAllocator,
                idAllocator,
                lookup);

        if (!aggregationOverNullInfoResultNode.isPresent()) {
            return Optional.empty();
        }

        MappedAggregationInfo aggregationOverNullInfo = aggregationOverNullInfoResultNode.get();

        AggregationNode aggregationOverNull = aggregationOverNullInfo.getAggregation();
        Map<VariableReferenceExpression, VariableReferenceExpression> sourceAggregationToOverNullMapping = aggregationOverNullInfo.getVariableMapping();

        // Do a cross join with the aggregation over null
        JoinNode crossJoin = new JoinNode(
                outerJoin.getSourceLocation(),
                idAllocator.getNextId(),
                JoinNode.Type.INNER,
                outerJoin,
                aggregationOverNull,
                ImmutableList.of(),
                ImmutableList.<VariableReferenceExpression>builder()
                        .addAll(outerJoin.getOutputVariables())
                        .addAll(aggregationOverNull.getOutputVariables())
                        .build(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of());

        // Add coalesce expressions for all aggregation functions
        Assignments.Builder assignmentsBuilder = Assignments.builder();
        for (VariableReferenceExpression variable : outerJoin.getOutputVariables()) {
            if (aggregationNode.getAggregations().keySet().contains(variable)) {
                assignmentsBuilder.put(variable, coalesce(ImmutableList.of(variable, sourceAggregationToOverNullMapping.get(variable))));
            }
            else {
                assignmentsBuilder.put(variable, variable);
            }
        }
        return Optional.of(new ProjectNode(idAllocator.getNextId(), crossJoin, assignmentsBuilder.build()));
    }

    private static RowExpression coalesce(List<RowExpression> expressions)
    {
        return new SpecialFormExpression(SpecialFormExpression.Form.COALESCE, expressions.get(0).getType(), expressions);
    }

    private Optional<MappedAggregationInfo> createAggregationOverNull(AggregationNode referenceAggregation, PlanVariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, Lookup lookup)
    {
        // Create a values node that consists of a single row of nulls.
        // Map the output symbols from the referenceAggregation's source
        // to symbol references for the new values node.
        ImmutableList.Builder<VariableReferenceExpression> nullVariables = ImmutableList.builder();
        ImmutableList.Builder<RowExpression> nullLiterals = ImmutableList.builder();
        ImmutableMap.Builder<VariableReferenceExpression, VariableReferenceExpression> sourcesVariableMappingBuilder = ImmutableMap.builder();
        for (VariableReferenceExpression sourceVariable : referenceAggregation.getSource().getOutputVariables()) {
            RowExpression nullLiteral = constantNull(sourceVariable.getSourceLocation(), sourceVariable.getType());
            nullLiterals.add(nullLiteral);
            VariableReferenceExpression nullVariable = variableAllocator.newVariable(nullLiteral);
            nullVariables.add(nullVariable);
            // TODO The type should be from sourceVariable.getType
            sourcesVariableMappingBuilder.put(sourceVariable, nullVariable);
        }
        ValuesNode nullRow = new ValuesNode(
                referenceAggregation.getSourceLocation(),
                idAllocator.getNextId(),
                nullVariables.build(),
                ImmutableList.of(nullLiterals.build()),
                Optional.empty());
        Map<VariableReferenceExpression, VariableReferenceExpression> sourcesVariableMapping = sourcesVariableMappingBuilder.build();

        // For each aggregation function in the reference node, create a corresponding aggregation function
        // that points to the nullRow. Map the symbols from the aggregations in referenceAggregation to the
        // symbols in these new aggregations.
        ImmutableMap.Builder<VariableReferenceExpression, VariableReferenceExpression> aggregationsVariableMappingBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<VariableReferenceExpression, AggregationNode.Aggregation> aggregationsOverNullBuilder = ImmutableMap.builder();
        for (Map.Entry<VariableReferenceExpression, AggregationNode.Aggregation> entry : referenceAggregation.getAggregations().entrySet()) {
            VariableReferenceExpression aggregationVariable = entry.getKey();
            AggregationNode.Aggregation aggregation = entry.getValue();

            if (!isUsingVariables(aggregation, sourcesVariableMapping.keySet())) {
                return Optional.empty();
            }

            AggregationNode.Aggregation overNullAggregation = new AggregationNode.Aggregation(
                    new CallExpression(
                            aggregation.getCall().getSourceLocation(),
                            aggregation.getCall().getDisplayName(),
                            aggregation.getCall().getFunctionHandle(),
                            aggregation.getCall().getType(),
                            aggregation.getArguments()
                                    .stream()
                                    .map(argument -> inlineVariables(sourcesVariableMapping, argument))
                                    .collect(toImmutableList())),
                    aggregation.getFilter().map(filter -> inlineVariables(sourcesVariableMapping, filter)),
                    aggregation.getOrderBy().map(orderBy -> inlineOrderByVariables(sourcesVariableMapping, orderBy)),
                    aggregation.isDistinct(),
                    aggregation.getMask().map(x -> new VariableReferenceExpression(sourcesVariableMapping.get(x).getSourceLocation(), sourcesVariableMapping.get(x).getName(), x.getType())));
            QualifiedObjectName functionName = functionAndTypeManager.getFunctionMetadata(overNullAggregation.getFunctionHandle()).getName();
            VariableReferenceExpression overNull = variableAllocator.newVariable(aggregation.getCall().getSourceLocation(), functionName.getObjectName(), aggregationVariable.getType());
            aggregationsOverNullBuilder.put(overNull, overNullAggregation);
            aggregationsVariableMappingBuilder.put(aggregationVariable, overNull);
        }
        Map<VariableReferenceExpression, VariableReferenceExpression> aggregationsSymbolMapping = aggregationsVariableMappingBuilder.build();

        // create an aggregation node whose source is the null row.
        AggregationNode aggregationOverNullRow = new AggregationNode(
                referenceAggregation.getSourceLocation(),
                idAllocator.getNextId(),
                nullRow,
                aggregationsOverNullBuilder.build(),
                globalAggregation(),
                ImmutableList.of(),
                AggregationNode.Step.SINGLE,
                Optional.empty(),
                Optional.empty());

        return Optional.of(new MappedAggregationInfo(aggregationOverNullRow, aggregationsSymbolMapping));
    }

    private static OrderingScheme inlineOrderByVariables(Map<VariableReferenceExpression, VariableReferenceExpression> variableMapping, OrderingScheme orderingScheme)
    {
        // This is a logic expanded from ExpressionTreeRewriter::rewriteSortItems
        ImmutableList.Builder<VariableReferenceExpression> orderBy = ImmutableList.builder();
        ImmutableMap.Builder<VariableReferenceExpression, SortOrder> ordering = new ImmutableMap.Builder<>();
        for (VariableReferenceExpression variable : orderingScheme.getOrderByVariables()) {
            VariableReferenceExpression translated = variableMapping.get(variable);
            orderBy.add(translated);
            ordering.put(translated, orderingScheme.getOrdering(variable));
        }

        ImmutableMap<VariableReferenceExpression, SortOrder> orderingMap = ordering.build();
        return new OrderingScheme(orderBy.build().stream().map(variable -> new Ordering(variable, orderingMap.get(variable))).collect(toImmutableList()));
    }

    private static boolean isUsingVariables(AggregationNode.Aggregation aggregation, Set<VariableReferenceExpression> sourceVariables)
    {
        Set<VariableReferenceExpression> inputVariables = new HashSet<>();
        for (RowExpression argument : aggregation.getArguments()) {
            if (argument instanceof VariableReferenceExpression) {
                inputVariables.add((VariableReferenceExpression) argument);
            }
        }
        return sourceVariables.stream()
                .anyMatch(inputVariables::contains);
    }

    private static class MappedAggregationInfo
    {
        private final AggregationNode aggregationNode;
        private final Map<VariableReferenceExpression, VariableReferenceExpression> variableMapping;

        public MappedAggregationInfo(AggregationNode aggregationNode, Map<VariableReferenceExpression, VariableReferenceExpression> variableMapping)
        {
            this.aggregationNode = aggregationNode;
            this.variableMapping = variableMapping;
        }

        public Map<VariableReferenceExpression, VariableReferenceExpression> getVariableMapping()
        {
            return variableMapping;
        }

        public AggregationNode getAggregation()
        {
            return aggregationNode;
        }
    }
}
