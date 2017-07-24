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
import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.sql.planner.ExpressionSymbolInliner;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.tree.CoalesceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.shouldPushAggregationThroughJoin;
import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.sql.planner.optimizations.DistinctOutputQueryUtil.isDistinct;
import static com.facebook.presto.sql.planner.plan.Patterns.aggregation;
import static com.facebook.presto.sql.planner.plan.Patterns.join;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;

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
                || !groupsOnAllOuterTableColumns(aggregation, context.getLookup().resolve(getOuterTable(join)))
                || !isDistinct(context.getLookup().resolve(getOuterTable(join)), context.getLookup()::resolve)) {
            return Result.empty();
        }

        List<Symbol> groupingKeys = join.getCriteria().stream()
                .map(join.getType() == JoinNode.Type.RIGHT ? JoinNode.EquiJoinClause::getLeft : JoinNode.EquiJoinClause::getRight)
                .collect(toImmutableList());
        AggregationNode rewrittenAggregation = new AggregationNode(
                aggregation.getId(),
                getInnerTable(join),
                aggregation.getAggregations(),
                ImmutableList.of(groupingKeys),
                aggregation.getStep(),
                aggregation.getHashSymbol(),
                aggregation.getGroupIdSymbol());

        JoinNode rewrittenJoin;
        if (join.getType() == JoinNode.Type.LEFT) {
            rewrittenJoin = new JoinNode(
                    join.getId(),
                    join.getType(),
                    join.getLeft(),
                    rewrittenAggregation,
                    join.getCriteria(),
                    ImmutableList.<Symbol>builder()
                            .addAll(join.getLeft().getOutputSymbols())
                            .addAll(rewrittenAggregation.getAggregations().keySet())
                            .build(),
                    join.getFilter(),
                    join.getLeftHashSymbol(),
                    join.getRightHashSymbol(),
                    join.getDistributionType());
        }
        else {
            rewrittenJoin = new JoinNode(
                    join.getId(),
                    join.getType(),
                    rewrittenAggregation,
                    join.getRight(),
                    join.getCriteria(),
                    ImmutableList.<Symbol>builder()
                            .addAll(rewrittenAggregation.getAggregations().keySet())
                            .addAll(join.getRight().getOutputSymbols())
                            .build(),
                    join.getFilter(),
                    join.getLeftHashSymbol(),
                    join.getRightHashSymbol(),
                    join.getDistributionType());
        }

        return Result.ofPlanNode(coalesceWithNullAggregation(rewrittenAggregation, rewrittenJoin, context.getSymbolAllocator(), context.getIdAllocator(), context.getLookup()));
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

    private static boolean groupsOnAllOuterTableColumns(AggregationNode node, PlanNode outerTable)
    {
        return new HashSet<>(node.getGroupingKeys()).equals(new HashSet<>(outerTable.getOutputSymbols()));
    }

    // When the aggregation is done after the join, there will be a null value that gets aggregated over
    // where rows did not exist in the inner table.  For some aggregate functions, such as count, the result
    // of an aggregation over a single null row is one or zero rather than null. In order to ensure correct results,
    // we add a coalesce function with the output of the new outer join and the agggregation performed over a single
    // null row.
    private PlanNode coalesceWithNullAggregation(AggregationNode aggregationNode, PlanNode outerJoin, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator, Lookup lookup)
    {
        // Create an aggregation node over a row of nulls.
        MappedAggregationInfo aggregationOverNullInfo = createAggregationOverNull(aggregationNode, symbolAllocator, idAllocator, lookup);
        AggregationNode aggregationOverNull = aggregationOverNullInfo.getAggregation();
        Map<Symbol, Symbol> sourceAggregationToOverNullMapping = aggregationOverNullInfo.getSymbolMapping();

        // Do a cross join with the aggregation over null
        JoinNode crossJoin = new JoinNode(
                idAllocator.getNextId(),
                JoinNode.Type.INNER,
                outerJoin,
                aggregationOverNull,
                ImmutableList.of(),
                ImmutableList.<Symbol>builder()
                        .addAll(outerJoin.getOutputSymbols())
                        .addAll(aggregationOverNull.getOutputSymbols())
                        .build(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        // Add coalesce expressions for all aggregation functions
        Assignments.Builder assignmentsBuilder = Assignments.builder();
        for (Symbol symbol : outerJoin.getOutputSymbols()) {
            if (aggregationNode.getAggregations().containsKey(symbol)) {
                assignmentsBuilder.put(symbol, new CoalesceExpression(symbol.toSymbolReference(), sourceAggregationToOverNullMapping.get(symbol).toSymbolReference()));
            }
            else {
                assignmentsBuilder.put(symbol, symbol.toSymbolReference());
            }
        }
        return new ProjectNode(idAllocator.getNextId(), crossJoin, assignmentsBuilder.build());
    }

    private MappedAggregationInfo createAggregationOverNull(AggregationNode referenceAggregation, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator, Lookup lookup)
    {
        // Create a values node that consists of a single row of nulls.
        // Map the output symbols from the referenceAggregation's source
        // to symbol references for the new values node.
        NullLiteral nullLiteral = new NullLiteral();
        ImmutableList.Builder<Symbol> nullSymbols = ImmutableList.builder();
        ImmutableList.Builder<Expression> nullLiterals = ImmutableList.builder();
        ImmutableMap.Builder<Symbol, SymbolReference> sourcesSymbolMappingBuilder = ImmutableMap.builder();
        for (Symbol sourceSymbol : lookup.resolve(referenceAggregation.getSource()).getOutputSymbols()) {
            nullLiterals.add(nullLiteral);
            Symbol nullSymbol = symbolAllocator.newSymbol(nullLiteral, symbolAllocator.getTypes().get(sourceSymbol));
            nullSymbols.add(nullSymbol);
            sourcesSymbolMappingBuilder.put(sourceSymbol, nullSymbol.toSymbolReference());
        }
        ValuesNode nullRow = new ValuesNode(
                idAllocator.getNextId(),
                nullSymbols.build(),
                ImmutableList.of(nullLiterals.build()));
        Map<Symbol, SymbolReference> sourcesSymbolMapping = sourcesSymbolMappingBuilder.build();

        // For each aggregation function in the reference node, create a corresponding aggregation function
        // that points to the nullRow. Map the symbols from the aggregations in referenceAggregation to the
        // symbols in these new aggregations.
        ImmutableMap.Builder<Symbol, Symbol> aggregationsSymbolMappingBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<Symbol, AggregationNode.Aggregation> aggregationsOverNullBuilder = ImmutableMap.builder();
        for (Map.Entry<Symbol, AggregationNode.Aggregation> entry : referenceAggregation.getAggregations().entrySet()) {
            Symbol aggregationSymbol = entry.getKey();
            AggregationNode.Aggregation aggregation = entry.getValue();
            AggregationNode.Aggregation overNullAggregation = new AggregationNode.Aggregation(
                    (FunctionCall) new ExpressionSymbolInliner(sourcesSymbolMapping).rewrite(aggregation.getCall()),
                    aggregation.getSignature(),
                    aggregation.getMask().map(x -> Symbol.from(sourcesSymbolMapping.get(x))),
                    aggregation.getOrderBy().stream()
                            .map(x -> Symbol.from(sourcesSymbolMapping.get(x)))
                            .collect(toImmutableList()),
                    aggregation.getOrdering());
            Symbol overNullSymbol = symbolAllocator.newSymbol(overNullAggregation.getCall(), symbolAllocator.getTypes().get(aggregationSymbol));
            aggregationsOverNullBuilder.put(overNullSymbol, overNullAggregation);
            aggregationsSymbolMappingBuilder.put(aggregationSymbol, overNullSymbol);
        }
        Map<Symbol, Symbol> aggregationsSymbolMapping = aggregationsSymbolMappingBuilder.build();

        // create an aggregation node whose source is the null row.
        AggregationNode aggregationOverNullRow = new AggregationNode(
                idAllocator.getNextId(),
                nullRow,
                aggregationsOverNullBuilder.build(),
                ImmutableList.of(ImmutableList.of()),
                AggregationNode.Step.SINGLE,
                Optional.empty(),
                Optional.empty());
        return new MappedAggregationInfo(aggregationOverNullRow, aggregationsSymbolMapping);
    }

    private static class MappedAggregationInfo
    {
        private final AggregationNode aggregationNode;
        private final Map<Symbol, Symbol> symbolMapping;

        public MappedAggregationInfo(AggregationNode aggregationNode, Map<Symbol, Symbol> symbolMapping)
        {
            this.aggregationNode = aggregationNode;
            this.symbolMapping = symbolMapping;
        }

        public Map<Symbol, Symbol> getSymbolMapping()
        {
            return symbolMapping;
        }

        public AggregationNode getAggregation()
        {
            return aggregationNode;
        }
    }
}
