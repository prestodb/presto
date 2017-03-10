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
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.analyzer.TypeSignatureProvider;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.AggregationNode.Aggregation;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.GroupIdNode;
import com.facebook.presto.sql.planner.plan.MarkDistinctNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.ComparisonExpressionType;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.IfExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.SystemSessionProperties.isOptimizeDistinctAggregationEnabled;
import static com.facebook.presto.sql.planner.plan.AggregationNode.Step.SINGLE;
import static com.google.common.collect.ImmutableList.toImmutableList;

/*
 * Optimizes queries of the form:
 *
 *  SELECT a1, a2,..., an, F1(b1), F2(b2), F3(b3), ...., Fm(bm), F(distinct c) FROM Table GROUP BY a1, a2, ..., an
 *
 * Example: SELECT a, count(b), sum(distinct c) FROM table GROUP BY a
 *
 * Input AggregationNode of the query:
 *
 * AggregationNode[group by: a, aggregation: count(b) as count, sum(distinct c) as sum]
 *
 * Output plan from this rule looks like:
 *
 * AggreggationNode [group by: a, aggregation: arbitrary(if_expr_1) as count, sum(if_expr_2) as sum]
 *  |_ ProjectNode[a, if_expr_1 -> if(group_id=1,c), if_expr_2 -> if(group_id=0, count_b), c$distinct]
 *      |_ AggregationNode[group by: [a,c,group_id], aggregation: count(b) as count_b]
 *          |_ GroupId[groupingSetMapping: [a,b][a,c], groupId: group_id]
 */
public class OptimizeMixedDistinctAggregations
        implements Rule
{
    private final FunctionRegistry functionRegistry;

    public OptimizeMixedDistinctAggregations(FunctionRegistry functionRegistry)
    {
        this.functionRegistry = functionRegistry;
    }

    @Override
    public Optional<PlanNode> apply(PlanNode node, Lookup lookup, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, Session session)
    {
        if (!isOptimizeDistinctAggregationEnabled(session)) {
            return Optional.empty();
        }

        if (!(node instanceof AggregationNode)) {
            return Optional.empty();
        }

        AggregationNode aggregationNode = (AggregationNode) node;

        // optimize if and only if
        // there is exactly one distinct aggregation mask (distinct aggregations on the same column)
        // and if there are also non-distinct aggregations present along with distinct aggregation(this case handled by SingleMarkDistinctToGroupBy)
        List<Symbol> masks = aggregationNode.getAggregations().values().stream()
                .map(Aggregation::getMask).filter(Optional::isPresent).map(Optional::get).collect(toImmutableList());
        Set<Symbol> uniqueMasks = ImmutableSet.copyOf(masks);
        if (uniqueMasks.size() != 1 || masks.size() == aggregationNode.getAggregations().size()) {
            return Optional.empty();
        }

        if (aggregationNode.getAggregations().values().stream().map(Aggregation::getCall).map(FunctionCall::getFilter).anyMatch(Optional::isPresent)) {
            // Skip if any aggregation contains a filter
            return Optional.empty();
        }

        if (!checkAllEquatableTypes(aggregationNode, symbolAllocator)) {
            // This optimization relies on being able to GROUP BY arguments
            // of the original aggregation functions. If their types are
            // not comparable, we have to skip it.
            return Optional.empty();
        }

        PlanNode source = lookup.resolve(aggregationNode.getSource());

        if (!(source instanceof MarkDistinctNode)) {
            return Optional.empty();
        }

        MarkDistinctNode markDistinctNode = (MarkDistinctNode) source;
        // make sure there's a markdistinct associated with this aggregation
        // presence of aggregateInfo => mask also present
        if (!getDistinctAggregationMask(aggregationNode).equals(markDistinctNode.getMarkerSymbol())) {
            return Optional.empty();
        }

        MappedPlanNodeInfo rewrittenSource = rewriteMarkDistinct(markDistinctNode, aggregationNode, lookup, symbolAllocator, idAllocator);

        // Change aggregate node to do second aggregation, handles this part of optimized plan mentioned above:
        // AggreggationNode [group by: a, aggregation: arbitrary(if_expr_1) as count, sum(if_expr_2) as sum]
        ImmutableMap.Builder<Symbol, Aggregation> aggregations = ImmutableMap.builder();
        for (Map.Entry<Symbol, Aggregation> entry : aggregationNode.getAggregations().entrySet()) {
            FunctionCall functionCall = entry.getValue().getCall();
            if (functionCall.isDistinct()) {
                aggregations.put(entry.getKey(), new Aggregation(
                        new FunctionCall(functionCall.getName(),
                                functionCall.getWindow(),
                                false,
                                ImmutableList.of(rewrittenSource.getDistinctAggregationSymbol().get().toSymbolReference())),
                        entry.getValue().getSignature(),
                        Optional.empty()));
            }
            else {
                // Aggregations on non-distinct are already done by new node, just extract the non-null value
                Symbol argument = rewrittenSource.getNonDistinctAggregationSymbols().get(entry.getKey());
                QualifiedName functionName = QualifiedName.of("arbitrary");
                aggregations.put(entry.getKey(), new Aggregation(new FunctionCall(
                        functionName,
                        functionCall.getWindow(),
                        false,
                        ImmutableList.of(argument.toSymbolReference())),
                        getFunctionSignature(functionName, argument, symbolAllocator),
                        Optional.empty()));
            }
        }

        return Optional.of(new AggregationNode(
                idAllocator.getNextId(),
                rewrittenSource.getNode(),
                aggregations.build(),
                aggregationNode.getGroupingSets(),
                aggregationNode.getStep(),
                Optional.empty(),
                aggregationNode.getGroupIdSymbol()));
    }

    private static List<Symbol> getAggregationMask(AggregationNode aggregationNode)
    {
        return aggregationNode.getAggregations().values().stream()
                .map(Aggregation::getMask)
                .filter(Optional::isPresent)
                .map(Optional::get).collect(toImmutableList());
    }

    private static Symbol getDistinctAggregationMask(AggregationNode aggregationNode)
    {
        return Iterables.getOnlyElement(ImmutableSet.copyOf(getAggregationMask(aggregationNode)));
    }

    private static MappedPlanNodeInfo rewriteMarkDistinct(
            MarkDistinctNode markDistinctNode,
            AggregationNode aggregationNode,
            Lookup lookup,
            SymbolAllocator symbolAllocator,
            PlanNodeIdAllocator idAllocator)
    {
        PlanNode source = lookup.resolve(markDistinctNode.getSource());

        Set<Symbol> allSymbols = new HashSet<>();
        List<Symbol> groupBySymbols = aggregationNode.getGroupingKeys(); // a
        List<Symbol> nonDistinctAggregateSymbols = getOriginalNonDistinctAggregateArgs(aggregationNode.getAggregations()); //b
        Symbol distinctSymbol = Iterables.getOnlyElement(getOriginalDistinctAggregateArgs(aggregationNode.getAggregations())); // c

        // If the same symbol is present in aggregations on distinct and non-distinct values, e.g. select sum(a), count(distinct a),
        // then we need to create a duplicate stream for this symbol
        Optional<Symbol> duplicatedDistinctSymbol = Optional.empty();

        if (nonDistinctAggregateSymbols.contains(distinctSymbol)) {
            Symbol newSymbol = symbolAllocator.newSymbol(distinctSymbol.getName(), symbolAllocator.getTypes().get(distinctSymbol));
            nonDistinctAggregateSymbols.set(nonDistinctAggregateSymbols.indexOf(distinctSymbol), newSymbol);
            duplicatedDistinctSymbol = Optional.of(newSymbol);
        }

        allSymbols.addAll(groupBySymbols);
        allSymbols.addAll(nonDistinctAggregateSymbols);
        allSymbols.add(distinctSymbol);

        // 1. Add GroupIdNode
        Symbol groupSymbol = symbolAllocator.newSymbol("group_id", BigintType.BIGINT); // g
        GroupIdNode groupIdNode = createGroupIdNode(
                groupBySymbols,
                nonDistinctAggregateSymbols,
                distinctSymbol,
                duplicatedDistinctSymbol,
                groupSymbol,
                allSymbols,
                source,
                idAllocator);

        // 2. Add aggregation node
        Set<Symbol> groupByKeys = new HashSet<>();
        groupByKeys.addAll(groupBySymbols);
        groupByKeys.add(distinctSymbol);
        groupByKeys.add(groupSymbol);

        // This map has mapping only for aggregation on non-distinct symbols which the new AggregationNode handles
        MappedPlanNodeInfo nonDistinctAggregationInfo = createNonDistinctAggregationInfo(
                aggregationNode,
                distinctSymbol,
                duplicatedDistinctSymbol,
                groupByKeys,
                groupIdNode,
                markDistinctNode,
                symbolAllocator,
                idAllocator);

        // 3. Add new project node that adds IF expressions with original symbol mapped
        return createProjectNodeInfo(
                nonDistinctAggregationInfo,
                getDistinctAggregationMask(aggregationNode),
                distinctSymbol,
                groupSymbol,
                groupBySymbols,
                symbolAllocator,
                idAllocator);
    }

    // Returns false if either the mask symbol or any of the symbols in aggregations are not comparable
    private static boolean checkAllEquatableTypes(AggregationNode aggregationNode, SymbolAllocator symbolAllocator)
    {
        for (Symbol symbol : getOriginalNonDistinctAggregateArgs(aggregationNode.getAggregations())) {
            Type type = symbolAllocator.getTypes().get(symbol);
            if (!type.isComparable()) {
                return false;
            }
        }

        return symbolAllocator.getTypes().get(getDistinctAggregationMask(aggregationNode)).isComparable();
    }

    private static List<Symbol> getOriginalNonDistinctAggregateArgs(Map<Symbol, Aggregation> assignments)
    {
        return assignments.values().stream()
                .filter(aggregation -> !aggregation.getCall().isDistinct())
                .flatMap(aggregation -> aggregation.getCall().getArguments().stream())
                .distinct()
                .map(Symbol::from)
                .collect(Collectors.toList());
    }

    private static List<Symbol> getOriginalDistinctAggregateArgs(Map<Symbol, Aggregation> assignments)
    {
        return assignments.values().stream()
                .filter(aggregation -> aggregation.getCall().isDistinct())
                .flatMap(aggregation -> aggregation.getCall().getArguments().stream())
                .distinct()
                .map(Symbol::from)
                .collect(Collectors.toList());
    }

    private static GroupIdNode createGroupIdNode(
            List<Symbol> groupBySymbols,
            List<Symbol> nonDistinctAggregateSymbols,
            Symbol distinctSymbol,
            Optional<Symbol> duplicatedDistinctSymbol,
            Symbol groupSymbol,
            Set<Symbol> allSymbols,
            PlanNode source,
            PlanNodeIdAllocator idAllocator)
    {
        List<List<Symbol>> groups = new ArrayList<>();
        // g0 = {group-by symbols + allNonDistinctAggregateSymbols}
        // g1 = {group-by symbols + Distinct Symbol}
        // symbols present in Group_i will be set, rest will be Null

        //g0
        Set<Symbol> group0 = new HashSet<>();
        group0.addAll(groupBySymbols);
        group0.addAll(nonDistinctAggregateSymbols);
        groups.add(ImmutableList.copyOf(group0));

        // g1
        Set<Symbol> group1 = new HashSet<>();
        group1.addAll(groupBySymbols);
        group1.add(distinctSymbol);
        groups.add(ImmutableList.copyOf(group1));

        return new GroupIdNode(
                idAllocator.getNextId(),
                source,
                groups,
                allSymbols.stream().distinct().collect(Collectors.toMap(
                        symbol -> symbol,
                        symbol -> (duplicatedDistinctSymbol.isPresent() && symbol.equals(duplicatedDistinctSymbol.get()) ? distinctSymbol : symbol))),
                ImmutableMap.of(),
                groupSymbol);
    }

    /*
     * This method returns a new Aggregation node which has aggregations on non-distinct symbols from original plan. Generates
     * AggregationNode[group by: [a,c,group_id], aggregation: count(b) as count_b]
     * part in the optimized plan mentioned above
     *
     * It also populates the mappings of new function's output symbol to corresponding old function's output symbol, e.g.
     *     { count_b -> count }
     * The new AggregateNode aggregates on the symbols that original AggregationNode aggregated on
     * Original one will now aggregate on the output symbols of this new node
     */
    private static MappedPlanNodeInfo createNonDistinctAggregationInfo(
            AggregationNode aggregationNode,
            Symbol distinctSymbol,
            Optional<Symbol> duplicatedDistinctSymbol,
            Set<Symbol> groupByKeys,
            GroupIdNode groupIdNode,
            MarkDistinctNode originalNode,
            SymbolAllocator symbolAllocator,
            PlanNodeIdAllocator idAllocator
    )
    {
        ImmutableMap.Builder<Symbol, Aggregation> assignments = ImmutableMap.builder();
        ImmutableMap.Builder<Symbol, Symbol> aggregationOutputSymbolsMapBuilder = ImmutableMap.builder();

        for (Map.Entry<Symbol, Aggregation> entry : aggregationNode.getAggregations().entrySet()) {
            FunctionCall functionCall = entry.getValue().getCall();
            if (!functionCall.isDistinct()) {
                Symbol newSymbol = symbolAllocator.newSymbol(entry.getKey().toSymbolReference(), symbolAllocator.getTypes().get(entry.getKey()));
                aggregationOutputSymbolsMapBuilder.put(newSymbol, entry.getKey());
                if (duplicatedDistinctSymbol.isPresent()) {
                    // Handling for cases when mask symbol appears in non distinct aggregations too
                    // Now the aggregation should happen over the duplicate symbol added before
                    if (functionCall.getArguments().contains(distinctSymbol.toSymbolReference())) {
                        ImmutableList.Builder arguments = ImmutableList.builder();
                        for (Expression argument : functionCall.getArguments()) {
                            if (distinctSymbol.toSymbolReference().equals(argument)) {
                                arguments.add(duplicatedDistinctSymbol.get().toSymbolReference());
                            }
                            else {
                                arguments.add(argument);
                            }
                        }
                        functionCall = new FunctionCall(functionCall.getName(), functionCall.getWindow(), false, arguments.build());
                    }
                }
                assignments.put(newSymbol, new Aggregation(
                        functionCall,
                        entry.getValue().getSignature(),
                        Optional.empty()));
            }
        }

        return new MappedPlanNodeInfo(new AggregationNode(
                idAllocator.getNextId(),
                groupIdNode,
                assignments.build(),
                ImmutableList.of(ImmutableList.copyOf(groupByKeys)),
                SINGLE,
                originalNode.getHashSymbol(),
                Optional.empty()), aggregationOutputSymbolsMapBuilder.build());
    }

    private Signature getFunctionSignature(QualifiedName functionName, Symbol argument, SymbolAllocator symbolAllocator)
    {
        return functionRegistry
                .resolveFunction(
                        functionName,
                        ImmutableList.of(new TypeSignatureProvider(symbolAllocator.getTypes().get(argument).getTypeSignature())));
    }

    /*
     * This Project is useful for cases when we aggregate on distinct and non-distinct values of same symbol, eg:
     *  select a, sum(b), count(c), sum(distinct c) group by a
     * Without this Project, we would count additional values for count(c)
     *
     * This method also populates maps of old to new symbols. For each key of outputNonDistinctAggregateSymbols,
     * Higher level aggregation node's aggregation <key, AggregateExpression> will now have to run AggregateExpression on value of outputNonDistinctAggregateSymbols
     * Same for outputDistinctAggregateSymbols map
     */
    private static MappedPlanNodeInfo createProjectNodeInfo(
            MappedPlanNodeInfo sourceNodeInfo,
            Symbol maskSymbol,
            Symbol distinctSymbol,
            Symbol groupSymbol,
            List<Symbol> groupBySymbols,
            SymbolAllocator symbolAllocator,
            PlanNodeIdAllocator idAllocator)
    {
        Assignments.Builder outputSymbols = Assignments.builder();
        ImmutableMap.Builder<Symbol, Symbol> outputNonDistinctAggregateSymbols = ImmutableMap.builder();
        Optional<Symbol> newDistinctSymbol = Optional.empty();
        for (Symbol symbol : sourceNodeInfo.getNode().getOutputSymbols()) {
            if (distinctSymbol.equals(symbol)) {
                newDistinctSymbol = Optional.of(symbolAllocator.newSymbol("expr", symbolAllocator.getTypes().get(symbol)));

                outputSymbols.put(newDistinctSymbol.get(), createIfExpression(
                        groupSymbol.toSymbolReference(),
                        new Cast(new LongLiteral("1"), "bigint"), // TODO: this should use GROUPING() when that's available instead of relying on specific group numbering
                        ComparisonExpressionType.EQUAL,
                        symbol.toSymbolReference(),
                        symbolAllocator.getTypes().get(symbol)));
            }
            else if (sourceNodeInfo.getNonDistinctAggregationSymbols().containsKey(symbol)) {
                Symbol newSymbol = symbolAllocator.newSymbol("expr", symbolAllocator.getTypes().get(symbol));
                // key of outputNonDistinctAggregateSymbols is key of an aggregation in AggrNode above, it will now aggregate on this Map's value
                outputNonDistinctAggregateSymbols.put(sourceNodeInfo.getNonDistinctAggregationSymbols().get(symbol), newSymbol);
                outputSymbols.put(newSymbol, createIfExpression(
                        groupSymbol.toSymbolReference(),
                        new Cast(new LongLiteral("0"), "bigint"), // TODO: this should use GROUPING() when that's available instead of relying on specific group numbering
                        ComparisonExpressionType.EQUAL,
                        symbol.toSymbolReference(),
                        symbolAllocator.getTypes().get(symbol)));
            }
            // A symbol can appear both in groupBy and distinct/non-distinct aggregation
            if (groupBySymbols.contains(symbol)) {
                outputSymbols.put(symbol, symbol.toSymbolReference());
            }
        }

        // add null assignment for mask
        // unused mask will be removed by PruneUnreferencedOutputs
        outputSymbols.put(maskSymbol, new NullLiteral());

        return new MappedPlanNodeInfo(
                new ProjectNode(idAllocator.getNextId(), sourceNodeInfo.getNode(), outputSymbols.build()),
                outputNonDistinctAggregateSymbols.build(),
                newDistinctSymbol);
    }

    // creates if clause specific to use case here, default value always null
    private static IfExpression createIfExpression(Expression left, Expression right, ComparisonExpressionType type, Expression result, Type trueValueType)
    {
        return new IfExpression(
                new ComparisonExpression(type, left, right),
                result,
                new Cast(new NullLiteral(), trueValueType.getTypeSignature().toString()));
    }

    private static class MappedPlanNodeInfo
    {
        private final PlanNode node;
        private final Map<Symbol, Symbol> nonDistinctAggregationSymbols;
        private final Optional<Symbol> distinctAggregationSymbol;

        public MappedPlanNodeInfo(PlanNode node, Map<Symbol, Symbol> nonDistinctAggregationSymbols)
        {
            this.node = node;
            this.nonDistinctAggregationSymbols = nonDistinctAggregationSymbols;
            this.distinctAggregationSymbol = Optional.empty();
        }

        public MappedPlanNodeInfo(PlanNode node, Map<Symbol, Symbol> nonDistinctSymbolMapping, Optional<Symbol> distinctAggregationSymbol)
        {
            this.node = node;
            this.nonDistinctAggregationSymbols = nonDistinctSymbolMapping;
            this.distinctAggregationSymbol = distinctAggregationSymbol;
        }

        public Map<Symbol, Symbol> getNonDistinctAggregationSymbols()
        {
            return nonDistinctAggregationSymbols;
        }

        public PlanNode getNode()
        {
            return node;
        }

        public Optional<Symbol> getDistinctAggregationSymbol()
        {
            return distinctAggregationSymbol;
        }
    }
}
