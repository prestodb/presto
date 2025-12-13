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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.expressions.RowExpressionRewriter;
import com.facebook.presto.expressions.RowExpressionTreeRewriter;
import com.facebook.presto.spi.PrestoWarning;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.AggregationNode.Aggregation;
import com.facebook.presto.spi.plan.DataOrganizationSpecification;
import com.facebook.presto.spi.plan.ExchangeEncoding;
import com.facebook.presto.spi.plan.Ordering;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.PartitioningScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.StatisticAggregations;
import com.facebook.presto.spi.plan.StatisticAggregationsDescriptor;
import com.facebook.presto.spi.plan.TableFinishNode;
import com.facebook.presto.spi.plan.TableWriterNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.CallDistributedProcedureNode;
import com.facebook.presto.sql.planner.plan.MergeProcessorNode;
import com.facebook.presto.sql.planner.plan.MergeWriterNode;
import com.facebook.presto.sql.planner.plan.StatisticsWriterNode;
import com.facebook.presto.sql.planner.plan.TableFunctionNode;
import com.facebook.presto.sql.planner.plan.TableFunctionProcessorNode;
import com.facebook.presto.sql.planner.plan.TableWriterMergeNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.spi.StandardWarningCode.MULTIPLE_ORDER_BY;
import static com.facebook.presto.spi.plan.AggregationNode.groupingSets;
import static com.facebook.presto.sql.analyzer.ExpressionTreeUtils.getNodeLocation;
import static com.facebook.presto.sql.planner.optimizations.PartitioningUtils.translateVariable;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

public class SymbolMapper
{
    private final Map<String, String> mapping;
    private final TypeProvider types;
    private final WarningCollector warningCollector;

    public SymbolMapper(Map<VariableReferenceExpression, VariableReferenceExpression> mapping, WarningCollector warningCollector)
    {
        requireNonNull(mapping, "mapping is null");
        this.mapping = mapping.entrySet().stream().collect(toImmutableMap(entry -> entry.getKey().getName(), entry -> entry.getValue().getName()));
        ImmutableSet.Builder<VariableReferenceExpression> variables = ImmutableSet.builder();
        mapping.entrySet().forEach(entry -> {
            variables.add(entry.getKey());
            variables.add(entry.getValue());
        });
        this.types = TypeProvider.fromVariables(variables.build());
        this.warningCollector = warningCollector;
    }

    public SymbolMapper(Map<String, String> mapping, TypeProvider types, WarningCollector warningCollector)
    {
        requireNonNull(mapping, "mapping is null");
        this.mapping = ImmutableMap.copyOf(mapping);
        this.types = requireNonNull(types, "types is null");
        this.warningCollector = warningCollector;
    }

    public Symbol map(Symbol symbol)
    {
        String canonical = symbol.getName();
        while (mapping.containsKey(canonical) && !mapping.get(canonical).equals(canonical)) {
            canonical = mapping.get(canonical);
        }
        return new Symbol(canonical);
    }

    public VariableReferenceExpression map(VariableReferenceExpression variable)
    {
        String canonical = variable.getName();
        while (mapping.containsKey(canonical) && !mapping.get(canonical).equals(canonical)) {
            canonical = mapping.get(canonical);
        }
        if (canonical.equals(variable.getName())) {
            return variable;
        }
        return new VariableReferenceExpression(variable.getSourceLocation(), canonical, types.get(new SymbolReference(getNodeLocation(variable.getSourceLocation()), canonical)));
    }

    public List<VariableReferenceExpression> map(List<VariableReferenceExpression> variableReferenceExpressions)
    {
        return variableReferenceExpressions.stream()
                .map(this::map)
                .collect(toImmutableList());
    }

    public Expression map(Expression value)
    {
        return ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<Void>()
        {
            @Override
            public Expression rewriteSymbolReference(SymbolReference node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                Symbol canonical = map(Symbol.from(node));
                return canonical.toSymbolReference();
            }
        }, value);
    }

    public RowExpression map(RowExpression value)
    {
        return RowExpressionTreeRewriter.rewriteWith(new RowExpressionRewriter<Void>()
        {
            @Override
            public RowExpression rewriteVariableReference(VariableReferenceExpression variable, Void context, RowExpressionTreeRewriter<Void> treeRewriter)
            {
                return map(variable);
            }
        }, value);
    }

    public OrderingSchemeWithPreSortedPrefix map(OrderingScheme orderingScheme, int preSorted)
    {
        ImmutableList.Builder<Ordering> newOrderings = ImmutableList.builder();
        int newPreSorted = preSorted;

        Set<VariableReferenceExpression> added = new HashSet<>(orderingScheme.getOrderBy().size());

        for (int i = 0; i < orderingScheme.getOrderBy().size(); i++) {
            VariableReferenceExpression variable = orderingScheme.getOrderBy().get(i).getVariable();
            VariableReferenceExpression canonical = map(variable);
            if (added.add(canonical)) {
                newOrderings.add(new Ordering(canonical, orderingScheme.getOrdering(variable)));
            }
            else if (i < preSorted) {
                newPreSorted--;
            }
        }

        return new OrderingSchemeWithPreSortedPrefix(new OrderingScheme(newOrderings.build()), newPreSorted);
    }

    public OrderingScheme map(OrderingScheme orderingScheme)
    {
        // SymbolMapper inlines symbol with multiple level reference (SymbolInliner only inline single level).
        ImmutableList.Builder<VariableReferenceExpression> orderBy = ImmutableList.builder();
        HashMap<VariableReferenceExpression, SortOrder> orderingMap = new HashMap<>();
        for (VariableReferenceExpression variable : orderingScheme.getOrderByVariables()) {
            VariableReferenceExpression translated = map(variable);
            // Some variables may become duplicates after canonicalization, so we put them only once.
            if (!orderingMap.containsKey(translated)) {
                orderBy.add(translated);
                orderingMap.put(translated, orderingScheme.getOrdering(variable));
            }
            else if (orderingMap.get(translated) != orderingScheme.getOrdering(variable)) {
                warningCollector.add(new PrestoWarning(
                        MULTIPLE_ORDER_BY,
                        "Multiple ORDER BY for a variable were given, only first provided will be considered"));
            }
        }

        return new OrderingScheme(orderBy.build().stream().map(variable -> new Ordering(variable, orderingMap.get(variable))).collect(toImmutableList()));
    }

    public AggregationNode map(AggregationNode node, PlanNode source)
    {
        return map(node, source, node.getId());
    }

    public AggregationNode map(AggregationNode node, PlanNode source, PlanNodeIdAllocator idAllocator)
    {
        return map(node, source, idAllocator.getNextId());
    }

    private AggregationNode map(AggregationNode node, PlanNode source, PlanNodeId newNodeId)
    {
        ImmutableMap.Builder<VariableReferenceExpression, Aggregation> aggregations = ImmutableMap.builder();
        for (Entry<VariableReferenceExpression, Aggregation> entry : node.getAggregations().entrySet()) {
            aggregations.put(map(entry.getKey()), map(entry.getValue()));
        }

        return new AggregationNode(
                source.getSourceLocation(),
                newNodeId,
                source,
                aggregations.build(),
                groupingSets(
                        mapAndDistinctVariable(node.getGroupingKeys()),
                        node.getGroupingSetCount(),
                        node.getGlobalGroupingSets()),
                mapAndDistinctVariable(node.getPreGroupedVariables()),
                node.getStep(),
                node.getHashVariable().map(this::map),
                node.getGroupIdVariable().map(this::map),
                node.getAggregationId());
    }

    private Aggregation map(Aggregation aggregation)
    {
        return new Aggregation(
                new CallExpression(
                        aggregation.getCall().getSourceLocation(),
                        aggregation.getCall().getDisplayName(),
                        aggregation.getCall().getFunctionHandle(),
                        aggregation.getCall().getType(),
                        aggregation.getArguments().stream().map(this::map).collect(toImmutableList())),
                aggregation.getFilter().map(this::map),
                aggregation.getOrderBy().map(this::map),
                aggregation.isDistinct(),
                aggregation.getMask().map(this::map));
    }

    public TopNNode map(TopNNode node, PlanNode source, PlanNodeId newNodeId)
    {
        ImmutableList.Builder<VariableReferenceExpression> variables = ImmutableList.builder();
        ImmutableMap.Builder<VariableReferenceExpression, SortOrder> orderings = ImmutableMap.builder();
        Set<VariableReferenceExpression> seenCanonicals = new HashSet<>(node.getOrderingScheme().getOrderByVariables().size());
        for (VariableReferenceExpression variable : node.getOrderingScheme().getOrderByVariables()) {
            VariableReferenceExpression canonical = map(variable);
            if (seenCanonicals.add(canonical)) {
                seenCanonicals.add(canonical);
                variables.add(canonical);
                orderings.put(canonical, node.getOrderingScheme().getOrdering(variable));
            }
        }

        ImmutableMap<VariableReferenceExpression, SortOrder> orderingMap = orderings.build();
        return new TopNNode(
                node.getSourceLocation(),
                newNodeId,
                source,
                node.getCount(),
                new OrderingScheme(variables.build().stream().map(variable -> new Ordering(variable, orderingMap.get(variable))).collect(toImmutableList())),
                node.getStep());
    }

    public TableWriterNode map(TableWriterNode node, PlanNode source)
    {
        return map(node, source, node.getId());
    }

    public TableWriterNode map(TableWriterNode node, PlanNode source, PlanNodeId newNodeId)
    {
        // Intentionally does not use canonicalizeAndDistinct as that would remove columns
        ImmutableList<VariableReferenceExpression> columns = node.getColumns().stream()
                .map(this::map)
                .collect(toImmutableList());

        Set<VariableReferenceExpression> notNullColumnVariables = node.getNotNullColumnVariables().stream()
                .map(this::map)
                .collect(toImmutableSet());
        return new TableWriterNode(
                source.getSourceLocation(),
                newNodeId,
                node.getStatsEquivalentPlanNode(),
                source,
                node.getTarget(),
                map(node.getRowCountVariable()),
                map(node.getFragmentVariable()),
                map(node.getTableCommitContextVariable()),
                columns,
                node.getColumnNames(),
                notNullColumnVariables,
                node.getTablePartitioningScheme().map(partitioningScheme -> canonicalize(partitioningScheme, source)),
                node.getStatisticsAggregation().map(this::map),
                node.getTaskCountIfScaledWriter(),
                node.getIsTemporaryTableWriter());
    }

    public CallDistributedProcedureNode map(CallDistributedProcedureNode node, PlanNode source)
    {
        ImmutableList<VariableReferenceExpression> columns = node.getColumns().stream()
                .map(this::map)
                .collect(toImmutableList());
        Set<VariableReferenceExpression> notNullColumnVariables = node.getNotNullColumnVariables().stream()
                .map(this::map)
                .collect(toImmutableSet());

        return new CallDistributedProcedureNode(
                node.getSourceLocation(),
                node.getId(),
                source,
                node.getTarget(),
                node.getRowCountVariable(),
                node.getFragmentVariable(),
                node.getTableCommitContextVariable(),
                columns,
                columns.stream().map(VariableReferenceExpression::getName).collect(toImmutableList()),
                notNullColumnVariables,
                node.getPartitioningScheme().map(partitioningScheme -> canonicalize(partitioningScheme, source)));
    }

    public StatisticsWriterNode map(StatisticsWriterNode node, PlanNode source)
    {
        return new StatisticsWriterNode(
                node.getSourceLocation(),
                node.getId(),
                source,
                node.getTableHandle(),
                node.getRowCountVariable(),
                node.isRowCountEnabled(),
                node.getDescriptor().map(this::map));
    }

    public MergeWriterNode map(MergeWriterNode node, PlanNode source)
    {
        // Intentionally does not use mapAndDistinct on columns as that would remove columns
        List<VariableReferenceExpression> newOutputs = map(node.getOutputVariables());

        return new MergeWriterNode(
                source.getSourceLocation(),
                node.getId(),
                source,
                node.getTarget(),
                map(node.getMergeProcessorProjectedVariables()),
                newOutputs);
    }

    public MergeWriterNode map(MergeWriterNode node, PlanNode source, PlanNodeId newId)
    {
        // Intentionally does not use mapAndDistinct on columns as that would remove columns
        List<VariableReferenceExpression> newOutputs = map(node.getOutputVariables());

        return new MergeWriterNode(
                source.getSourceLocation(),
                newId,
                source,
                node.getTarget(),
                map(node.getMergeProcessorProjectedVariables()),
                newOutputs);
    }

    public MergeProcessorNode map(MergeProcessorNode node, PlanNode source)
    {
        List<VariableReferenceExpression> newOutputs = map(node.getOutputVariables());

        return new MergeProcessorNode(
                source.getSourceLocation(),
                node.getId(),
                source,
                node.getTarget(),
                map(node.getTargetTableRowIdColumnVariable()),
                map(node.getMergeRowVariable()),
                map(node.getTargetColumnVariables()),
                newOutputs);
    }

    public PartitioningScheme map(PartitioningScheme scheme, List<VariableReferenceExpression> sourceLayout)
    {
        return new PartitioningScheme(
                translateVariable(scheme.getPartitioning(), this::map),
                mapAndDistinctVariable(sourceLayout),
                scheme.getHashColumn().map(this::map),
                scheme.isReplicateNullsAndAny(),
                scheme.isScaleWriters(),
                ExchangeEncoding.COLUMNAR,
                scheme.getBucketToPartition());
    }

    public TableFinishNode map(TableFinishNode node, PlanNode source)
    {
        return new TableFinishNode(
                node.getSourceLocation(),
                node.getId(),
                source,
                node.getTarget(),
                map(node.getRowCountVariable()),
                node.getStatisticsAggregation().map(this::map),
                node.getStatisticsAggregationDescriptor().map(descriptor -> descriptor.map(this::map)),
                node.getCteMaterializationInfo());
    }

    public TableWriterMergeNode map(TableWriterMergeNode node, PlanNode source)
    {
        return new TableWriterMergeNode(
                node.getSourceLocation(),
                node.getId(),
                source,
                map(node.getRowCountVariable()),
                map(node.getFragmentVariable()),
                map(node.getTableCommitContextVariable()),
                node.getStatisticsAggregation().map(this::map));
    }

    public TableFunctionProcessorNode map(TableFunctionProcessorNode node, PlanNode source)
    {
        // rewrite and deduplicate pass-through specifications
        // note: Potentially, pass-through symbols from different sources might be recognized as semantically identical, and rewritten
        // to the same symbol. Currently, we retrieve the first occurrence of a symbol, and skip all the following occurrences.
        // For better performance, we could pick the occurrence with "isPartitioningColumn" property, since the pass-through mechanism
        // is more efficient for partitioning columns which are guaranteed to be constant within partition.
        // TODO choose a partitioning column to be retrieved while deduplicating
        ImmutableList.Builder<TableFunctionNode.PassThroughSpecification> newPassThroughSpecifications = ImmutableList.builder();
        Set<VariableReferenceExpression> newPassThroughVariables = new HashSet<>();
        for (TableFunctionNode.PassThroughSpecification specification : node.getPassThroughSpecifications()) {
            ImmutableList.Builder<TableFunctionNode.PassThroughColumn> newColumns = ImmutableList.builder();
            for (TableFunctionNode.PassThroughColumn column : specification.getColumns()) {
                VariableReferenceExpression newVariable = map(column.getOutputVariables());
                if (newPassThroughVariables.add(newVariable)) {
                    newColumns.add(new TableFunctionNode.PassThroughColumn(newVariable, column.isPartitioningColumn()));
                }
            }
            newPassThroughSpecifications.add(new TableFunctionNode.PassThroughSpecification(specification.isDeclaredAsPassThrough(), newColumns.build()));
        }

        // rewrite required symbols without deduplication. the table function expects specific input layout
        List<List<VariableReferenceExpression>> newRequiredVariables = node.getRequiredVariables().stream()
                .map(list -> list.stream()
                        .map(this::map)
                        .collect(toImmutableList()))
                .collect(toImmutableList());

        // rewrite and deduplicate marker mapping
        Optional<Map<VariableReferenceExpression, VariableReferenceExpression>> newMarkerVariables = node.getMarkerVariables()
                .map(mapping -> mapping.entrySet().stream()
                        .collect(toImmutableMap(
                                entry -> map(entry.getKey()),
                                entry -> map(entry.getValue()),
                                (first, second) -> {
                                    checkState(first.equals(second), "Ambiguous marker symbols: %s and %s", first, second);
                                    return first;
                                })));

        // rewrite and deduplicate specification
        Optional<SpecificationWithPreSortedPrefix> newSpecification = node.getSpecification().map(specification -> mapAndDistinct(specification, node.getPreSorted()));

        return new TableFunctionProcessorNode(
                node.getId(),
                node.getName(),
                node.getProperOutputs().stream()
                        .map(this::map)
                        .collect(toImmutableList()),
                Optional.of(source),
                node.isPruneWhenEmpty(),
                newPassThroughSpecifications.build(),
                newRequiredVariables,
                newMarkerVariables,
                newSpecification.map(SpecificationWithPreSortedPrefix::getSpecification),
                node.getPrePartitioned().stream()
                        .map(this::map)
                        .collect(toImmutableSet()),
                newSpecification.map(SpecificationWithPreSortedPrefix::getPreSorted).orElse(node.getPreSorted()),
                node.getHashSymbol().map(this::map),
                node.getHandle());
    }

    private PartitioningScheme canonicalize(PartitioningScheme scheme, PlanNode source)
    {
        return new PartitioningScheme(translateVariable(scheme.getPartitioning(), this::map),
                mapAndDistinctVariable(source.getOutputVariables()),
                scheme.getHashColumn().map(this::map),
                scheme.isReplicateNullsAndAny(),
                scheme.isScaleWriters(),
                scheme.getEncoding(),
                scheme.getBucketToPartition());
    }

    private StatisticAggregations map(StatisticAggregations statisticAggregations)
    {
        Map<VariableReferenceExpression, Aggregation> aggregations = statisticAggregations.getAggregations().entrySet().stream()
                .collect(toImmutableMap(entry -> map(entry.getKey()), entry -> map(entry.getValue())));
        return new StatisticAggregations(aggregations, mapAndDistinctVariable(statisticAggregations.getGroupingVariables()));
    }

    private StatisticAggregationsDescriptor<VariableReferenceExpression> map(StatisticAggregationsDescriptor<VariableReferenceExpression> descriptor)
    {
        return descriptor.map(this::map);
    }

    private List<Symbol> mapAndDistinctSymbol(List<Symbol> outputs)
    {
        Set<Symbol> added = new HashSet<>();
        ImmutableList.Builder<Symbol> builder = ImmutableList.builder();
        for (Symbol symbol : outputs) {
            Symbol canonical = map(symbol);
            if (added.add(canonical)) {
                builder.add(canonical);
            }
        }
        return builder.build();
    }

    private List<VariableReferenceExpression> mapAndDistinctVariable(List<VariableReferenceExpression> outputs)
    {
        Set<VariableReferenceExpression> added = new HashSet<>();
        ImmutableList.Builder<VariableReferenceExpression> builder = ImmutableList.builder();
        for (VariableReferenceExpression variable : outputs) {
            VariableReferenceExpression canonical = map(variable);
            if (added.add(canonical)) {
                builder.add(canonical);
            }
        }
        return builder.build();
    }

    private SpecificationWithPreSortedPrefix mapAndDistinct(DataOrganizationSpecification specification, int preSorted)
    {
        Optional<OrderingSchemeWithPreSortedPrefix> newOrderingScheme = specification.getOrderingScheme()
                .map(orderingScheme -> map(orderingScheme, preSorted));

        return new SpecificationWithPreSortedPrefix(
                new DataOrganizationSpecification(
                        mapAndDistinctVariable(specification.getPartitionBy()),
                        newOrderingScheme.map(OrderingSchemeWithPreSortedPrefix::getOrderingScheme)),
                newOrderingScheme.map(OrderingSchemeWithPreSortedPrefix::getPreSorted).orElse(preSorted));
    }

    DataOrganizationSpecification mapAndDistinct(DataOrganizationSpecification specification)
    {
        return new DataOrganizationSpecification(
                mapAndDistinctVariable(specification.getPartitionBy()),
                specification.getOrderingScheme().map(this::map));
    }

    public static SymbolMapper.Builder builder(WarningCollector warningCollector)
    {
        return new Builder(warningCollector);
    }

    public static SymbolMapper.Builder builder()
    {
        return new Builder(WarningCollector.NOOP);
    }

    public static class Builder
    {
        private final ImmutableMap.Builder<VariableReferenceExpression, VariableReferenceExpression> mappingsBuilder;
        private final WarningCollector warningCollector;

        public Builder(WarningCollector warningCollector)
        {
            this.warningCollector = warningCollector;
            this.mappingsBuilder = ImmutableMap.builder();
        }

        public SymbolMapper build()
        {
            return new SymbolMapper(mappingsBuilder.build(), warningCollector);
        }

        public void put(VariableReferenceExpression from, VariableReferenceExpression to)
        {
            mappingsBuilder.put(from, to);
        }
    }

    private static class OrderingSchemeWithPreSortedPrefix
    {
        private final OrderingScheme orderingScheme;
        private final int preSorted;

        public OrderingSchemeWithPreSortedPrefix(OrderingScheme orderingScheme, int preSorted)
        {
            this.orderingScheme = requireNonNull(orderingScheme, "orderingScheme is null");
            this.preSorted = preSorted;
        }

        public OrderingScheme getOrderingScheme()
        {
            return orderingScheme;
        }

        public int getPreSorted()
        {
            return preSorted;
        }
    }

    private static class SpecificationWithPreSortedPrefix
    {
        private final DataOrganizationSpecification specification;
        private final int preSorted;

        public SpecificationWithPreSortedPrefix(DataOrganizationSpecification specification, int preSorted)
        {
            this.specification = requireNonNull(specification, "specification is null");
            this.preSorted = preSorted;
        }

        public DataOrganizationSpecification getSpecification()
        {
            return specification;
        }

        public int getPreSorted()
        {
            return preSorted;
        }
    }
}
