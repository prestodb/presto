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

import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableLayout;
import com.facebook.presto.metadata.TableLayout.TablePartitioning;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConstantProperty;
import com.facebook.presto.spi.GroupingProperty;
import com.facebook.presto.spi.LocalProperty;
import com.facebook.presto.spi.SortingProperty;
import com.facebook.presto.spi.predicate.NullableValue;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.DomainTranslator;
import com.facebook.presto.sql.planner.ExpressionInterpreter;
import com.facebook.presto.sql.planner.NoOpSymbolResolver;
import com.facebook.presto.sql.planner.OrderingScheme;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.optimizations.ActualProperties.Global;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.AssignUniqueId;
import com.facebook.presto.sql.planner.plan.DeleteNode;
import com.facebook.presto.sql.planner.plan.DistinctLimitNode;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.ExplainAnalyzeNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.GroupIdNode;
import com.facebook.presto.sql.planner.plan.IndexJoinNode;
import com.facebook.presto.sql.planner.plan.IndexSourceNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LateralJoinNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.MarkDistinctNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.RowNumberNode;
import com.facebook.presto.sql.planner.plan.SampleNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.TableFinishNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.TableWriterNode;
import com.facebook.presto.sql.planner.plan.TopNNode;
import com.facebook.presto.sql.planner.plan.TopNRowNumberNode;
import com.facebook.presto.sql.planner.plan.UnnestNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.SystemSessionProperties.planWithTableNodePartitioning;
import static com.facebook.presto.spi.predicate.TupleDomain.extractFixedValues;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypes;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.ARBITRARY_DISTRIBUTION;
import static com.facebook.presto.sql.planner.optimizations.ActualProperties.Global.arbitraryPartition;
import static com.facebook.presto.sql.planner.optimizations.ActualProperties.Global.coordinatorSingleStreamPartition;
import static com.facebook.presto.sql.planner.optimizations.ActualProperties.Global.partitionedOn;
import static com.facebook.presto.sql.planner.optimizations.ActualProperties.Global.singleStreamPartition;
import static com.facebook.presto.sql.planner.optimizations.ActualProperties.Global.streamPartitionedOn;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

public class PropertyDerivations
{
    private PropertyDerivations() {}

    public static ActualProperties derivePropertiesRecursively(PlanNode node, Metadata metadata, Session session, TypeProvider types, SqlParser parser)
    {
        List<ActualProperties> inputProperties = node.getSources().stream()
                .map(source -> derivePropertiesRecursively(source, metadata, session, types, parser))
                .collect(toImmutableList());
        return deriveProperties(node, inputProperties, metadata, session, types, parser);
    }

    public static ActualProperties deriveProperties(PlanNode node, List<ActualProperties> inputProperties, Metadata metadata, Session session, TypeProvider types, SqlParser parser)
    {
        ActualProperties output = node.accept(new Visitor(metadata, session, types, parser), inputProperties);

        output.getNodePartitioning().ifPresent(partitioning ->
                verify(node.getOutputSymbols().containsAll(partitioning.getColumns()), "Node-level partitioning properties contain columns not present in node's output"));

        verify(node.getOutputSymbols().containsAll(output.getConstants().keySet()), "Node-level constant properties contain columns not present in node's output");

        Set<Symbol> localPropertyColumns = output.getLocalProperties().stream()
                .flatMap(property -> property.getColumns().stream())
                .collect(Collectors.toSet());

        verify(node.getOutputSymbols().containsAll(localPropertyColumns), "Node-level local properties contain columns not present in node's output");
        return output;
    }

    public static ActualProperties streamBackdoorDeriveProperties(PlanNode node, List<ActualProperties> inputProperties, Metadata metadata, Session session, TypeProvider types, SqlParser parser)
    {
        return node.accept(new Visitor(metadata, session, types, parser), inputProperties);
    }

    private static class Visitor
            extends PlanVisitor<ActualProperties, List<ActualProperties>>
    {
        private final Metadata metadata;
        private final Session session;
        private final TypeProvider types;
        private final SqlParser parser;

        public Visitor(Metadata metadata, Session session, TypeProvider types, SqlParser parser)
        {
            this.metadata = metadata;
            this.session = session;
            this.types = types;
            this.parser = parser;
        }

        @Override
        protected ActualProperties visitPlan(PlanNode node, List<ActualProperties> inputProperties)
        {
            throw new UnsupportedOperationException("not yet implemented: " + node.getClass().getName());
        }

        @Override
        public ActualProperties visitExplainAnalyze(ExplainAnalyzeNode node, List<ActualProperties> inputProperties)
        {
            return ActualProperties.builder()
                    .global(coordinatorSingleStreamPartition())
                    .build();
        }

        @Override
        public ActualProperties visitOutput(OutputNode node, List<ActualProperties> inputProperties)
        {
            return Iterables.getOnlyElement(inputProperties)
                    .translate(column -> PropertyDerivations.filterIfMissing(node.getOutputSymbols(), column));
        }

        @Override
        public ActualProperties visitEnforceSingleRow(EnforceSingleRowNode node, List<ActualProperties> inputProperties)
        {
            return Iterables.getOnlyElement(inputProperties);
        }

        @Override
        public ActualProperties visitAssignUniqueId(AssignUniqueId node, List<ActualProperties> inputProperties)
        {
            ActualProperties properties = Iterables.getOnlyElement(inputProperties);

            ImmutableList.Builder<LocalProperty<Symbol>> newLocalProperties = ImmutableList.builder();
            newLocalProperties.addAll(properties.getLocalProperties());
            newLocalProperties.add(new GroupingProperty<>(ImmutableList.of(node.getIdColumn())));
            node.getSource().getOutputSymbols().stream()
                    .forEach(column -> newLocalProperties.add(new ConstantProperty<>(column)));

            if (properties.getNodePartitioning().isPresent()) {
                // preserve input (possibly preferred) partitioning
                return ActualProperties.builderFrom(properties)
                        .local(newLocalProperties.build())
                        .build();
            }

            return ActualProperties.builderFrom(properties)
                    .global(partitionedOn(ARBITRARY_DISTRIBUTION, ImmutableList.of(node.getIdColumn()), Optional.empty()))
                    .local(newLocalProperties.build())
                    .build();
        }

        @Override
        public ActualProperties visitApply(ApplyNode node, List<ActualProperties> inputProperties)
        {
            throw new IllegalArgumentException("Unexpected node: " + node.getClass().getName());
        }

        @Override
        public ActualProperties visitLateralJoin(LateralJoinNode node, List<ActualProperties> inputProperties)
        {
            throw new IllegalArgumentException("Unexpected node: " + node.getClass().getName());
        }

        @Override
        public ActualProperties visitMarkDistinct(MarkDistinctNode node, List<ActualProperties> inputProperties)
        {
            return Iterables.getOnlyElement(inputProperties);
        }

        @Override
        public ActualProperties visitWindow(WindowNode node, List<ActualProperties> inputProperties)
        {
            ActualProperties properties = Iterables.getOnlyElement(inputProperties);

            // If the input is completely pre-partitioned and sorted, then the original input properties will be respected
            Optional<OrderingScheme> orderingScheme = node.getOrderingScheme();
            if (ImmutableSet.copyOf(node.getPartitionBy()).equals(node.getPrePartitionedInputs())
                    && (!orderingScheme.isPresent() || node.getPreSortedOrderPrefix() == orderingScheme.get().getOrderBy().size())) {
                return properties;
            }

            ImmutableList.Builder<LocalProperty<Symbol>> localProperties = ImmutableList.builder();

            // If the WindowNode has pre-partitioned inputs, then it will not change the order of those inputs at output,
            // so we should just propagate those underlying local properties that guarantee the pre-partitioning.
            // TODO: come up with a more general form of this operation for other streaming operators
            if (!node.getPrePartitionedInputs().isEmpty()) {
                GroupingProperty<Symbol> prePartitionedProperty = new GroupingProperty<>(node.getPrePartitionedInputs());
                for (LocalProperty<Symbol> localProperty : properties.getLocalProperties()) {
                    if (!prePartitionedProperty.isSimplifiedBy(localProperty)) {
                        break;
                    }
                    localProperties.add(localProperty);
                }
            }

            if (!node.getPartitionBy().isEmpty()) {
                localProperties.add(new GroupingProperty<>(node.getPartitionBy()));
            }

            orderingScheme.ifPresent(scheme ->
                    scheme.getOrderBy().stream()
                            .map(column -> new SortingProperty<>(column, scheme.getOrdering(column)))
                            .forEach(localProperties::add));

            return ActualProperties.builderFrom(properties)
                    .local(LocalProperties.normalizeAndPrune(localProperties.build()))
                    .build();
        }

        @Override
        public ActualProperties visitGroupId(GroupIdNode node, List<ActualProperties> inputProperties)
        {
            Map<Symbol, Symbol> inputToOutputMappings = new HashMap<>();
            for (Map.Entry<Symbol, Symbol> setMapping : node.getGroupingColumns().entrySet()) {
                if (node.getCommonGroupingColumns().contains(setMapping.getKey())) {
                    // TODO: Add support for translating a property on a single column to multiple columns
                    // when GroupIdNode is copying a single input grouping column into multiple output grouping columns (i.e. aliases), this is basically picking one arbitrarily
                    inputToOutputMappings.putIfAbsent(setMapping.getValue(), setMapping.getKey());
                }
            }

            // TODO: Add support for translating a property on a single column to multiple columns
            // this is deliberately placed after the grouping columns, because preserving properties has a bigger perf impact
            for (Symbol argument : node.getAggregationArguments()) {
                inputToOutputMappings.putIfAbsent(argument, argument);
            }

            return Iterables.getOnlyElement(inputProperties).translate(column -> Optional.ofNullable(inputToOutputMappings.get(column)));
        }

        @Override
        public ActualProperties visitAggregation(AggregationNode node, List<ActualProperties> inputProperties)
        {
            ActualProperties properties = Iterables.getOnlyElement(inputProperties);

            ActualProperties translated = properties.translate(symbol -> node.getGroupingKeys().contains(symbol) ? Optional.of(symbol) : Optional.empty());

            return ActualProperties.builderFrom(translated)
                    .local(LocalProperties.grouped(node.getGroupingKeys()))
                    .build();
        }

        @Override
        public ActualProperties visitRowNumber(RowNumberNode node, List<ActualProperties> inputProperties)
        {
            return Iterables.getOnlyElement(inputProperties);
        }

        @Override
        public ActualProperties visitTopNRowNumber(TopNRowNumberNode node, List<ActualProperties> inputProperties)
        {
            ActualProperties properties = Iterables.getOnlyElement(inputProperties);

            ImmutableList.Builder<LocalProperty<Symbol>> localProperties = ImmutableList.builder();
            localProperties.add(new GroupingProperty<>(node.getPartitionBy()));
            for (Symbol column : node.getOrderingScheme().getOrderBy()) {
                localProperties.add(new SortingProperty<>(column, node.getOrderingScheme().getOrdering(column)));
            }

            return ActualProperties.builderFrom(properties)
                    .local(localProperties.build())
                    .build();
        }

        @Override
        public ActualProperties visitTopN(TopNNode node, List<ActualProperties> inputProperties)
        {
            ActualProperties properties = Iterables.getOnlyElement(inputProperties);

            List<SortingProperty<Symbol>> localProperties = node.getOrderingScheme().getOrderBy().stream()
                    .map(column -> new SortingProperty<>(column, node.getOrderingScheme().getOrdering(column)))
                    .collect(toImmutableList());

            return ActualProperties.builderFrom(properties)
                    .local(localProperties)
                    .build();
        }

        @Override
        public ActualProperties visitSort(SortNode node, List<ActualProperties> inputProperties)
        {
            ActualProperties properties = Iterables.getOnlyElement(inputProperties);

            List<SortingProperty<Symbol>> localProperties = node.getOrderingScheme().getOrderBy().stream()
                    .map(column -> new SortingProperty<>(column, node.getOrderingScheme().getOrdering(column)))
                    .collect(toImmutableList());

            return ActualProperties.builderFrom(properties)
                    .local(localProperties)
                    .build();
        }

        @Override
        public ActualProperties visitLimit(LimitNode node, List<ActualProperties> inputProperties)
        {
            return Iterables.getOnlyElement(inputProperties);
        }

        @Override
        public ActualProperties visitDistinctLimit(DistinctLimitNode node, List<ActualProperties> inputProperties)
        {
            ActualProperties properties = Iterables.getOnlyElement(inputProperties);

            return ActualProperties.builderFrom(properties)
                    .local(LocalProperties.grouped(node.getDistinctSymbols()))
                    .build();
        }

        @Override
        public ActualProperties visitTableFinish(TableFinishNode node, List<ActualProperties> inputProperties)
        {
            return ActualProperties.builder()
                    .global(coordinatorSingleStreamPartition())
                    .build();
        }

        @Override
        public ActualProperties visitDelete(DeleteNode node, List<ActualProperties> inputProperties)
        {
            // drop all symbols in property because delete doesn't pass on any of the columns
            return Iterables.getOnlyElement(inputProperties).translate(symbol -> Optional.empty());
        }

        @Override
        public ActualProperties visitJoin(JoinNode node, List<ActualProperties> inputProperties)
        {
            ActualProperties probeProperties = inputProperties.get(0);
            ActualProperties buildProperties = inputProperties.get(1);

            boolean unordered = spillPossible(session, node.getType());

            switch (node.getType()) {
                case INNER:
                    probeProperties = probeProperties.translate(column -> filterOrRewrite(node.getOutputSymbols(), node.getCriteria(), column));
                    buildProperties = buildProperties.translate(column -> filterOrRewrite(node.getOutputSymbols(), node.getCriteria(), column));

                    Map<Symbol, NullableValue> constants = new HashMap<>();
                    constants.putAll(probeProperties.getConstants());
                    constants.putAll(buildProperties.getConstants());

                    if (node.isCrossJoin()) {
                        // Cross join preserves only constants from probe and build sides.
                        // Cross join doesn't preserve sorting or grouping local properties on either side.
                        return ActualProperties.builder()
                                .global(probeProperties)
                                .local(ImmutableList.of())
                                .constants(constants)
                                .build();
                    }

                    return ActualProperties.builderFrom(probeProperties)
                            .constants(constants)
                            .unordered(unordered)
                            .build();
                case LEFT:
                    return ActualProperties.builderFrom(probeProperties.translate(column -> filterIfMissing(node.getOutputSymbols(), column)))
                            .unordered(unordered)
                            .build();
                case RIGHT:
                    buildProperties = buildProperties.translate(column -> filterIfMissing(node.getOutputSymbols(), column));

                    return ActualProperties.builderFrom(buildProperties.translate(column -> filterIfMissing(node.getOutputSymbols(), column)))
                            .local(ImmutableList.of())
                            .unordered(unordered)
                            .build();
                case FULL:
                    // We can't say anything about the partitioning scheme because any partition of
                    // a hash-partitioned join can produce nulls in case of a lack of matches
                    return ActualProperties.builder()
                            .global(probeProperties.isSingleNode() ? singleStreamPartition() : arbitraryPartition())
                            .unordered(unordered)
                            .build();
                default:
                    throw new UnsupportedOperationException("Unsupported join type: " + node.getType());
            }
        }

        @Override
        public ActualProperties visitSemiJoin(SemiJoinNode node, List<ActualProperties> inputProperties)
        {
            return inputProperties.get(0);
        }

        @Override
        public ActualProperties visitIndexJoin(IndexJoinNode node, List<ActualProperties> inputProperties)
        {
            // TODO: include all equivalent columns in partitioning properties
            ActualProperties probeProperties = inputProperties.get(0);
            ActualProperties indexProperties = inputProperties.get(1);

            switch (node.getType()) {
                case INNER:
                    return ActualProperties.builderFrom(probeProperties)
                            .constants(ImmutableMap.<Symbol, NullableValue>builder()
                                    .putAll(probeProperties.getConstants())
                                    .putAll(indexProperties.getConstants())
                                    .build())
                            .build();
                case SOURCE_OUTER:
                    return ActualProperties.builderFrom(probeProperties)
                            .constants(probeProperties.getConstants())
                            .build();
                default:
                    throw new UnsupportedOperationException("Unsupported join type: " + node.getType());
            }
        }

        @Override
        public ActualProperties visitIndexSource(IndexSourceNode node, List<ActualProperties> context)
        {
            return ActualProperties.builder()
                    .global(singleStreamPartition())
                    .build();
        }

        public static Map<Symbol, Symbol> exchangeInputToOutput(ExchangeNode node, int sourceIndex)
        {
            List<Symbol> inputSymbols = node.getInputs().get(sourceIndex);
            Map<Symbol, Symbol> inputToOutput = new HashMap<>();
            for (int i = 0; i < node.getOutputSymbols().size(); i++) {
                inputToOutput.put(inputSymbols.get(i), node.getOutputSymbols().get(i));
            }
            return inputToOutput;
        }

        @Override
        public ActualProperties visitExchange(ExchangeNode node, List<ActualProperties> inputProperties)
        {
            checkArgument(node.getScope() != REMOTE || inputProperties.stream().noneMatch(ActualProperties::isNullsAndAnyReplicated), "Null-and-any replicated inputs should not be remotely exchanged");

            Set<Map.Entry<Symbol, NullableValue>> entries = null;
            for (int sourceIndex = 0; sourceIndex < node.getSources().size(); sourceIndex++) {
                Map<Symbol, Symbol> inputToOutput = exchangeInputToOutput(node, sourceIndex);
                ActualProperties translated = inputProperties.get(sourceIndex).translate(symbol -> Optional.ofNullable(inputToOutput.get(symbol)));

                entries = (entries == null) ? translated.getConstants().entrySet() : Sets.intersection(entries, translated.getConstants().entrySet());
            }
            checkState(entries != null);

            Map<Symbol, NullableValue> constants = entries.stream()
                    .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));

            ImmutableList.Builder<SortingProperty<Symbol>> localProperties = ImmutableList.builder();
            if (node.getOrderingScheme().isPresent()) {
                node.getOrderingScheme().get().getOrderBy().stream()
                        .map(column -> new SortingProperty<>(column, node.getOrderingScheme().get().getOrdering(column)))
                        .forEach(localProperties::add);
            }

            // Local exchanges are only created in AddLocalExchanges, at the end of optimization, and
            // local exchanges do not produce all global properties as represented by ActualProperties.
            // This is acceptable because AddLocalExchanges does not use global properties and is only
            // interested in the local properties.
            // However, for the purpose of validation, some global properties (single-node vs distributed)
            // are computed for local exchanges.
            // TODO: implement full properties for local exchanges
            if (node.getScope() == LOCAL) {
                ActualProperties.Builder builder = ActualProperties.builder();
                builder.local(localProperties.build());
                builder.constants(constants);

                if (inputProperties.stream().anyMatch(ActualProperties::isCoordinatorOnly)) {
                    builder.global(coordinatorSingleStreamPartition());
                }
                else if (inputProperties.stream().anyMatch(ActualProperties::isSingleNode)) {
                    builder.global(coordinatorSingleStreamPartition());
                }

                return builder.build();
            }

            switch (node.getType()) {
                case GATHER:
                    boolean coordinatorOnly = node.getPartitioningScheme().getPartitioning().getHandle().isCoordinatorOnly();
                    return ActualProperties.builder()
                            .global(coordinatorOnly ? coordinatorSingleStreamPartition() : singleStreamPartition())
                            .local(localProperties.build())
                            .constants(constants)
                            .build();
                case REPARTITION:
                    return ActualProperties.builder()
                            .global(partitionedOn(
                                    node.getPartitioningScheme().getPartitioning(),
                                    Optional.of(node.getPartitioningScheme().getPartitioning()))
                                    .withReplicatedNulls(node.getPartitioningScheme().isReplicateNullsAndAny()))
                            .constants(constants)
                            .build();
                case REPLICATE:
                    // TODO: this should have the same global properties as the stream taking the replicated data
                    return ActualProperties.builder()
                            .global(arbitraryPartition())
                            .constants(constants)
                            .build();
            }

            throw new UnsupportedOperationException("not yet implemented");
        }

        @Override
        public ActualProperties visitFilter(FilterNode node, List<ActualProperties> inputProperties)
        {
            ActualProperties properties = Iterables.getOnlyElement(inputProperties);

            DomainTranslator.ExtractionResult decomposedPredicate = DomainTranslator.fromPredicate(
                    metadata,
                    session,
                    node.getPredicate(),
                    types);

            Map<Symbol, NullableValue> constants = new HashMap<>(properties.getConstants());
            constants.putAll(extractFixedValues(decomposedPredicate.getTupleDomain()).orElse(ImmutableMap.of()));

            return ActualProperties.builderFrom(properties)
                    .constants(constants)
                    .build();
        }

        @Override
        public ActualProperties visitProject(ProjectNode node, List<ActualProperties> inputProperties)
        {
            ActualProperties properties = Iterables.getOnlyElement(inputProperties);

            Map<Symbol, Symbol> identities = computeIdentityTranslations(node.getAssignments().getMap());

            ActualProperties translatedProperties = properties.translate(column -> Optional.ofNullable(identities.get(column)));

            // Extract additional constants
            Map<Symbol, NullableValue> constants = new HashMap<>();
            for (Map.Entry<Symbol, Expression> assignment : node.getAssignments().entrySet()) {
                Expression expression = assignment.getValue();

                Map<NodeRef<Expression>, Type> expressionTypes = getExpressionTypes(session, metadata, parser, types, expression, emptyList() /* parameters already replaced */);
                Type type = requireNonNull(expressionTypes.get(NodeRef.of(expression)));
                ExpressionInterpreter optimizer = ExpressionInterpreter.expressionOptimizer(expression, metadata, session, expressionTypes);
                // TODO:
                // We want to use a symbol resolver that looks up in the constants from the input subplan
                // to take advantage of constant-folding for complex expressions
                // However, that currently causes errors when those expressions operate on arrays or row types
                // ("ROW comparison not supported for fields with null elements", etc)
                Object value = optimizer.optimize(NoOpSymbolResolver.INSTANCE);

                if (value instanceof SymbolReference) {
                    Symbol symbol = Symbol.from((SymbolReference) value);
                    NullableValue existingConstantValue = constants.get(symbol);
                    if (existingConstantValue != null) {
                        constants.put(assignment.getKey(), new NullableValue(type, value));
                    }
                }
                else if (!(value instanceof Expression)) {
                    constants.put(assignment.getKey(), new NullableValue(type, value));
                }
            }
            constants.putAll(translatedProperties.getConstants());

            return ActualProperties.builderFrom(translatedProperties)
                    .constants(constants)
                    .build();
        }

        @Override
        public ActualProperties visitTableWriter(TableWriterNode node, List<ActualProperties> inputProperties)
        {
            ActualProperties properties = Iterables.getOnlyElement(inputProperties);

            if (properties.isCoordinatorOnly()) {
                return ActualProperties.builder()
                        .global(coordinatorSingleStreamPartition())
                        .build();
            }
            return ActualProperties.builder()
                    .global(properties.isSingleNode() ? singleStreamPartition() : arbitraryPartition())
                    .build();
        }

        @Override
        public ActualProperties visitSample(SampleNode node, List<ActualProperties> inputProperties)
        {
            return Iterables.getOnlyElement(inputProperties);
        }

        @Override
        public ActualProperties visitUnnest(UnnestNode node, List<ActualProperties> inputProperties)
        {
            Set<Symbol> passThroughInputs = ImmutableSet.copyOf(node.getReplicateSymbols());

            return Iterables.getOnlyElement(inputProperties).translate(column -> {
                if (passThroughInputs.contains(column)) {
                    return Optional.of(column);
                }
                return Optional.empty();
            });
        }

        @Override
        public ActualProperties visitValues(ValuesNode node, List<ActualProperties> context)
        {
            return ActualProperties.builder()
                    .global(singleStreamPartition())
                    .build();
        }

        @Override
        public ActualProperties visitTableScan(TableScanNode node, List<ActualProperties> inputProperties)
        {
            checkArgument(node.getLayout().isPresent(), "table layout has not yet been chosen");

            TableLayout layout = metadata.getLayout(session, node.getLayout().get());
            Map<ColumnHandle, Symbol> assignments = ImmutableBiMap.copyOf(node.getAssignments()).inverse();

            ActualProperties.Builder properties = ActualProperties.builder();

            // Globally constant assignments
            Map<ColumnHandle, NullableValue> globalConstants = new HashMap<>();
            extractFixedValues(node.getCurrentConstraint()).orElse(ImmutableMap.of())
                    .entrySet().stream()
                    .filter(entry -> !entry.getValue().isNull())
                    .forEach(entry -> globalConstants.put(entry.getKey(), entry.getValue()));

            Map<Symbol, NullableValue> symbolConstants = globalConstants.entrySet().stream()
                    .filter(entry -> assignments.containsKey(entry.getKey()))
                    .collect(toMap(entry -> assignments.get(entry.getKey()), Map.Entry::getValue));
            properties.constants(symbolConstants);

            // Partitioning properties
            properties.global(deriveGlobalProperties(layout, assignments, globalConstants));

            // Append the global constants onto the local properties to maximize their translation potential
            List<LocalProperty<ColumnHandle>> constantAppendedLocalProperties = ImmutableList.<LocalProperty<ColumnHandle>>builder()
                    .addAll(globalConstants.keySet().stream().map(column -> new ConstantProperty<>(column)).iterator())
                    .addAll(layout.getLocalProperties())
                    .build();
            properties.local(LocalProperties.translate(constantAppendedLocalProperties, column -> Optional.ofNullable(assignments.get(column))));

            return properties.build();
        }

        private Global deriveGlobalProperties(TableLayout layout, Map<ColumnHandle, Symbol> assignments, Map<ColumnHandle, NullableValue> constants)
        {
            Optional<List<Symbol>> streamPartitioning = layout.getStreamPartitioningColumns()
                    .flatMap(columns -> translateToNonConstantSymbols(columns, assignments, constants));

            if (planWithTableNodePartitioning(session) && layout.getTablePartitioning().isPresent()) {
                TablePartitioning tablePartitioning = layout.getTablePartitioning().get();
                if (assignments.keySet().containsAll(tablePartitioning.getPartitioningColumns())) {
                    List<Symbol> arguments = tablePartitioning.getPartitioningColumns().stream()
                            .map(assignments::get)
                            .collect(toImmutableList());

                    return partitionedOn(tablePartitioning.getPartitioningHandle(), arguments, streamPartitioning);
                }
            }

            if (streamPartitioning.isPresent()) {
                return streamPartitionedOn(streamPartitioning.get());
            }
            return arbitraryPartition();
        }

        private static Optional<List<Symbol>> translateToNonConstantSymbols(
                Set<ColumnHandle> columnHandles,
                Map<ColumnHandle, Symbol> assignments,
                Map<ColumnHandle, NullableValue> globalConstants)
        {
            // Strip off the constants from the partitioning columns (since those are not required for translation)
            Set<ColumnHandle> constantsStrippedColumns = columnHandles.stream()
                    .filter(column -> !globalConstants.containsKey(column))
                    .collect(toImmutableSet());

            ImmutableSet.Builder<Symbol> builder = ImmutableSet.builder();
            for (ColumnHandle column : constantsStrippedColumns) {
                Symbol translated = assignments.get(column);
                if (translated == null) {
                    return Optional.empty();
                }
                builder.add(translated);
            }

            return Optional.of(ImmutableList.copyOf(builder.build()));
        }

        private static Map<Symbol, Symbol> computeIdentityTranslations(Map<Symbol, Expression> assignments)
        {
            Map<Symbol, Symbol> inputToOutput = new HashMap<>();
            for (Map.Entry<Symbol, Expression> assignment : assignments.entrySet()) {
                if (assignment.getValue() instanceof SymbolReference) {
                    inputToOutput.put(Symbol.from(assignment.getValue()), assignment.getKey());
                }
            }
            return inputToOutput;
        }
    }

    static boolean spillPossible(Session session, JoinNode.Type joinType)
    {
        if (!SystemSessionProperties.isSpillEnabled(session)) {
            return false;
        }
        switch (joinType) {
            case INNER:
            case LEFT:
                return true;
            case RIGHT:
            case FULL:
                // Currently there is no spill support for outer on the build side.
                return false;
            default:
                throw new IllegalStateException("Unknown join type: " + joinType);
        }
    }

    public static Optional<Symbol> filterIfMissing(Collection<Symbol> columns, Symbol column)
    {
        if (columns.contains(column)) {
            return Optional.of(column);
        }

        return Optional.empty();
    }

    // Used to filter columns that are not exposed by join node
    // Or, if they are part of the equalities, to translate them
    // to the other symbol if that's exposed, instead.
    public static Optional<Symbol> filterOrRewrite(Collection<Symbol> columns, Collection<JoinNode.EquiJoinClause> equalities, Symbol column)
    {
        // symbol is exposed directly, so no translation needed
        if (columns.contains(column)) {
            return Optional.of(column);
        }

        // if the column is part of the equality conditions and its counterpart
        // is exposed, use that, instead
        for (JoinNode.EquiJoinClause equality : equalities) {
            if (equality.getLeft().equals(column) && columns.contains(equality.getRight())) {
                return Optional.of(equality.getRight());
            }
            else if (equality.getRight().equals(column) && columns.contains(equality.getLeft())) {
                return Optional.of(equality.getLeft());
            }
        }

        return Optional.empty();
    }
}
