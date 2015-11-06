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
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableLayout;
import com.facebook.presto.metadata.TableLayout.NodePartitioning;
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
import com.facebook.presto.sql.planner.PartitionFunctionBinding.PartitionFunctionArgumentBinding;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.optimizations.ActualProperties.Global;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.DeleteNode;
import com.facebook.presto.sql.planner.plan.DistinctLimitNode;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.GroupIdNode;
import com.facebook.presto.sql.planner.plan.IndexJoinNode;
import com.facebook.presto.sql.planner.plan.IndexSourceNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
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
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.planWithTableNodePartitioning;
import static com.facebook.presto.spi.predicate.TupleDomain.extractFixedValues;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypes;
import static com.facebook.presto.sql.planner.optimizations.ActualProperties.Global.arbitraryPartition;
import static com.facebook.presto.sql.planner.optimizations.ActualProperties.Global.coordinatorSingleStreamPartition;
import static com.facebook.presto.sql.planner.optimizations.ActualProperties.Global.partitionedOn;
import static com.facebook.presto.sql.planner.optimizations.ActualProperties.Global.singleStreamPartition;
import static com.facebook.presto.sql.planner.optimizations.ActualProperties.Global.streamPartitionedOn;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableSet;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

class PropertyDerivations
{
    private PropertyDerivations() {}

    public static ActualProperties deriveProperties(PlanNode node, ActualProperties inputProperties, Metadata metadata, Session session, Map<Symbol, Type> types, SqlParser parser)
    {
        return deriveProperties(node, ImmutableList.of(inputProperties), metadata, session, types, parser);
    }

    public static ActualProperties deriveProperties(PlanNode node, List<ActualProperties> inputProperties, Metadata metadata, Session session, Map<Symbol, Type> types, SqlParser parser)
    {
        return node.accept(new Visitor(metadata, session, types, parser), inputProperties);
    }

    private static class Visitor
            extends PlanVisitor<List<ActualProperties>, ActualProperties>
    {
        private final Metadata metadata;
        private final Session session;
        private final Map<Symbol, Type> types;
        private final SqlParser parser;

        public Visitor(Metadata metadata, Session session, Map<Symbol, Type> types, SqlParser parser)
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
        public ActualProperties visitOutput(OutputNode node, List<ActualProperties> inputProperties)
        {
            return Iterables.getOnlyElement(inputProperties);
        }

        @Override
        public ActualProperties visitEnforceSingleRow(EnforceSingleRowNode node, List<ActualProperties> inputProperties)
        {
            return Iterables.getOnlyElement(inputProperties);
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
            if (ImmutableSet.copyOf(node.getPartitionBy()).equals(node.getPrePartitionedInputs()) && node.getPreSortedOrderPrefix() == node.getOrderBy().size()) {
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
            for (Symbol column : node.getOrderBy()) {
                localProperties.add(new SortingProperty<>(column, node.getOrderings().get(column)));
            }

            return ActualProperties.builderFrom(properties)
                    .local(LocalProperties.normalizeAndPrune(localProperties.build()))
                    .build();
        }

        @Override
        public ActualProperties visitGroupId(GroupIdNode node, List<ActualProperties> inputProperties)
        {
            return Iterables.getOnlyElement(inputProperties);
        }

        @Override
        public ActualProperties visitAggregation(AggregationNode node, List<ActualProperties> inputProperties)
        {
            ActualProperties properties = Iterables.getOnlyElement(inputProperties);

            ActualProperties translated = properties.translate(symbol -> node.getGroupBy().contains(symbol) ? Optional.of(symbol) : Optional.<Symbol>empty());

            return ActualProperties.builderFrom(translated)
                    .local(LocalProperties.grouped(node.getGroupBy()))
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
            for (Symbol column : node.getOrderBy()) {
                localProperties.add(new SortingProperty<>(column, node.getOrderings().get(column)));
            }

            return ActualProperties.builderFrom(properties)
                    .local(localProperties.build())
                    .build();
        }

        @Override
        public ActualProperties visitTopN(TopNNode node, List<ActualProperties> inputProperties)
        {
            ActualProperties properties = Iterables.getOnlyElement(inputProperties);

            List<SortingProperty<Symbol>> localProperties = node.getOrderBy().stream()
                    .map(column -> new SortingProperty<>(column, node.getOrderings().get(column)))
                    .collect(toImmutableList());

            return ActualProperties.builderFrom(properties)
                    .local(localProperties)
                    .build();
        }

        @Override
        public ActualProperties visitSort(SortNode node, List<ActualProperties> inputProperties)
        {
            ActualProperties properties = Iterables.getOnlyElement(inputProperties);

            List<SortingProperty<Symbol>> localProperties = node.getOrderBy().stream()
                    .map(column -> new SortingProperty<>(column, node.getOrderings().get(column)))
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
            // TODO: include all equivalent columns in partitioning properties
            ActualProperties probeProperties = inputProperties.get(0);
            ActualProperties buildProperties = inputProperties.get(1);

            switch (node.getType()) {
                case INNER:
                    return ActualProperties.builderFrom(probeProperties)
                            .constants(ImmutableMap.<Symbol, NullableValue>builder()
                                    .putAll(probeProperties.getConstants())
                                    .putAll(buildProperties.getConstants())
                                    .build())
                            .build();
                case LEFT:
                    return ActualProperties.builderFrom(probeProperties)
                            .constants(probeProperties.getConstants())
                            .build();
                case RIGHT:
                    return ActualProperties.builderFrom(buildProperties)
                            .constants(buildProperties.getConstants())
                            .local(ImmutableList.of())
                            .build();
                case FULL:
                    // We can't say anything about the partitioning scheme because any partition of
                    // a hash-partitioned join can produce nulls in case of a lack of matches
                    return ActualProperties.builder()
                            .global(probeProperties.isSingleNode() ? singleStreamPartition() : arbitraryPartition())
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
            Set<Map.Entry<Symbol, NullableValue>> entries = null;
            for (int sourceIndex = 0; sourceIndex < node.getSources().size(); sourceIndex++) {
                Map<Symbol, Symbol> inputToOutput = exchangeInputToOutput(node, sourceIndex);
                ActualProperties translated = inputProperties.get(sourceIndex).translate(symbol -> Optional.ofNullable(inputToOutput.get(symbol)));

                entries = (entries == null) ? translated.getConstants().entrySet() : Sets.intersection(entries, translated.getConstants().entrySet());
            }
            checkState(entries != null);

            Map<Symbol, NullableValue> constants = entries.stream()
                    .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));

            switch (node.getType()) {
                case GATHER:
                    return ActualProperties.builder()
                            .global(singleStreamPartition())
                            .constants(constants)
                            .build();
                case REPARTITION:
                    return ActualProperties.builder()
                            .global(partitionedOn(
                                    node.getPartitionFunction().getPartitioningHandle(),
                                    node.getPartitionFunction().getPartitionFunctionArguments(),
                                    Optional.of(node.getPartitionFunction().getPartitionFunctionArguments())))
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

            Map<Symbol, Symbol> identities = computeIdentityTranslations(node.getAssignments());

            ActualProperties translatedProperties = properties.translate(column -> Optional.ofNullable(identities.get(column)));

            // Extract additional constants
            Map<Symbol, NullableValue> constants = new HashMap<>();
            for (Map.Entry<Symbol, Expression> assignment : node.getAssignments().entrySet()) {
                Expression expression = assignment.getValue();

                IdentityHashMap<Expression, Type> expressionTypes = getExpressionTypes(session, metadata, parser, types, expression);
                Type type = requireNonNull(expressionTypes.get(expression));
                ExpressionInterpreter optimizer = ExpressionInterpreter.expressionOptimizer(expression, metadata, session, expressionTypes);
                // TODO:
                // We want to use a symbol resolver that looks up in the constants from the input subplan
                // to take advantage of constant-folding for complex expressions
                // However, that currently causes errors when those expressions operate on arrays or row types
                // ("ROW comparison not supported for fields with null elements", etc)
                Object value = optimizer.optimize(NoOpSymbolResolver.INSTANCE);

                if (value instanceof QualifiedNameReference) {
                    Symbol symbol = Symbol.fromQualifiedName(((QualifiedNameReference) value).getName());
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
            Optional<List<PartitionFunctionArgumentBinding>> partitioning = layout.getPartitioningColumns()
                    .flatMap(columns -> translateToNonConstantSymbols(columns, assignments, constants));

            if (planWithTableNodePartitioning(session) && layout.getNodePartitioning().isPresent()) {
                NodePartitioning nodePartitioning = layout.getNodePartitioning().get();
                if (assignments.keySet().containsAll(nodePartitioning.getPartitioningColumns())) {
                    List<PartitionFunctionArgumentBinding> arguments = nodePartitioning.getPartitioningColumns().stream()
                            .map(assignments::get)
                            .map(PartitionFunctionArgumentBinding::new)
                            .collect(toImmutableList());

                    return partitionedOn(nodePartitioning.getPartitioningHandle(), arguments, partitioning);
                }
            }

            if (partitioning.isPresent()) {
                return streamPartitionedOn(partitioning.get());
            }
            return arbitraryPartition();
        }

        private static Optional<List<PartitionFunctionArgumentBinding>> translateToNonConstantSymbols(
                Set<ColumnHandle> columnHandles,
                Map<ColumnHandle, Symbol> assignments,
                Map<ColumnHandle, NullableValue> globalConstants)
        {
            // Strip off the constants from the partitioning columns (since those are not required for translation)
            Set<ColumnHandle> constantsStrippedColumns = columnHandles.stream()
                    .filter(column -> !globalConstants.containsKey(column))
                    .collect(toImmutableSet());

            ImmutableSet.Builder<PartitionFunctionArgumentBinding> builder = ImmutableSet.builder();
            for (ColumnHandle column : constantsStrippedColumns) {
                Symbol translated = assignments.get(column);
                if (translated == null) {
                    return Optional.empty();
                }
                builder.add(new PartitionFunctionArgumentBinding(translated));
            }

            return Optional.of(ImmutableList.copyOf(builder.build()));
        }

        private static Map<Symbol, Symbol> computeIdentityTranslations(Map<Symbol, Expression> assignments)
        {
            Map<Symbol, Symbol> inputToOutput = new HashMap<>();
            for (Map.Entry<Symbol, Expression> assignment : assignments.entrySet()) {
                if (assignment.getValue() instanceof QualifiedNameReference) {
                    inputToOutput.put(Symbol.fromQualifiedName(((QualifiedNameReference) assignment.getValue()).getName()), assignment.getKey());
                }
            }
            return inputToOutput;
        }
    }
}
