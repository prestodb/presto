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
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableLayout;
import com.facebook.presto.metadata.TableLayout.TablePartitioning;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConstantProperty;
import com.facebook.presto.spi.GroupingProperty;
import com.facebook.presto.spi.LocalProperty;
import com.facebook.presto.spi.SortingProperty;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.DeleteNode;
import com.facebook.presto.spi.plan.DistinctLimitNode;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.IndexSourceNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.MarkDistinctNode;
import com.facebook.presto.spi.plan.MergeJoinNode;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.OutputNode;
import com.facebook.presto.spi.plan.PartitioningHandle;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.SemiJoinNode;
import com.facebook.presto.spi.plan.SortNode;
import com.facebook.presto.spi.plan.SpatialJoinNode;
import com.facebook.presto.spi.plan.TableFinishNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.TableWriterNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.plan.UnnestNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.plan.WindowNode;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.RowExpressionInterpreter;
import com.facebook.presto.sql.planner.optimizations.ActualProperties.Global;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.AssignUniqueId;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.ExplainAnalyzeNode;
import com.facebook.presto.sql.planner.plan.GroupIdNode;
import com.facebook.presto.sql.planner.plan.IndexJoinNode;
import com.facebook.presto.sql.planner.plan.InternalPlanVisitor;
import com.facebook.presto.sql.planner.plan.LateralJoinNode;
import com.facebook.presto.sql.planner.plan.MergeProcessorNode;
import com.facebook.presto.sql.planner.plan.MergeWriterNode;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.facebook.presto.sql.planner.plan.RowNumberNode;
import com.facebook.presto.sql.planner.plan.SampleNode;
import com.facebook.presto.sql.planner.plan.SequenceNode;
import com.facebook.presto.sql.planner.plan.StatisticsWriterNode;
import com.facebook.presto.sql.planner.plan.TableWriterMergeNode;
import com.facebook.presto.sql.planner.plan.TopNRowNumberNode;
import com.facebook.presto.sql.planner.plan.UpdateNode;
import com.facebook.presto.sql.relational.RowExpressionDomainTranslator;
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

import static com.facebook.presto.SystemSessionProperties.isJoinSpillingEnabled;
import static com.facebook.presto.SystemSessionProperties.isSpillEnabled;
import static com.facebook.presto.SystemSessionProperties.planWithTableNodePartitioning;
import static com.facebook.presto.common.predicate.TupleDomain.toLinkedMap;
import static com.facebook.presto.spi.relation.DomainTranslator.BASIC_COLUMN_EXTRACTOR;
import static com.facebook.presto.spi.relation.ExpressionOptimizer.Level.OPTIMIZED;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.ARBITRARY_DISTRIBUTION;
import static com.facebook.presto.sql.planner.optimizations.ActualProperties.Global.arbitraryPartition;
import static com.facebook.presto.sql.planner.optimizations.ActualProperties.Global.coordinatorSingleStreamPartition;
import static com.facebook.presto.sql.planner.optimizations.ActualProperties.Global.partitionedOn;
import static com.facebook.presto.sql.planner.optimizations.ActualProperties.Global.partitionedOnCoalesce;
import static com.facebook.presto.sql.planner.optimizations.ActualProperties.Global.singleStreamPartition;
import static com.facebook.presto.sql.planner.optimizations.ActualProperties.Global.streamPartitionedOn;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.stream.Collectors.toMap;

public class PropertyDerivations
{
    private PropertyDerivations() {}

    public static ActualProperties derivePropertiesRecursively(PlanNode node, Metadata metadata, Session session)
    {
        List<ActualProperties> inputProperties = node.getSources().stream()
                .map(source -> derivePropertiesRecursively(source, metadata, session))
                .collect(toImmutableList());
        return deriveProperties(node, inputProperties, metadata, session);
    }

    public static ActualProperties deriveProperties(PlanNode node, List<ActualProperties> inputProperties, Metadata metadata, Session session)
    {
        ActualProperties output = node.accept(new Visitor(metadata, session), inputProperties);

        output.getNodePartitioning().ifPresent(partitioning ->
                verify(node.getOutputVariables().containsAll(partitioning.getVariableReferences()), "Node-level partitioning properties contain columns not present in node's output"));

        verify(node.getOutputVariables().containsAll(output.getConstants().keySet()), "Node-level constant properties contain columns not present in node's output");

        Set<VariableReferenceExpression> localPropertyColumns = output.getLocalProperties().stream()
                .flatMap(property -> property.getColumns().stream())
                .collect(Collectors.toSet());

        verify(node.getOutputVariables().containsAll(localPropertyColumns), "Node-level local properties contain columns not present in node's output");
        return output;
    }

    public static ActualProperties streamBackdoorDeriveProperties(PlanNode node, List<ActualProperties> inputProperties, Metadata metadata, Session session)
    {
        return node.accept(new Visitor(metadata, session), inputProperties);
    }

    private static class Visitor
            extends InternalPlanVisitor<ActualProperties, List<ActualProperties>>
    {
        private final Metadata metadata;
        private final Session session;

        public Visitor(Metadata metadata, Session session)
        {
            this.metadata = metadata;
            this.session = session;
        }

        @Override
        public ActualProperties visitPlan(PlanNode node, List<ActualProperties> inputProperties)
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
                    .translateVariable(column -> PropertyDerivations.filterIfMissing(node.getOutputVariables(), column));
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

            ImmutableList.Builder<LocalProperty<VariableReferenceExpression>> newLocalProperties = ImmutableList.builder();
            newLocalProperties.addAll(properties.getLocalProperties());
            newLocalProperties.add(new GroupingProperty<>(ImmutableList.of(node.getIdVariable())));
            node.getSource().getOutputVariables().stream()
                    .forEach(column -> newLocalProperties.add(new ConstantProperty<>(column)));

            if (properties.getNodePartitioning().isPresent()) {
                // preserve input (possibly preferred) partitioning
                return ActualProperties.builderFrom(properties)
                        .local(newLocalProperties.build())
                        .build();
            }

            return ActualProperties.builderFrom(properties)
                    .global(partitionedOn(ARBITRARY_DISTRIBUTION, ImmutableList.of(node.getIdVariable()), Optional.empty()))
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
                    && (!orderingScheme.isPresent() || node.getPreSortedOrderPrefix() == orderingScheme.get().getOrderByVariables().size())) {
                return properties;
            }

            ImmutableList.Builder<LocalProperty<VariableReferenceExpression>> localProperties = ImmutableList.builder();

            // If the WindowNode has pre-partitioned inputs, then it will not change the order of those inputs at output,
            // so we should just propagate those underlying local properties that guarantee the pre-partitioning.
            // TODO: come up with a more general form of this operation for other streaming operators
            if (!node.getPrePartitionedInputs().isEmpty()) {
                GroupingProperty<VariableReferenceExpression> prePartitionedProperty = new GroupingProperty<>(node.getPrePartitionedInputs());
                for (LocalProperty<VariableReferenceExpression> localProperty : properties.getLocalProperties()) {
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
                    scheme.getOrderByVariables().stream()
                            .map(column -> new SortingProperty<>(column, scheme.getOrdering(column)))
                            .forEach(localProperties::add));

            return ActualProperties.builderFrom(properties)
                    .local(LocalProperties.normalizeAndPrune(localProperties.build()))
                    .build();
        }

        @Override
        public ActualProperties visitGroupId(GroupIdNode node, List<ActualProperties> inputProperties)
        {
            Map<VariableReferenceExpression, VariableReferenceExpression> inputToOutputMappings = new HashMap<>();
            for (Map.Entry<VariableReferenceExpression, VariableReferenceExpression> setMapping : node.getGroupingColumns().entrySet()) {
                if (node.getCommonGroupingColumns().contains(setMapping.getKey())) {
                    // TODO: Add support for translating a property on a single column to multiple columns
                    // when GroupIdNode is copying a single input grouping column into multiple output grouping columns (i.e. aliases), this is basically picking one arbitrarily
                    inputToOutputMappings.putIfAbsent(setMapping.getValue(), setMapping.getKey());
                }
            }

            // TODO: Add support for translating a property on a single column to multiple columns
            // this is deliberately placed after the grouping columns, because preserving properties has a bigger perf impact
            for (VariableReferenceExpression argument : node.getAggregationArguments()) {
                inputToOutputMappings.putIfAbsent(argument, argument);
            }

            return Iterables.getOnlyElement(inputProperties).translateVariable(column -> Optional.ofNullable(inputToOutputMappings.get(column)));
        }

        @Override
        public ActualProperties visitAggregation(AggregationNode node, List<ActualProperties> inputProperties)
        {
            ActualProperties properties = Iterables.getOnlyElement(inputProperties);

            ActualProperties translated = properties.translateVariable(variable -> node.getGroupingKeys().contains(variable) ? Optional.of(variable) : Optional.empty());

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

            ImmutableList.Builder<LocalProperty<VariableReferenceExpression>> localProperties = ImmutableList.builder();
            localProperties.add(new GroupingProperty<>(node.getPartitionBy()));
            for (VariableReferenceExpression column : node.getOrderingScheme().getOrderByVariables()) {
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

            List<SortingProperty<VariableReferenceExpression>> localProperties = node.getOrderingScheme().getOrderByVariables().stream()
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

            List<SortingProperty<VariableReferenceExpression>> localProperties = node.getOrderingScheme().getOrderByVariables().stream()
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
                    .local(LocalProperties.grouped(node.getDistinctVariables()))
                    .build();
        }

        @Override
        public ActualProperties visitStatisticsWriterNode(StatisticsWriterNode node, List<ActualProperties> context)
        {
            return ActualProperties.builder()
                    .global(coordinatorSingleStreamPartition())
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
            return Iterables.getOnlyElement(inputProperties).translateVariable(symbol -> Optional.empty());
        }

        @Override
        public ActualProperties visitUpdate(UpdateNode node, List<ActualProperties> inputProperties)
        {
            return Iterables.getOnlyElement(inputProperties).translateVariable(symbol -> Optional.empty());
        }

        @Override
        public ActualProperties visitMergeWriter(MergeWriterNode node, List<ActualProperties> inputProperties)
        {
            return visitPartitionedWriter(inputProperties);
        }

        @Override
        public ActualProperties visitMergeProcessor(MergeProcessorNode node, List<ActualProperties> inputProperties)
        {
            return Iterables.getOnlyElement(inputProperties).translateVariable(symbol -> Optional.empty());
        }

        @Override
        public ActualProperties visitJoin(JoinNode node, List<ActualProperties> inputProperties)
        {
            ActualProperties probeProperties = inputProperties.get(0);
            ActualProperties buildProperties = inputProperties.get(1);
            List<VariableReferenceExpression> outputVariableReferences = node.getOutputVariables();

            boolean unordered = spillPossible(session, node.getType());

            switch (node.getType()) {
                case INNER:
                    probeProperties = probeProperties.translateVariable(column -> filterOrRewrite(outputVariableReferences, node.getCriteria(), column));
                    buildProperties = buildProperties.translateVariable(column -> filterOrRewrite(outputVariableReferences, node.getCriteria(), column));

                    Map<VariableReferenceExpression, ConstantExpression> constants = new HashMap<>();
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
                    return ActualProperties.builderFrom(probeProperties.translateVariable(column -> filterIfMissing(outputVariableReferences, column)))
                            .unordered(unordered)
                            .build();
                case RIGHT:
                    buildProperties = buildProperties.translateVariable(column -> filterIfMissing(node.getOutputVariables(), column));

                    return ActualProperties.builderFrom(buildProperties.translateVariable(column -> filterIfMissing(outputVariableReferences, column)))
                            .local(ImmutableList.of())
                            .unordered(true)
                            .build();
                case FULL:
                    if (probeProperties.isSingleNode()) {
                        return ActualProperties.builder()
                                .global(singleStreamPartition())
                                .build();
                    }

                    if (probeProperties.getNodePartitioning().isPresent() &&
                            buildProperties.getNodePartitioning().isPresent() &&
                            arePartitionHandlesCompatibleForCoalesce(
                                    probeProperties.getNodePartitioning().get().getHandle(),
                                    buildProperties.getNodePartitioning().get().getHandle(),
                                    metadata,
                                    session)) {
                        return ActualProperties.builder()
                                .global(partitionedOnCoalesce(probeProperties.getNodePartitioning().get(), buildProperties.getNodePartitioning().get(), metadata, session))
                                .build();
                    }

                    return ActualProperties.builder()
                            .global(arbitraryPartition())
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
        public ActualProperties visitSpatialJoin(SpatialJoinNode node, List<ActualProperties> inputProperties)
        {
            ActualProperties probeProperties = inputProperties.get(0);
            ActualProperties buildProperties = inputProperties.get(1);
            List<VariableReferenceExpression> outputs = node.getOutputVariables();

            switch (node.getType()) {
                case INNER:
                    probeProperties = probeProperties.translateVariable(column -> filterIfMissing(outputs, column));
                    buildProperties = buildProperties.translateVariable(column -> filterIfMissing(outputs, column));

                    Map<VariableReferenceExpression, ConstantExpression> constants = new HashMap<>();
                    constants.putAll(probeProperties.getConstants());
                    constants.putAll(buildProperties.getConstants());

                    return ActualProperties.builderFrom(probeProperties)
                            .constants(constants)
                            .build();
                case LEFT:
                    return ActualProperties.builderFrom(probeProperties.translateVariable(column -> filterIfMissing(outputs, column)))
                            .build();
                default:
                    throw new IllegalArgumentException("Unsupported spatial join type: " + node.getType());
            }
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
                            .constants(ImmutableMap.<VariableReferenceExpression, ConstantExpression>builder()
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

        @Override
        public ActualProperties visitMergeJoin(MergeJoinNode node, List<ActualProperties> inputProperties)
        {
            ActualProperties leftProperties = inputProperties.get(0);
            ActualProperties rightProperties = inputProperties.get(1);
            List<VariableReferenceExpression> outputVariableReferences = node.getOutputVariables();

            switch (node.getType()) {
                case INNER:
                    leftProperties = leftProperties.translateVariable(column -> filterOrRewrite(outputVariableReferences, node.getCriteria(), column));
                    rightProperties = rightProperties.translateVariable(column -> filterOrRewrite(outputVariableReferences, node.getCriteria(), column));

                    Map<VariableReferenceExpression, ConstantExpression> constants = new HashMap<>();
                    constants.putAll(leftProperties.getConstants());
                    constants.putAll(rightProperties.getConstants());

                    return ActualProperties.builderFrom(leftProperties)
                            .constants(constants)
                            .build();
                case LEFT:
                    return ActualProperties.builderFrom(leftProperties.translateVariable(column -> filterIfMissing(outputVariableReferences, column)))
                            .build();
                case RIGHT:
                    rightProperties = rightProperties.translateVariable(column -> filterIfMissing(node.getOutputVariables(), column));

                    return ActualProperties.builderFrom(rightProperties.translateVariable(column -> filterIfMissing(outputVariableReferences, column)))
                            .local(ImmutableList.of())
                            .unordered(true)
                            .build();
                case FULL:
                    if (leftProperties.isSingleNode()) {
                        return ActualProperties.builder()
                                .global(singleStreamPartition())
                                .build();
                    }

                    if (leftProperties.getNodePartitioning().isPresent() &&
                            rightProperties.getNodePartitioning().isPresent() &&
                            arePartitionHandlesCompatibleForCoalesce(
                                    leftProperties.getNodePartitioning().get().getHandle(),
                                    rightProperties.getNodePartitioning().get().getHandle(),
                                    metadata,
                                    session)) {
                        return ActualProperties.builder()
                                .global(partitionedOnCoalesce(leftProperties.getNodePartitioning().get(), rightProperties.getNodePartitioning().get(), metadata, session))
                                .build();
                    }

                    return ActualProperties.builder()
                            .global(arbitraryPartition())
                            .build();
                default:
                    throw new UnsupportedOperationException("Unsupported join type: " + node.getType());
            }
        }

        @Override
        public ActualProperties visitExchange(ExchangeNode node, List<ActualProperties> inputProperties)
        {
            checkArgument(!node.getScope().isRemote() || inputProperties.stream().noneMatch(ActualProperties::isNullsAndAnyReplicated), "Null-and-any replicated inputs should not be remotely exchanged");

            Set<Map.Entry<VariableReferenceExpression, ConstantExpression>> entries = null;
            for (int sourceIndex = 0; sourceIndex < node.getSources().size(); sourceIndex++) {
                List<VariableReferenceExpression> inputVariables = node.getInputs().get(sourceIndex);
                Map<VariableReferenceExpression, VariableReferenceExpression> inputToOutput = new HashMap<>();
                for (int i = 0; i < node.getOutputVariables().size(); i++) {
                    inputToOutput.put(inputVariables.get(i), node.getOutputVariables().get(i));
                }

                ActualProperties translated = inputProperties.get(sourceIndex).translateVariable(variable -> Optional.ofNullable(inputToOutput.get(variable)));

                entries = (entries == null) ? translated.getConstants().entrySet() : Sets.intersection(entries, translated.getConstants().entrySet());
            }
            checkState(entries != null);

            Map<VariableReferenceExpression, ConstantExpression> constants = entries.stream()
                    .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));

            ImmutableList.Builder<SortingProperty<VariableReferenceExpression>> localProperties = ImmutableList.builder();
            if (node.getOrderingScheme().isPresent()) {
                node.getOrderingScheme().get().getOrderByVariables().stream()
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
            if (node.getScope().isLocal()) {
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
                case REPARTITION: {
                    Global globalPartitioning;
                    if (node.getPartitioningScheme().isScaleWriters()) {
                        // no strict partitioning guarantees when multiple writers per partitions allowed (scaled writers)
                        globalPartitioning = arbitraryPartition();
                    }
                    else {
                        globalPartitioning = partitionedOn(
                                node.getPartitioningScheme().getPartitioning(),
                                Optional.of(node.getPartitioningScheme().getPartitioning()))
                                .withReplicatedNulls(node.getPartitioningScheme().isReplicateNullsAndAny());
                    }
                    return ActualProperties.builder()
                            .global(globalPartitioning)
                            .constants(constants)
                            .build();
                }
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

            Map<VariableReferenceExpression, ConstantExpression> constants = new HashMap<>(properties.getConstants());
            TupleDomain<VariableReferenceExpression> tupleDomain = new RowExpressionDomainTranslator(metadata).fromPredicate(session.toConnectorSession(), node.getPredicate(), BASIC_COLUMN_EXTRACTOR).getTupleDomain();
            constants.putAll(extractFixedValuesToConstantExpressions(tupleDomain)
                    .orElse(ImmutableMap.of()));

            return ActualProperties.builderFrom(properties)
                    .constants(constants)
                    .build();
        }

        @Override
        public ActualProperties visitProject(ProjectNode node, List<ActualProperties> inputProperties)
        {
            ActualProperties properties = Iterables.getOnlyElement(inputProperties);

            ActualProperties translatedProperties = properties.translateRowExpression(node.getAssignments().getMap());

            // Extract additional constants
            Map<VariableReferenceExpression, ConstantExpression> constants = new HashMap<>();
            for (Map.Entry<VariableReferenceExpression, RowExpression> assignment : node.getAssignments().entrySet()) {
                RowExpression expression = assignment.getValue();
                VariableReferenceExpression output = assignment.getKey();

                // TODO:
                // We want to use a symbol resolver that looks up in the constants from the input subplan
                // to take advantage of constant-folding for complex expressions
                // However, that currently causes errors when those expressions operate on arrays or row types
                // ("ROW comparison not supported for fields with null elements", etc)
                Object value = new RowExpressionInterpreter(expression, metadata.getFunctionAndTypeManager(), session.toConnectorSession(), OPTIMIZED).optimize();

                if (value instanceof VariableReferenceExpression) {
                    ConstantExpression existingConstantValue = constants.get(value);
                    if (existingConstantValue != null) {
                        constants.put(output, new ConstantExpression(((VariableReferenceExpression) value).getSourceLocation(), value, expression.getType()));
                    }
                }
                else if (!(value instanceof RowExpression)) {
                    constants.put(output, new ConstantExpression(value, expression.getType()));
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
            return visitPartitionedWriter(inputProperties);
        }

        private ActualProperties visitPartitionedWriter(List<ActualProperties> inputProperties)
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
        public ActualProperties visitTableWriteMerge(TableWriterMergeNode node, List<ActualProperties> inputProperties)
        {
            return Iterables.getOnlyElement(inputProperties);
        }

        @Override
        public ActualProperties visitSample(SampleNode node, List<ActualProperties> inputProperties)
        {
            return Iterables.getOnlyElement(inputProperties);
        }

        @Override
        public ActualProperties visitUnnest(UnnestNode node, List<ActualProperties> inputProperties)
        {
            Set<VariableReferenceExpression> passThroughInputs = ImmutableSet.copyOf(node.getReplicateVariables());

            return Iterables.getOnlyElement(inputProperties).translateVariable(column -> {
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

        public ActualProperties visitSequence(SequenceNode node, List<ActualProperties> context)
        {
            // Return the rightmost node properties
            return context.get(context.size() - 1);
        }

        @Override
        public ActualProperties visitTableScan(TableScanNode node, List<ActualProperties> inputProperties)
        {
            TableLayout layout = metadata.getLayout(session, node.getTable());
            Map<ColumnHandle, VariableReferenceExpression> assignments = ImmutableBiMap.copyOf(node.getAssignments()).inverse();

            ActualProperties.Builder properties = ActualProperties.builder();

            // Globally constant assignments
            Map<ColumnHandle, ConstantExpression> globalConstants = new HashMap<>();
            extractFixedValuesToConstantExpressions(node.getCurrentConstraint()).orElse(ImmutableMap.of())
                    .entrySet().stream()
                    .filter(entry -> !entry.getValue().isNull())
                    .forEach(entry -> globalConstants.put(entry.getKey(), entry.getValue()));

            Map<VariableReferenceExpression, ConstantExpression> symbolConstants = globalConstants.entrySet().stream()
                    .filter(entry -> assignments.containsKey(entry.getKey()))
                    .collect(toMap(entry -> assignments.get(entry.getKey()), Map.Entry::getValue));
            properties.constants(symbolConstants);

            // Partitioning properties
            properties.global(deriveGlobalProperties(layout, assignments, globalConstants));

            // Append the global constants onto the local properties to maximize their translation potential
            List<LocalProperty<ColumnHandle>> constantAppendedLocalProperties = ImmutableList.<LocalProperty<ColumnHandle>>builder()
                    .addAll(globalConstants.keySet().stream().map(ConstantProperty::new).iterator())
                    .addAll(layout.getLocalProperties())
                    .build();
            properties.local(LocalProperties.translate(constantAppendedLocalProperties, column -> Optional.ofNullable(assignments.get(column))));

            return properties.build();
        }

        @Override
        public ActualProperties visitRemoteSource(RemoteSourceNode node, List<ActualProperties> inputProperties)
        {
            if (node.getOrderingScheme().isPresent()) {
                return ActualProperties.builder()
                        .global(singleStreamPartition())
                        .unordered(false)
                        .build();
            }
            if (node.isEnsureSourceOrdering()) {
                return ActualProperties.builder()
                        .global(singleStreamPartition())
                        .build();
            }

            return ActualProperties.builder().build();
        }

        private Global deriveGlobalProperties(TableLayout layout, Map<ColumnHandle, VariableReferenceExpression> assignments, Map<ColumnHandle, ConstantExpression> constants)
        {
            Optional<List<VariableReferenceExpression>> streamPartitioning = layout.getStreamPartitioningColumns()
                    .flatMap(columns -> translateToNonConstantSymbols(columns, assignments, constants));

            if (planWithTableNodePartitioning(session) && layout.getTablePartitioning().isPresent()) {
                TablePartitioning tablePartitioning = layout.getTablePartitioning().get();

                Set<ColumnHandle> assignmentsAndConstants = ImmutableSet.<ColumnHandle>builder()
                        .addAll(assignments.keySet())
                        .addAll(constants.keySet())
                        .build();
                if (assignmentsAndConstants.containsAll(tablePartitioning.getPartitioningColumns())) {
                    List<RowExpression> arguments = tablePartitioning.getPartitioningColumns().stream()
                            .map(column -> assignments.containsKey(column) ? assignments.get(column) : constants.get(column))
                            .collect(toImmutableList());

                    return partitionedOn(tablePartitioning.getPartitioningHandle(), arguments, streamPartitioning);
                }
            }

            if (streamPartitioning.isPresent()) {
                return streamPartitionedOn(streamPartitioning.get());
            }
            return arbitraryPartition();
        }

        private static Optional<List<VariableReferenceExpression>> translateToNonConstantSymbols(
                Set<ColumnHandle> columnHandles,
                Map<ColumnHandle, VariableReferenceExpression> assignments,
                Map<ColumnHandle, ConstantExpression> globalConstants)
        {
            // Strip off the constants from the partitioning columns (since those are not required for translation)
            Set<ColumnHandle> constantsStrippedColumns = columnHandles.stream()
                    .filter(column -> !globalConstants.containsKey(column))
                    .collect(toImmutableSet());

            ImmutableSet.Builder<VariableReferenceExpression> builder = ImmutableSet.builder();
            for (ColumnHandle column : constantsStrippedColumns) {
                VariableReferenceExpression translated = assignments.get(column);
                if (translated == null) {
                    return Optional.empty();
                }
                builder.add(translated);
            }

            return Optional.of(ImmutableList.copyOf(builder.build()));
        }
    }

    static boolean spillPossible(Session session, JoinType joinType)
    {
        if (!isSpillEnabled(session) || !isJoinSpillingEnabled(session)) {
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

    public static Optional<VariableReferenceExpression> filterIfMissing(Collection<VariableReferenceExpression> columns, VariableReferenceExpression column)
    {
        if (columns.contains(column)) {
            return Optional.of(column);
        }

        return Optional.empty();
    }

    // Used to filter columns that are not exposed by join node
    // Or, if they are part of the equalities, to translate them
    // to the other symbol if that's exposed, instead.
    public static Optional<VariableReferenceExpression> filterOrRewrite(Collection<VariableReferenceExpression> columns, List<EquiJoinClause> equalities, VariableReferenceExpression column)
    {
        // symbol is exposed directly, so no translation needed
        if (columns.contains(column)) {
            return Optional.of(column);
        }

        // if the column is part of the equality conditions and its counterpart
        // is exposed, use that, instead
        for (EquiJoinClause equality : equalities) {
            if (equality.getLeft().equals(column) && columns.contains(equality.getRight())) {
                return Optional.of(equality.getRight());
            }
            else if (equality.getRight().equals(column) && columns.contains(equality.getLeft())) {
                return Optional.of(equality.getLeft());
            }
        }

        return Optional.empty();
    }

    public static boolean arePartitionHandlesCompatibleForCoalesce(PartitioningHandle a, PartitioningHandle b, Metadata metadata, Session session)
    {
        return a.equals(b) || metadata.isRefinedPartitioningOver(session, a, b) || metadata.isRefinedPartitioningOver(session, b, a);
    }

    /**
     * Extract all column constraints that require exactly one value or only null in their respective Domains.
     * Returns an empty Optional if the Domain is none.
     */
    public static <T> Optional<Map<T, ConstantExpression>> extractFixedValuesToConstantExpressions(TupleDomain<T> tupleDomain)
    {
        if (!tupleDomain.getDomains().isPresent()) {
            return Optional.empty();
        }

        return Optional.of(tupleDomain.getDomains().get()
                .entrySet().stream()
                .filter(entry -> entry.getValue().isNullableSingleValue())
                .collect(toLinkedMap(Map.Entry::getKey, entry -> new ConstantExpression(entry.getValue().getNullableSingleValue(), entry.getValue().getType()))));
    }
}
