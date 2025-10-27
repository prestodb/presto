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
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.LocalProperty;
import com.facebook.presto.spi.UniqueProperty;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.DeleteNode;
import com.facebook.presto.spi.plan.DistinctLimitNode;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.IndexJoinNode;
import com.facebook.presto.spi.plan.IndexSourceNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.MarkDistinctNode;
import com.facebook.presto.spi.plan.MergeJoinNode;
import com.facebook.presto.spi.plan.MetadataDeleteNode;
import com.facebook.presto.spi.plan.OutputNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.SemiJoinNode;
import com.facebook.presto.spi.plan.SortNode;
import com.facebook.presto.spi.plan.SpatialJoinNode;
import com.facebook.presto.spi.plan.TableFinishNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.TableWriterNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.plan.UnionNode;
import com.facebook.presto.spi.plan.UnnestNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.plan.WindowNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.optimizations.StreamPropertyDerivations.StreamProperties.StreamDistribution;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.AssignUniqueId;
import com.facebook.presto.sql.planner.plan.CallDistributedProcedureNode;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.ExplainAnalyzeNode;
import com.facebook.presto.sql.planner.plan.GroupIdNode;
import com.facebook.presto.sql.planner.plan.InternalPlanVisitor;
import com.facebook.presto.sql.planner.plan.LateralJoinNode;
import com.facebook.presto.sql.planner.plan.MergeProcessorNode;
import com.facebook.presto.sql.planner.plan.MergeWriterNode;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.facebook.presto.sql.planner.plan.RowNumberNode;
import com.facebook.presto.sql.planner.plan.SampleNode;
import com.facebook.presto.sql.planner.plan.SequenceNode;
import com.facebook.presto.sql.planner.plan.StatisticsWriterNode;
import com.facebook.presto.sql.planner.plan.TableFunctionNode;
import com.facebook.presto.sql.planner.plan.TableFunctionProcessorNode;
import com.facebook.presto.sql.planner.plan.TableWriterMergeNode;
import com.facebook.presto.sql.planner.plan.TopNRowNumberNode;
import com.facebook.presto.sql.planner.plan.UpdateNode;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.facebook.presto.SystemSessionProperties.isUtilizeUniquePropertyInQueryPlanningEnabled;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static com.facebook.presto.sql.planner.optimizations.PropertyDerivations.extractFixedValuesToConstantExpressions;
import static com.facebook.presto.sql.planner.optimizations.StreamPropertyDerivations.StreamProperties.StreamDistribution.FIXED;
import static com.facebook.presto.sql.planner.optimizations.StreamPropertyDerivations.StreamProperties.StreamDistribution.MULTIPLE;
import static com.facebook.presto.sql.planner.optimizations.StreamPropertyDerivations.StreamProperties.StreamDistribution.SINGLE;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE_MATERIALIZED;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class StreamPropertyDerivations
{
    private StreamPropertyDerivations() {}

    public static StreamProperties derivePropertiesRecursively(PlanNode node, Metadata metadata, Session session, boolean nativeExecution)
    {
        List<StreamProperties> inputProperties = node.getSources().stream()
                .map(source -> derivePropertiesRecursively(source, metadata, session, nativeExecution))
                .collect(toImmutableList());
        return StreamPropertyDerivations.deriveProperties(node, inputProperties, metadata, session, nativeExecution);
    }

    public static StreamProperties deriveProperties(PlanNode node, StreamProperties inputProperties, Metadata metadata, Session session, boolean nativeExecution)
    {
        return deriveProperties(node, ImmutableList.of(inputProperties), metadata, session, nativeExecution);
    }

    public static StreamProperties deriveProperties(PlanNode node, List<StreamProperties> inputProperties, Metadata metadata, Session session, boolean nativeExecution)
    {
        requireNonNull(node, "node is null");
        requireNonNull(inputProperties, "inputProperties is null");
        requireNonNull(metadata, "metadata is null");
        requireNonNull(session, "session is null");

        // properties.otherActualProperties will never be null here because the only way
        // an external caller should obtain StreamProperties is from this method, and the
        // last line of this method assures otherActualProperties is set.
        ActualProperties otherProperties = PropertyDerivations.streamBackdoorDeriveProperties(
                node,
                inputProperties.stream()
                        .map(properties -> {
                            checkState(properties.otherActualProperties.isPresent(), "otherActualProperties should always exist");
                            return properties.otherActualProperties.get();
                        })
                        .collect(toImmutableList()),
                metadata,
                session);

        StreamProperties result = node.accept(new Visitor(metadata, session, nativeExecution), inputProperties)
                .withOtherActualProperties(otherProperties);

        result.getPartitioningColumns().ifPresent(columns ->
                verify(node.getOutputVariables().containsAll(columns), "Stream-level partitioning properties contain columns not present in node's output"));

        Set<VariableReferenceExpression> localPropertyColumns = result.getLocalProperties().stream()
                .flatMap(property -> property.getColumns().stream())
                .collect(Collectors.toSet());

        verify(node.getOutputVariables().containsAll(localPropertyColumns), "Stream-level local properties contain columns not present in node's output");

        return result;
    }

    private static class Visitor
            extends InternalPlanVisitor<StreamProperties, List<StreamProperties>>
    {
        private final Metadata metadata;
        private final Session session;
        private final boolean nativeExecution;

        private Visitor(Metadata metadata, Session session, boolean nativeExecution)
        {
            this.metadata = metadata;
            this.session = session;
            this.nativeExecution = nativeExecution;
        }

        @Override
        public StreamProperties visitPlan(PlanNode node, List<StreamProperties> inputProperties)
        {
            throw new UnsupportedOperationException("not yet implemented: " + node.getClass().getName());
        }

        //
        // Joins
        //

        @Override
        public StreamProperties visitJoin(JoinNode node, List<StreamProperties> inputProperties)
        {
            StreamProperties leftProperties = inputProperties.get(0);
            List<VariableReferenceExpression> outputs = node.getOutputVariables();
            boolean unordered = PropertyDerivations.spillPossible(session, node.getType());

            switch (node.getType()) {
                case INNER:
                    return leftProperties
                            .translate(column -> PropertyDerivations.filterOrRewrite(outputs, node.getCriteria(), column))
                            .unordered(unordered);
                case LEFT:
                    return leftProperties
                            .translate(column -> PropertyDerivations.filterIfMissing(outputs, column))
                            .unordered(unordered);
                case RIGHT:
                    // since this is a right join, none of the matched output rows will contain nulls
                    // in the left partitioning columns, and all of the unmatched rows will have
                    // null for all left columns.  therefore, the output is still partitioned on the
                    // left columns.  the only change is there will be at least two streams so the
                    // output is multiple
                    // There is one exception to this.  If the left is partitioned on empty set, we
                    // we can't say that the output is partitioned on empty set, but we can say that
                    // it is partitioned on the left join symbols
                    // todo do something smarter after https://github.com/prestodb/presto/pull/5877 is merged
                    return new StreamProperties(MULTIPLE, Optional.empty(), false);
                case FULL:
                    // the left can contain nulls in any stream so we can't say anything about the
                    // partitioning, and nulls from the right are produced from a extra new stream
                    // so we will always have multiple streams.
                    return new StreamProperties(MULTIPLE, Optional.empty(), false);
                default:
                    throw new UnsupportedOperationException("Unsupported join type: " + node.getType());
            }
        }

        @Override
        public StreamProperties visitSpatialJoin(SpatialJoinNode node, List<StreamProperties> inputProperties)
        {
            StreamProperties leftProperties = inputProperties.get(0);

            switch (node.getType()) {
                case INNER:
                case LEFT:
                    return leftProperties.translate(column -> PropertyDerivations.filterIfMissing(node.getOutputVariables(), column));
                default:
                    throw new IllegalArgumentException("Unsupported spatial join type: " + node.getType());
            }
        }

        @Override
        public StreamProperties visitIndexJoin(IndexJoinNode node, List<StreamProperties> inputProperties)
        {
            StreamProperties probeProperties = inputProperties.get(0);

            switch (node.getType()) {
                case INNER:
                case SOURCE_OUTER:
                    return probeProperties.uniqueToGroupProperties();
                default:
                    throw new UnsupportedOperationException("Unsupported join type: " + node.getType());
            }
        }

        @Override
        public StreamProperties visitMergeJoin(MergeJoinNode node, List<StreamProperties> inputProperties)
        {
            StreamProperties leftProperties = inputProperties.get(0);
            StreamProperties rightProperties = inputProperties.get(1);
            List<VariableReferenceExpression> outputs = node.getOutputVariables();

            switch (node.getType()) {
                case INNER:
                    return leftProperties
                            .translate(column -> PropertyDerivations.filterOrRewrite(outputs, node.getCriteria(), column));
                case LEFT:
                    return leftProperties
                            .translate(column -> PropertyDerivations.filterIfMissing(outputs, column));
                case RIGHT:
                    return rightProperties
                            .translate(column -> PropertyDerivations.filterIfMissing(outputs, column));
                case FULL:
                    // the left can contain nulls in any stream so we can't say anything about the
                    // partitioning, and nulls from the right are produced from a extra new stream
                    // so we will always have multiple streams.
                    return new StreamProperties(MULTIPLE, Optional.empty(), false);
                default:
                    throw new UnsupportedOperationException("Unsupported join type: " + node.getType());
            }
        }

        //
        // Source nodes
        //
        @Override
        public StreamProperties visitSequence(SequenceNode node, List<StreamProperties> inputProperties)
        {
            return new StreamProperties(MULTIPLE, Optional.empty(), false);
        }

        @Override
        public StreamProperties visitValues(ValuesNode node, List<StreamProperties> context)
        {
            // values always produces a single stream
            return StreamProperties.singleStream();
        }

        @Override
        public StreamProperties visitTableScan(TableScanNode node, List<StreamProperties> inputProperties)
        {
            TableLayout layout = metadata.getLayout(session, node.getTable());
            Map<ColumnHandle, VariableReferenceExpression> assignments = ImmutableBiMap.copyOf(node.getAssignments()).inverse();

            // Globally constant assignments
            Set<ColumnHandle> constants = new HashSet<>();
            extractFixedValuesToConstantExpressions(node.getCurrentConstraint()).orElse(ImmutableMap.of())
                    .entrySet().stream()
                    .filter(entry -> !entry.getValue().isNull())  // TODO consider allowing nulls
                    .forEach(entry -> constants.add(entry.getKey()));

            Optional<Set<VariableReferenceExpression>> streamPartitionSymbols = layout.getStreamPartitioningColumns()
                    .flatMap(columns -> getNonConstantVariables(columns, assignments, constants));

            // Native execution creates a fixed number of drivers for TableScan pipelines
            StreamDistribution streamDistribution = nativeExecution ? FIXED : MULTIPLE;

            Optional<StreamProperties> streamPropertiesFromUniqueColumn = Optional.empty();
            if (isUtilizeUniquePropertyInQueryPlanningEnabled(session) && layout.getUniqueColumn().isPresent() && assignments.containsKey(layout.getUniqueColumn().get())) {
                streamPropertiesFromUniqueColumn = Optional.of(new StreamProperties(streamDistribution, Optional.of(ImmutableList.of(assignments.get(layout.getUniqueColumn().get()))), false));
            }

            // if we are partitioned on empty set, we must say multiple of unknown partitioning, because
            // the connector does not guarantee a single split in this case (since it might not understand
            // that the value is a constant).
            if (streamPartitionSymbols.isPresent() && streamPartitionSymbols.get().isEmpty()) {
                return new StreamProperties(streamDistribution, Optional.empty(), false, Optional.empty(), streamPropertiesFromUniqueColumn);
            }
            return new StreamProperties(streamDistribution, streamPartitionSymbols, false, Optional.empty(), streamPropertiesFromUniqueColumn);
        }

        private Optional<Set<VariableReferenceExpression>> getNonConstantVariables(Set<ColumnHandle> columnHandles, Map<ColumnHandle, VariableReferenceExpression> assignments, Set<ColumnHandle> globalConstants)
        {
            // Strip off the constants from the partitioning columns (since those are not required for translation)
            Set<ColumnHandle> constantsStrippedPartitionColumns = columnHandles.stream()
                    .filter(column -> !globalConstants.contains(column))
                    .collect(toImmutableSet());
            ImmutableSet.Builder<VariableReferenceExpression> builder = ImmutableSet.builder();

            for (ColumnHandle column : constantsStrippedPartitionColumns) {
                VariableReferenceExpression translated = assignments.get(column);
                if (translated == null) {
                    return Optional.empty();
                }
                builder.add(translated);
            }

            return Optional.of(builder.build());
        }

        @Override
        public StreamProperties visitExchange(ExchangeNode node, List<StreamProperties> inputProperties)
        {
            if (node.isEnsureSourceOrdering() || node.getOrderingScheme().isPresent()) {
                return StreamProperties.ordered();
            }

            if (node.getScope() == REMOTE_MATERIALIZED) {
                // remote materialized exchanges get converted to table scans. Return the properties that the table scan would have
                return new StreamProperties(MULTIPLE, Optional.empty(), false);
            }

            Optional<StreamProperties> additionalUniqueProperty = Optional.empty();
            if (inputProperties.size() == 1 && inputProperties.get(0).hasUniqueProperties() && !node.getType().equals(ExchangeNode.Type.REPLICATE)) {
                List<VariableReferenceExpression> inputVariables = node.getInputs().get(0);
                Map<VariableReferenceExpression, VariableReferenceExpression> inputToOutput = new HashMap<>();
                for (int i = 0; i < node.getOutputVariables().size(); i++) {
                    inputToOutput.put(inputVariables.get(i), node.getOutputVariables().get(i));
                }
                checkState(inputProperties.get(0).getStreamPropertiesFromUniqueColumn().isPresent(),
                        "when unique columns exists, the stream is also partitioned by the unique column and should be represented in the streamPropertiesFromUniqueColumn field");
                additionalUniqueProperty = Optional.of(inputProperties.get(0).getStreamPropertiesFromUniqueColumn().get().translate(column -> Optional.ofNullable(inputToOutput.get(column))));
            }

            if (node.getScope().isRemote()) {
                // TODO: correctly determine if stream is parallelised
                // based on session properties
                return StreamProperties.fixedStreams().withStreamPropertiesFromUniqueColumn(additionalUniqueProperty);
            }

            switch (node.getType()) {
                case GATHER:
                    return StreamProperties.singleStream().withStreamPropertiesFromUniqueColumn(additionalUniqueProperty);
                case REPARTITION:
                    if (node.getPartitioningScheme().getPartitioning().getHandle().equals(FIXED_ARBITRARY_DISTRIBUTION) ||
                            // no strict partitioning guarantees when multiple writers per partitions are allows (scaled writers)
                            node.getPartitioningScheme().isScaleWriters()) {
                        return new StreamProperties(FIXED, Optional.empty(), false).withStreamPropertiesFromUniqueColumn(additionalUniqueProperty);
                    }
                    checkArgument(
                            node.getPartitioningScheme().getPartitioning().getArguments().stream().allMatch(VariableReferenceExpression.class::isInstance),
                            format("Expect all partitioning arguments to be VariableReferenceExpression, but get %s", node.getPartitioningScheme().getPartitioning().getArguments()));
                    return new StreamProperties(
                            FIXED,
                            Optional.of(node.getPartitioningScheme().getPartitioning().getArguments().stream()
                                    .map(VariableReferenceExpression.class::cast)
                                    .collect(toImmutableList())), false).withStreamPropertiesFromUniqueColumn(additionalUniqueProperty);
                case REPLICATE:
                    return new StreamProperties(MULTIPLE, Optional.empty(), false);
            }

            throw new UnsupportedOperationException("not yet implemented");
        }

        //
        // Nodes that rewrite and/or drop symbols
        //

        @Override
        public StreamProperties visitProject(ProjectNode node, List<StreamProperties> inputProperties)
        {
            StreamProperties properties = Iterables.getOnlyElement(inputProperties);

            // We can describe properties in terms of inputs that are projected unmodified (i.e., identity projections)
            Map<VariableReferenceExpression, VariableReferenceExpression> identities = computeIdentityTranslations(node.getAssignments().getMap());

            return properties.translate(column -> Optional.ofNullable(identities.get(column)));
        }

        private static Map<VariableReferenceExpression, VariableReferenceExpression> computeIdentityTranslations(Map<VariableReferenceExpression, RowExpression> assignments)
        {
            Map<VariableReferenceExpression, VariableReferenceExpression> inputToOutput = new HashMap<>();
            for (Map.Entry<VariableReferenceExpression, RowExpression> assignment : assignments.entrySet()) {
                RowExpression expression = assignment.getValue();
                if (expression instanceof VariableReferenceExpression) {
                    inputToOutput.put((VariableReferenceExpression) expression, assignment.getKey());
                }
            }
            return inputToOutput;
        }

        @Override
        public StreamProperties visitGroupId(GroupIdNode node, List<StreamProperties> inputProperties)
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

            return Iterables.getOnlyElement(inputProperties).translate(column -> Optional.ofNullable(inputToOutputMappings.get(column)));
        }

        @Override
        public StreamProperties visitAggregation(AggregationNode node, List<StreamProperties> inputProperties)
        {
            StreamProperties properties = Iterables.getOnlyElement(inputProperties);

            // Only grouped symbols projected symbols are passed through
            return properties.translate(variable -> node.getGroupingKeys().contains(variable) ? Optional.of(variable) : Optional.empty());
        }

        @Override
        public StreamProperties visitStatisticsWriterNode(StatisticsWriterNode node, List<StreamProperties> inputProperties)
        {
            StreamProperties properties = Iterables.getOnlyElement(inputProperties);
            // analyze finish only outputs row count
            return properties.withUnspecifiedPartitioning();
        }

        @Override
        public StreamProperties visitTableFinish(TableFinishNode node, List<StreamProperties> inputProperties)
        {
            StreamProperties properties = Iterables.getOnlyElement(inputProperties);
            // table finish only outputs the row count
            return properties.withUnspecifiedPartitioning();
        }

        @Override
        public StreamProperties visitDelete(DeleteNode node, List<StreamProperties> inputProperties)
        {
            StreamProperties properties = Iterables.getOnlyElement(inputProperties);
            // delete only outputs the row count
            return properties.withUnspecifiedPartitioning();
        }

        @Override
        public StreamProperties visitCallDistributedProcedure(CallDistributedProcedureNode node, List<StreamProperties> inputProperties)
        {
            StreamProperties properties = Iterables.getOnlyElement(inputProperties);
            // call distributed procedure only outputs the row count
            return properties.withUnspecifiedPartitioning();
        }

        @Override
        public StreamProperties visitTableWriter(TableWriterNode node, List<StreamProperties> inputProperties)
        {
            StreamProperties properties = Iterables.getOnlyElement(inputProperties);
            // table writer only outputs the row count
            return properties.withUnspecifiedPartitioning();
        }

        @Override
        public StreamProperties visitUpdate(UpdateNode node, List<StreamProperties> inputProperties)
        {
            StreamProperties properties = Iterables.getOnlyElement(inputProperties);
            return properties.withUnspecifiedPartitioning();
        }

        @Override
        public StreamProperties visitMergeWriter(MergeWriterNode node, List<StreamProperties> inputProperties)
        {
            StreamProperties properties = Iterables.getOnlyElement(inputProperties);
            return properties.withUnspecifiedPartitioning();
        }

        @Override
        public StreamProperties visitMergeProcessor(MergeProcessorNode node, List<StreamProperties> inputProperties)
        {
            StreamProperties properties = Iterables.getOnlyElement(inputProperties);
            return properties.withUnspecifiedPartitioning();
        }

        @Override
        public StreamProperties visitMetadataDelete(MetadataDeleteNode node, List<StreamProperties> inputProperties)
        {
            // MetadataDeleteNode runs on coordinator and outputs a single row count
            return StreamProperties.singleStream();
        }

        @Override
        public StreamProperties visitTableWriteMerge(TableWriterMergeNode node, List<StreamProperties> inputProperties)
        {
            return Iterables.getOnlyElement(inputProperties);
        }

        @Override
        public StreamProperties visitUnnest(UnnestNode node, List<StreamProperties> inputProperties)
        {
            StreamProperties properties = Iterables.getOnlyElement(inputProperties);

            // We can describe properties in terms of inputs that are projected unmodified (i.e., not the unnested symbols)
            Set<VariableReferenceExpression> passThroughInputs = ImmutableSet.copyOf(node.getReplicateVariables());
            return properties.translate(column -> {
                if (passThroughInputs.contains(column)) {
                    return Optional.of(column);
                }
                return Optional.empty();
            });
        }

        @Override
        public StreamProperties visitExplainAnalyze(ExplainAnalyzeNode node, List<StreamProperties> inputProperties)
        {
            StreamProperties properties = Iterables.getOnlyElement(inputProperties);
            // explain only outputs the plan string
            return properties.withUnspecifiedPartitioning();
        }

        //
        // Nodes that gather data into a single stream
        //

        @Override
        public StreamProperties visitIndexSource(IndexSourceNode node, List<StreamProperties> context)
        {
            return StreamProperties.singleStream();
        }

        @Override
        public StreamProperties visitUnion(UnionNode node, List<StreamProperties> context)
        {
            // union is implemented using a local gather exchange
            return StreamProperties.singleStream();
        }

        @Override
        public StreamProperties visitEnforceSingleRow(EnforceSingleRowNode node, List<StreamProperties> context)
        {
            return StreamProperties.singleStream();
        }

        @Override
        public StreamProperties visitAssignUniqueId(AssignUniqueId node, List<StreamProperties> inputProperties)
        {
            StreamProperties properties = Iterables.getOnlyElement(inputProperties);
            if (properties.getPartitioningColumns().isPresent()) {
                // preserve input (possibly preferred) partitioning
                return properties;
            }

            return new StreamProperties(properties.getDistribution(),
                    Optional.of(ImmutableList.of(node.getIdVariable())),
                    properties.isOrdered());
        }

        //
        // Simple nodes that pass through stream properties
        //

        @Override
        public StreamProperties visitOutput(OutputNode node, List<StreamProperties> inputProperties)
        {
            return Iterables.getOnlyElement(inputProperties)
                    .translate(column -> PropertyDerivations.filterIfMissing(node.getOutputVariables(), column));
        }

        @Override
        public StreamProperties visitMarkDistinct(MarkDistinctNode node, List<StreamProperties> inputProperties)
        {
            return Iterables.getOnlyElement(inputProperties);
        }

        @Override
        public StreamProperties visitWindow(WindowNode node, List<StreamProperties> inputProperties)
        {
            StreamProperties childProperties = Iterables.getOnlyElement(inputProperties);
            if (childProperties.isSingleStream() && node.getPartitionBy().isEmpty() && node.getOrderingScheme().isPresent()) {
                return StreamProperties.ordered();
            }
            return Iterables.getOnlyElement(inputProperties);
        }

        @Override
        public StreamProperties visitTableFunction(TableFunctionNode node, List<StreamProperties> inputProperties)
        {
            throw new IllegalStateException(format("Unexpected node: TableFunctionNode (%s)", node.getName()));
        }

        @Override
        public StreamProperties visitTableFunctionProcessor(TableFunctionProcessorNode node, List<StreamProperties> inputProperties)
        {
            if (!node.getSource().isPresent()) {
                return StreamProperties.singleStream();
            }

            StreamProperties properties = Iterables.getOnlyElement(inputProperties);

            Set<VariableReferenceExpression> passThroughInputs = Sets.intersection(ImmutableSet.copyOf(node.getSource().orElseThrow(NoSuchElementException::new).getOutputVariables()), ImmutableSet.copyOf(node.getOutputVariables()));
            StreamProperties translatedProperties = properties.translate(column -> {
                if (passThroughInputs.contains(column)) {
                    return Optional.of(column);
                }
                return Optional.empty();
            });

            return translatedProperties.unordered(true);
        }

        @Override
        public StreamProperties visitRowNumber(RowNumberNode node, List<StreamProperties> inputProperties)
        {
            return Iterables.getOnlyElement(inputProperties);
        }

        @Override
        public StreamProperties visitTopNRowNumber(TopNRowNumberNode node, List<StreamProperties> inputProperties)
        {
            return Iterables.getOnlyElement(inputProperties);
        }

        @Override
        public StreamProperties visitTopN(TopNNode node, List<StreamProperties> inputProperties)
        {
            // Partial TopN doesn't guarantee that stream is ordered
            if (node.getStep().equals(TopNNode.Step.PARTIAL)) {
                return Iterables.getOnlyElement(inputProperties);
            }
            return StreamProperties.ordered();
        }

        @Override
        public StreamProperties visitSort(SortNode node, List<StreamProperties> inputProperties)
        {
            StreamProperties sourceProperties = Iterables.getOnlyElement(inputProperties);
            if (sourceProperties.isSingleStream()) {
                // stream is only sorted if sort operator is executed without parallelism
                return StreamProperties.ordered();
            }

            return sourceProperties;
        }

        @Override
        public StreamProperties visitLimit(LimitNode node, List<StreamProperties> inputProperties)
        {
            return Iterables.getOnlyElement(inputProperties);
        }

        @Override
        public StreamProperties visitDistinctLimit(DistinctLimitNode node, List<StreamProperties> inputProperties)
        {
            return Iterables.getOnlyElement(inputProperties);
        }

        @Override
        public StreamProperties visitSemiJoin(SemiJoinNode node, List<StreamProperties> inputProperties)
        {
            return inputProperties.get(0);
        }

        @Override
        public StreamProperties visitApply(ApplyNode node, List<StreamProperties> inputProperties)
        {
            throw new IllegalStateException("Unexpected node: " + node.getClass());
        }

        @Override
        public StreamProperties visitLateralJoin(LateralJoinNode node, List<StreamProperties> inputProperties)
        {
            throw new IllegalStateException("Unexpected node: " + node.getClass());
        }

        @Override
        public StreamProperties visitFilter(FilterNode node, List<StreamProperties> inputProperties)
        {
            return Iterables.getOnlyElement(inputProperties);
        }

        @Override
        public StreamProperties visitSample(SampleNode node, List<StreamProperties> inputProperties)
        {
            return Iterables.getOnlyElement(inputProperties);
        }

        @Override
        public StreamProperties visitRemoteSource(RemoteSourceNode node, List<StreamProperties> inputProperties)
        {
            if (node.getOrderingScheme().isPresent()) {
                return StreamProperties.ordered();
            }
            if (node.isEnsureSourceOrdering()) {
                return StreamProperties.singleStream();
            }

            return StreamProperties.fixedStreams();
        }
    }

    public static final class StreamProperties
    {
        public enum StreamDistribution
        {
            SINGLE, MULTIPLE, FIXED
        }

        private final StreamDistribution distribution;

        private final Optional<List<VariableReferenceExpression>> partitioningColumns; // if missing => partitioned with some unknown scheme

        private final boolean ordered;

        // We are only interested in the local properties, but PropertyDerivations requires input
        // ActualProperties, so we hold on to the whole object
        private final Optional<ActualProperties> otherActualProperties;

        // NOTE: Partitioning on zero columns (or effectively zero columns if the columns are constant) indicates that all
        // the rows will be partitioned into a single stream.

        private final Optional<StreamProperties> streamPropertiesFromUniqueColumn;

        private StreamProperties(StreamDistribution distribution, Optional<? extends Iterable<VariableReferenceExpression>> partitioningColumns, boolean ordered)
        {
            this(distribution, partitioningColumns, ordered, Optional.empty());
        }

        private StreamProperties(
                StreamDistribution distribution,
                Optional<? extends Iterable<VariableReferenceExpression>> partitioningColumns,
                boolean ordered,
                Optional<ActualProperties> otherActualProperties)
        {
            this(distribution, partitioningColumns, ordered, otherActualProperties, Optional.empty());
        }

        private StreamProperties(
                StreamDistribution distribution,
                Optional<? extends Iterable<VariableReferenceExpression>> partitioningColumns,
                boolean ordered,
                Optional<ActualProperties> otherActualProperties,
                Optional<StreamProperties> streamPropertiesFromUniqueColumn)
        {
            this.distribution = requireNonNull(distribution, "distribution is null");

            this.partitioningColumns = requireNonNull(partitioningColumns, "partitioningProperties is null")
                    .map(ImmutableList::copyOf);

            checkArgument(distribution != SINGLE || this.partitioningColumns.equals(Optional.of(ImmutableList.of())),
                    "Single stream must be partitioned on empty set");
            checkArgument(distribution == SINGLE || !this.partitioningColumns.equals(Optional.of(ImmutableList.of())),
                    "Multiple streams must not be partitioned on empty set");

            this.ordered = ordered;
            checkArgument(!ordered || distribution == SINGLE, "Ordered must be a single stream");

            this.otherActualProperties = requireNonNull(otherActualProperties);
            requireNonNull(streamPropertiesFromUniqueColumn).ifPresent(properties -> checkArgument(!properties.streamPropertiesFromUniqueColumn.isPresent()));
            // When unique properties exists, the stream is also partitioned on the unique column
            if (otherActualProperties.isPresent() && otherActualProperties.get().getPropertiesFromUniqueColumn().isPresent()) {
                ActualProperties propertiesFromUniqueColumn = otherActualProperties.get().getPropertiesFromUniqueColumn().get();
                if (Iterables.getOnlyElement(propertiesFromUniqueColumn.getLocalProperties()) instanceof UniqueProperty) {
                    VariableReferenceExpression uniqueVariable = (VariableReferenceExpression) ((UniqueProperty<?>) Iterables.getOnlyElement(propertiesFromUniqueColumn.getLocalProperties())).getColumn();
                    checkState(streamPropertiesFromUniqueColumn.isPresent() && streamPropertiesFromUniqueColumn.get().partitioningColumns.isPresent()
                            && Iterables.getOnlyElement(streamPropertiesFromUniqueColumn.get().partitioningColumns.get()).equals(uniqueVariable));
                }
            }
            this.streamPropertiesFromUniqueColumn = streamPropertiesFromUniqueColumn;
        }

        public List<LocalProperty<VariableReferenceExpression>> getLocalProperties()
        {
            checkState(otherActualProperties.isPresent(), "otherActualProperties not set");
            return otherActualProperties.get().getLocalProperties();
        }

        public List<LocalProperty<VariableReferenceExpression>> getAdditionalLocalProperties()
        {
            checkState(otherActualProperties.isPresent(), "otherActualProperties not set");
            if (!otherActualProperties.get().getPropertiesFromUniqueColumn().isPresent()) {
                return ImmutableList.of();
            }
            return otherActualProperties.get().getPropertiesFromUniqueColumn().get().getLocalProperties();
        }

        public Optional<StreamProperties> getStreamPropertiesFromUniqueColumn()
        {
            return streamPropertiesFromUniqueColumn;
        }

        private static StreamProperties singleStream()
        {
            return new StreamProperties(SINGLE, Optional.of(ImmutableSet.of()), false);
        }

        private static StreamProperties fixedStreams()
        {
            return new StreamProperties(FIXED, Optional.empty(), false);
        }

        private static StreamProperties ordered()
        {
            return new StreamProperties(SINGLE, Optional.of(ImmutableSet.of()), true);
        }

        private StreamProperties unordered(boolean unordered)
        {
            if (unordered) {
                ActualProperties updatedProperties = null;
                if (otherActualProperties.isPresent()) {
                    updatedProperties = ActualProperties.builderFrom(otherActualProperties.get())
                            .propertiesFromUniqueColumn(otherActualProperties.get().getPropertiesFromUniqueColumn().map(x -> ActualProperties.builderFrom(x).unordered(true).build()))
                            .unordered(true)
                            .build();
                }
                return new StreamProperties(
                        distribution,
                        partitioningColumns,
                        false,
                        Optional.ofNullable(updatedProperties),
                        streamPropertiesFromUniqueColumn.map(x -> x.unordered(true)));
            }
            return this;
        }

        public StreamProperties uniqueToGroupProperties()
        {
            if (otherActualProperties.isPresent() && otherActualProperties.get().getPropertiesFromUniqueColumn().isPresent()) {
                if (Iterables.getOnlyElement(otherActualProperties.get().getPropertiesFromUniqueColumn().get().getLocalProperties()) instanceof UniqueProperty) {
                    Optional<ActualProperties> groupedProperties = PropertyDerivations.uniqueToGroupProperties(otherActualProperties.get().getPropertiesFromUniqueColumn().get());
                    return new StreamProperties(distribution, partitioningColumns, ordered,
                            otherActualProperties.map(x -> ActualProperties.builderFrom(x).propertiesFromUniqueColumn(groupedProperties).build()),
                            streamPropertiesFromUniqueColumn.map(StreamProperties::uniqueToGroupProperties));
                }
            }
            return this;
        }

        public boolean hasUniqueProperties()
        {
            if (otherActualProperties.isPresent() && otherActualProperties.get().getPropertiesFromUniqueColumn().isPresent()) {
                return Iterables.getOnlyElement(otherActualProperties.get().getPropertiesFromUniqueColumn().get().getLocalProperties()) instanceof UniqueProperty;
            }
            return false;
        }

        public boolean isSingleStream()
        {
            return distribution == SINGLE;
        }

        public StreamDistribution getDistribution()
        {
            return distribution;
        }

        public boolean isExactlyPartitionedOn(Iterable<VariableReferenceExpression> columns)
        {
            return partitioningColumns.isPresent() && columns.equals(ImmutableList.copyOf(partitioningColumns.get()));
        }

        public boolean isPartitionedOn(Iterable<VariableReferenceExpression> columns)
        {
            if (!partitioningColumns.isPresent()) {
                return false;
            }

            // partitioned on (k_1, k_2, ..., k_n) => partitioned on (k_1, k_2, ..., k_n, k_n+1, ...)
            // can safely ignore all constant columns when comparing partition properties
            return ImmutableSet.copyOf(columns).containsAll(partitioningColumns.get());
        }

        public boolean isOrdered()
        {
            return ordered;
        }

        private StreamProperties withUnspecifiedPartitioning()
        {
            // a single stream has no symbols
            if (isSingleStream()) {
                return this;
            }
            // otherwise we are distributed on some symbols, but since we are trying to remove all symbols,
            // just say we have multiple partitions with an unknown scheme
            return new StreamProperties(distribution, Optional.empty(), ordered);
        }

        private StreamProperties withOtherActualProperties(ActualProperties actualProperties)
        {
            return new StreamProperties(distribution, partitioningColumns, ordered, Optional.ofNullable(actualProperties), streamPropertiesFromUniqueColumn);
        }

        private StreamProperties withStreamPropertiesFromUniqueColumn(Optional<StreamProperties> streamPropertiesFromUniqueColumn)
        {
            return new StreamProperties(distribution, partitioningColumns, ordered, otherActualProperties, streamPropertiesFromUniqueColumn);
        }

        public StreamProperties translate(Function<VariableReferenceExpression, Optional<VariableReferenceExpression>> translator)
        {
            return new StreamProperties(
                    distribution,
                    partitioningColumns.flatMap(partitioning -> {
                        ImmutableList.Builder<VariableReferenceExpression> newPartitioningColumns = ImmutableList.builder();
                        for (VariableReferenceExpression partitioningColumn : partitioning) {
                            Optional<VariableReferenceExpression> translated = translator.apply(partitioningColumn);
                            if (!translated.isPresent()) {
                                return Optional.empty();
                            }
                            newPartitioningColumns.add(translated.get());
                        }
                        return Optional.of(newPartitioningColumns.build());
                    }),
                    ordered, otherActualProperties.map(x -> x.translateVariable(translator)),
                    streamPropertiesFromUniqueColumn.map(x -> x.translate(translator)));
        }

        public Optional<List<VariableReferenceExpression>> getPartitioningColumns()
        {
            return partitioningColumns;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(distribution, partitioningColumns, ordered, otherActualProperties, streamPropertiesFromUniqueColumn);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            StreamProperties other = (StreamProperties) obj;
            return Objects.equals(this.distribution, other.distribution) &&
                    Objects.equals(this.partitioningColumns, other.partitioningColumns) &&
                    this.ordered == other.ordered &&
                    Objects.equals(this.otherActualProperties, other.otherActualProperties) &&
                    Objects.equals(this.streamPropertiesFromUniqueColumn, other.streamPropertiesFromUniqueColumn);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("distribution", distribution)
                    .add("partitioningColumns", partitioningColumns)
                    .add("ordered", ordered)
                    .add("otherActualProperties", otherActualProperties)
                    .add("streamPropertiesFromUniqueColumn", streamPropertiesFromUniqueColumn)
                    .toString();
        }
    }
}
