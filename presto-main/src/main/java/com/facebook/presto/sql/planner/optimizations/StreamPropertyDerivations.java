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
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.Partitioning.ArgumentBinding;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.AggregationNode;
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
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.facebook.presto.sql.planner.plan.UnnestNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import javax.annotation.concurrent.Immutable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.facebook.presto.spi.predicate.TupleDomain.extractFixedValues;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_RANDOM_DISTRIBUTION;
import static com.facebook.presto.sql.planner.optimizations.StreamPropertyDerivations.StreamProperties.StreamDistribution.FIXED;
import static com.facebook.presto.sql.planner.optimizations.StreamPropertyDerivations.StreamProperties.StreamDistribution.MULTIPLE;
import static com.facebook.presto.sql.planner.optimizations.StreamPropertyDerivations.StreamProperties.StreamDistribution.SINGLE;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableSet;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

final class StreamPropertyDerivations
{
    private StreamPropertyDerivations() {}

    public static StreamProperties deriveProperties(PlanNode node, StreamProperties inputProperties, Metadata metadata, Session session, Map<Symbol, Type> types, SqlParser parser)
    {
        return deriveProperties(node, ImmutableList.of(inputProperties), metadata, session, types, parser);
    }

    public static StreamProperties deriveProperties(PlanNode node, List<StreamProperties> inputProperties, Metadata metadata, Session session, Map<Symbol, Type> types, SqlParser parser)
    {
        requireNonNull(node, "node is null");
        requireNonNull(inputProperties, "inputProperties is null");
        requireNonNull(metadata, "metadata is null");
        requireNonNull(session, "session is null");
        requireNonNull(types, "types is null");
        requireNonNull(parser, "parser is null");

        // properties.otherActualProperties will never be null here because the only way
        // an external caller should obtain StreamProperties is from this method, and the
        // last line of this method assures otherActualProperties is set.
        ActualProperties otherProperties = PropertyDerivations.streamBackdoorDeriveProperties(
                node,
                inputProperties.stream()
                        .map(properties -> properties.otherActualProperties)
                        .collect(toImmutableList()),
                metadata,
                session,
                types,
                parser);

        return node.accept(new Visitor(metadata, session), inputProperties)
                .withOtherActualProperties(otherProperties);
    }

    private static class Visitor
            extends PlanVisitor<List<StreamProperties>, StreamProperties>
    {
        private final Metadata metadata;
        private final Session session;

        private Visitor(Metadata metadata, Session session)
        {
            this.metadata = metadata;
            this.session = session;
        }

        @Override
        protected StreamProperties visitPlan(PlanNode node, List<StreamProperties> inputProperties)
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
            StreamProperties rightProperties = inputProperties.get(1);

            switch (node.getType()) {
                case INNER:
                    return leftProperties;
                case LEFT:
                    // the left can contain nulls in any stream so we can't say anything about the
                    // partitioning but the other properties of the left will be maintained.
                    return leftProperties.withUnspecifiedPartitioning();
                case RIGHT:
                    // all of the "nulls" from the right are produced from a separate stream, so the
                    // partitioning still holds, but we will always have multiple streams
                    return new StreamProperties(MULTIPLE, false, leftProperties.getPartitioningColumns(), false);
                case FULL:
                    // the left can contain nulls in any stream so we can't say anything about the
                    // partitioning, and nulls from the right are produced from a extra new stream
                    // so we will always have multiple streams.
                    return new StreamProperties(MULTIPLE, false, Optional.empty(), false);
                default:
                    throw new UnsupportedOperationException("Unsupported join type: " + node.getType());
            }
        }

        @Override
        public StreamProperties visitIndexJoin(IndexJoinNode node, List<StreamProperties> inputProperties)
        {
            StreamProperties probeProperties = inputProperties.get(0);

            switch (node.getType()) {
                case INNER:
                    return probeProperties;
                case SOURCE_OUTER:
                    // the probe can contain nulls in any stream so we can't say anything about the
                    // partitioning but the other properties of the probe will be maintained.
                    return probeProperties.withUnspecifiedPartitioning();
                default:
                    throw new UnsupportedOperationException("Unsupported join type: " + node.getType());
            }
        }

        //
        // Source nodes
        //

        @Override
        public StreamProperties visitValues(ValuesNode node, List<StreamProperties> context)
        {
            // values always produces a single stream
            return StreamProperties.singleStream();
        }

        @Override
        public StreamProperties visitTableScan(TableScanNode node, List<StreamProperties> inputProperties)
        {
            checkArgument(node.getLayout().isPresent(), "table layout has not yet been chosen");

            TableLayout layout = metadata.getLayout(session, node.getLayout().get());
            Map<ColumnHandle, Symbol> assignments = ImmutableBiMap.copyOf(node.getAssignments()).inverse();

            // Globally constant assignments
            Set<ColumnHandle> constants = new HashSet<>();
            extractFixedValues(node.getCurrentConstraint()).orElse(ImmutableMap.of())
                    .entrySet().stream()
                    .filter(entry -> !entry.getValue().isNull())  // TODO consider allowing nulls
                    .forEach(entry -> constants.add(entry.getKey()));

            Optional<Set<Symbol>> partitionSymbols = layout.getPartitioningColumns()
                    .flatMap(columns -> getNonConstantSymbols(columns, assignments, constants));

            return new StreamProperties(MULTIPLE, false, partitionSymbols, false);
        }

        private static Optional<Set<Symbol>> getNonConstantSymbols(Set<ColumnHandle> columnHandles, Map<ColumnHandle, Symbol> assignments, Set<ColumnHandle> globalConstants)
        {
            // Strip off the constants from the partitioning columns (since those are not required for translation)
            Set<ColumnHandle> constantsStrippedPartitionColumns = columnHandles.stream()
                    .filter(column -> !globalConstants.contains(column))
                    .collect(toImmutableSet());
            ImmutableSet.Builder<Symbol> builder = ImmutableSet.builder();

            for (ColumnHandle column : constantsStrippedPartitionColumns) {
                Symbol translated = assignments.get(column);
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
            // remote exchange always produces a single stream
            if (node.getScope() == REMOTE) {
                return StreamProperties.singleStream();
            }

            switch (node.getType()) {
                case GATHER:
                    return StreamProperties.singleStream();
                case REPARTITION:
                    if (node.getPartitioningScheme().getPartitioning().getHandle().equals(FIXED_RANDOM_DISTRIBUTION)) {
                        return new StreamProperties(FIXED, false, Optional.empty(), false);
                    }
                    return new StreamProperties(
                            FIXED,
                            true,
                            Optional.of(node.getPartitioningScheme().getPartitioning().getArguments().stream()
                                    .map(ArgumentBinding::getColumn)
                                    .collect(toImmutableList())), false);
                case REPLICATE:
                    return new StreamProperties(MULTIPLE, false, Optional.empty(), false);
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
            Map<Symbol, Symbol> identities = computeIdentityTranslations(node.getAssignments());
            return properties.translate(column -> Optional.ofNullable(identities.get(column)));
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

        @Override
        public StreamProperties visitAggregation(AggregationNode node, List<StreamProperties> inputProperties)
        {
            StreamProperties properties = Iterables.getOnlyElement(inputProperties);

            // Only grouped symbols projected symbols are passed through
            return properties.translate(symbol -> node.getGroupBy().contains(symbol) ? Optional.of(symbol) : Optional.<Symbol>empty());
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
        public StreamProperties visitTableWriter(TableWriterNode node, List<StreamProperties> inputProperties)
        {
            StreamProperties properties = Iterables.getOnlyElement(inputProperties);
            // table writer only outputs the row count
            return properties.withUnspecifiedPartitioning();
        }

        @Override
        public StreamProperties visitUnnest(UnnestNode node, List<StreamProperties> inputProperties)
        {
            StreamProperties properties = Iterables.getOnlyElement(inputProperties);

            // We can describe properties in terms of inputs that are projected unmodified (i.e., not the unnested symbols)
            Set<Symbol> passThroughInputs = ImmutableSet.copyOf(node.getReplicateSymbols());
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

        //
        // Simple nodes that pass through stream properties
        //

        @Override
        public StreamProperties visitOutput(OutputNode node, List<StreamProperties> inputProperties)
        {
            return Iterables.getOnlyElement(inputProperties);
        }

        @Override
        public StreamProperties visitMarkDistinct(MarkDistinctNode node, List<StreamProperties> inputProperties)
        {
            return Iterables.getOnlyElement(inputProperties);
        }

        @Override
        public StreamProperties visitGroupId(GroupIdNode node, List<StreamProperties> inputProperties)
        {
            return Iterables.getOnlyElement(inputProperties);
        }

        @Override
        public StreamProperties visitWindow(WindowNode node, List<StreamProperties> inputProperties)
        {
            return Iterables.getOnlyElement(inputProperties);
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
            return StreamProperties.ordered();
        }

        @Override
        public StreamProperties visitSort(SortNode node, List<StreamProperties> inputProperties)
        {
            return StreamProperties.ordered();
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
        public StreamProperties visitFilter(FilterNode node, List<StreamProperties> inputProperties)
        {
            return Iterables.getOnlyElement(inputProperties);
        }

        @Override
        public StreamProperties visitSample(SampleNode node, List<StreamProperties> inputProperties)
        {
            return Iterables.getOnlyElement(inputProperties);
        }
    }

    @Immutable
    public static final class StreamProperties
    {
        public enum StreamDistribution
        {
            SINGLE, MULTIPLE, FIXED
        }

        private final StreamDistribution distribution;

        private final boolean exactColumnOrder;
        private final Optional<List<Symbol>> partitioningColumns; // if missing => partitioned with some unknown scheme

        private final boolean ordered;

        // We are only interested in the local properties, but PropertyDerivations requires input
        // ActualProperties, so we hold on to the whole object
        private final ActualProperties otherActualProperties;

        // NOTE: Partitioning on zero columns (or effectively zero columns if the columns are constant) indicates that all
        // the rows will be partitioned into a single stream.

        private StreamProperties(StreamDistribution distribution, boolean exactColumnOrder, Optional<? extends Iterable<Symbol>> partitioningColumns, boolean ordered)
        {
            this(distribution, exactColumnOrder, partitioningColumns, ordered, null);
        }

        private StreamProperties(
                StreamDistribution distribution,
                boolean exactColumnOrder,
                Optional<? extends Iterable<Symbol>> partitioningColumns,
                boolean ordered,
                ActualProperties otherActualProperties)
        {
            this.distribution = requireNonNull(distribution, "distribution is null");

            this.exactColumnOrder = exactColumnOrder;
            this.partitioningColumns = requireNonNull(partitioningColumns, "partitioningProperties is null")
                    .map(ImmutableList::copyOf);

            checkArgument(distribution != SINGLE || this.partitioningColumns.equals(Optional.of(ImmutableList.of())),
                    "Single stream must be partitioned on empty set");

            this.ordered = ordered;
            checkArgument(!ordered || distribution == SINGLE, "Ordered must be a single stream");

            this.otherActualProperties = otherActualProperties;
        }

        public List<LocalProperty<Symbol>> getLocalProperties()
        {
            checkState(otherActualProperties != null, "otherActualProperties not set");
            return otherActualProperties.getLocalProperties();
        }

        private static StreamProperties singleStream()
        {
            return new StreamProperties(SINGLE, false, Optional.of(ImmutableSet.of()), false);
        }

        private static StreamProperties ordered()
        {
            return new StreamProperties(SINGLE, false, Optional.of(ImmutableSet.of()), true);
        }

        public boolean isSingleStream()
        {
            return distribution == SINGLE;
        }

        public StreamDistribution getDistribution()
        {
            return distribution;
        }

        public boolean isExactlyPartitionedOn(Iterable<Symbol> columns)
        {
            if (!partitioningColumns.isPresent()) {
                return false;
            }

            return columns.equals(ImmutableList.copyOf(partitioningColumns.get()));
        }

        public boolean isPartitionedOn(Iterable<Symbol> columns)
        {
            if (!partitioningColumns.isPresent()) {
                return false;
            }

            // partitioned on (k_1, k_2, ..., k_n) => partitioned on (k_1, k_2, ..., k_n, k_n+1, ...)
            // can safely ignore all constant columns when comparing partition properties
            return ImmutableSet.copyOf(columns).containsAll(partitioningColumns.get());
        }

        public Optional<List<Symbol>> getPartitioningColumns()
        {
            return partitioningColumns;
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
            return new StreamProperties(distribution, false, Optional.empty(), ordered);
        }

        private StreamProperties withOtherActualProperties(ActualProperties actualProperties)
        {
            return new StreamProperties(distribution, exactColumnOrder, partitioningColumns, ordered, actualProperties);
        }

        public StreamProperties translate(Function<Symbol, Optional<Symbol>> translator)
        {
            return new StreamProperties(
                    distribution,
                    exactColumnOrder,
                    partitioningColumns.flatMap(partitioning -> {
                        ImmutableList.Builder<Symbol> newPartitioningColumns = ImmutableList.builder();
                        for (Symbol partitioningColumn : partitioning) {
                            Optional<Symbol> translated = translator.apply(partitioningColumn);
                            if (!translated.isPresent()) {
                                return Optional.empty();
                            }
                            newPartitioningColumns.add(translated.get());
                        }
                        return Optional.of(newPartitioningColumns.build());
                    }),
                    ordered, otherActualProperties.translate(translator));
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(distribution, partitioningColumns);
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
                    Objects.equals(this.partitioningColumns, other.partitioningColumns);
        }

        @Override
        public String toString()
        {
            return MoreObjects.toStringHelper(this)
                    .add("distribution", distribution)
                    .add("partitioningColumns", partitioningColumns)
                    .toString();
        }
    }
}
