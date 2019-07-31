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
import com.facebook.presto.connector.system.GlobalSystemConnector;
import com.facebook.presto.execution.QueryManagerConfig.ExchangeMaterializationStrategy;
import com.facebook.presto.execution.warnings.WarningCollector;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.GroupingProperty;
import com.facebook.presto.spi.LocalProperty;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SortingProperty;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.analyzer.FeaturesConfig.PartialMergePushdownStrategy;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.ExpressionDomainTranslator;
import com.facebook.presto.sql.planner.LiteralEncoder;
import com.facebook.presto.sql.planner.Partitioning;
import com.facebook.presto.sql.planner.PartitioningHandle;
import com.facebook.presto.sql.planner.PartitioningScheme;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.optimizations.PreferredProperties.PartitioningProperties;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.ChildReplacer;
import com.facebook.presto.sql.planner.plan.DistinctLimitNode;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode.Scope;
import com.facebook.presto.sql.planner.plan.ExplainAnalyzeNode;
import com.facebook.presto.sql.planner.plan.GroupIdNode;
import com.facebook.presto.sql.planner.plan.IndexJoinNode;
import com.facebook.presto.sql.planner.plan.IndexSourceNode;
import com.facebook.presto.sql.planner.plan.InternalPlanVisitor;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LateralJoinNode;
import com.facebook.presto.sql.planner.plan.MarkDistinctNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.RowNumberNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.SpatialJoinNode;
import com.facebook.presto.sql.planner.plan.StatisticsWriterNode;
import com.facebook.presto.sql.planner.plan.TableFinishNode;
import com.facebook.presto.sql.planner.plan.TableWriterNode;
import com.facebook.presto.sql.planner.plan.TopNRowNumberNode;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.facebook.presto.sql.planner.plan.UnnestNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.facebook.presto.SystemSessionProperties.getExchangeMaterializationStrategy;
import static com.facebook.presto.SystemSessionProperties.getHashPartitionCount;
import static com.facebook.presto.SystemSessionProperties.getPartialMergePushdownStrategy;
import static com.facebook.presto.SystemSessionProperties.getPartitioningProviderCatalog;
import static com.facebook.presto.SystemSessionProperties.isColocatedJoinEnabled;
import static com.facebook.presto.SystemSessionProperties.isDistributedIndexJoinEnabled;
import static com.facebook.presto.SystemSessionProperties.isDistributedSortEnabled;
import static com.facebook.presto.SystemSessionProperties.isForceSingleNodeOutput;
import static com.facebook.presto.SystemSessionProperties.isRedistributeWrites;
import static com.facebook.presto.SystemSessionProperties.isScaleWriters;
import static com.facebook.presto.SystemSessionProperties.preferStreamingOperators;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.plan.LimitNode.Step.PARTIAL;
import static com.facebook.presto.sql.planner.FragmentTableScanCounter.getNumberOfTableScans;
import static com.facebook.presto.sql.planner.FragmentTableScanCounter.hasMultipleTableScans;
import static com.facebook.presto.sql.planner.PlannerUtils.toVariableReference;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SCALED_WRITER_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.isCompatibleSystemPartitioning;
import static com.facebook.presto.sql.planner.iterative.rule.PickTableLayout.pushPredicateIntoTableScan;
import static com.facebook.presto.sql.planner.optimizations.ActualProperties.Global.partitionedOn;
import static com.facebook.presto.sql.planner.optimizations.ActualProperties.Global.singleStreamPartition;
import static com.facebook.presto.sql.planner.optimizations.LocalProperties.grouped;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE_MATERIALIZED;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE_STREAMING;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.GATHER;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.gatheringExchange;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.mergingExchange;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.partitionedExchange;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.replicatedExchange;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.roundRobinExchange;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToExpression;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

public class AddExchanges
        implements PlanOptimizer
{
    private final SqlParser parser;
    private final Metadata metadata;
    private final ExpressionDomainTranslator domainTranslator;

    public AddExchanges(Metadata metadata, SqlParser parser)
    {
        this.metadata = metadata;
        this.domainTranslator = new ExpressionDomainTranslator(new LiteralEncoder(metadata.getBlockEncodingSerde()));
        this.parser = parser;
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, PlanVariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        PlanWithProperties result = plan.accept(new Rewriter(idAllocator, variableAllocator, session), PreferredProperties.any());
        return result.getNode();
    }

    private class Rewriter
            extends InternalPlanVisitor<PlanWithProperties, PreferredProperties>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final PlanVariableAllocator variableAllocator;
        private final TypeProvider types;
        private final Session session;
        private final boolean distributedIndexJoins;
        private final boolean preferStreamingOperators;
        private final boolean redistributeWrites;
        private final boolean scaleWriters;
        private final PartialMergePushdownStrategy partialMergePushdownStrategy;
        private final String partitioningProviderCatalog;
        private final int hashPartitionCount;
        private final ExchangeMaterializationStrategy exchangeMaterializationStrategy;

        public Rewriter(PlanNodeIdAllocator idAllocator, PlanVariableAllocator variableAllocator, Session session)
        {
            this.idAllocator = idAllocator;
            this.variableAllocator = variableAllocator;
            this.types = variableAllocator.getTypes();
            this.session = session;
            this.distributedIndexJoins = isDistributedIndexJoinEnabled(session);
            this.redistributeWrites = isRedistributeWrites(session);
            this.scaleWriters = isScaleWriters(session);
            this.partialMergePushdownStrategy = getPartialMergePushdownStrategy(session);
            this.preferStreamingOperators = preferStreamingOperators(session);
            this.partitioningProviderCatalog = getPartitioningProviderCatalog(session);
            this.hashPartitionCount = getHashPartitionCount(session);
            this.exchangeMaterializationStrategy = getExchangeMaterializationStrategy(session);
        }

        @Override
        public PlanWithProperties visitPlan(PlanNode node, PreferredProperties preferredProperties)
        {
            return rebaseAndDeriveProperties(node, planChild(node, preferredProperties));
        }

        @Override
        public PlanWithProperties visitProject(ProjectNode node, PreferredProperties preferredProperties)
        {
            Map<VariableReferenceExpression, VariableReferenceExpression> identities = computeIdentityTranslations(node.getAssignments(), types);
            PreferredProperties translatedPreferred = preferredProperties.translate(symbol -> Optional.ofNullable(identities.get(symbol)));

            return rebaseAndDeriveProperties(node, planChild(node, translatedPreferred));
        }

        @Override
        public PlanWithProperties visitOutput(OutputNode node, PreferredProperties preferredProperties)
        {
            PlanWithProperties child = planChild(node, PreferredProperties.undistributed());

            if (!child.getProperties().isSingleNode() && isForceSingleNodeOutput(session)) {
                child = withDerivedProperties(
                        gatheringExchange(idAllocator.getNextId(), REMOTE_STREAMING, child.getNode()),
                        child.getProperties());
            }

            return rebaseAndDeriveProperties(node, child);
        }

        @Override
        public PlanWithProperties visitEnforceSingleRow(EnforceSingleRowNode node, PreferredProperties preferredProperties)
        {
            PlanWithProperties child = planChild(node, PreferredProperties.any());

            if (!child.getProperties().isSingleNode()) {
                child = withDerivedProperties(
                        gatheringExchange(idAllocator.getNextId(), REMOTE_STREAMING, child.getNode()),
                        child.getProperties());
            }

            return rebaseAndDeriveProperties(node, child);
        }

        @Override
        public PlanWithProperties visitAggregation(AggregationNode node, PreferredProperties parentPreferredProperties)
        {
            Set<VariableReferenceExpression> partitioningRequirement = ImmutableSet.copyOf(node.getGroupingKeys());

            boolean preferSingleNode = node.hasSingleNodeExecutionPreference(metadata.getFunctionManager());
            PreferredProperties preferredProperties = preferSingleNode ? PreferredProperties.undistributed() : PreferredProperties.any();

            if (!node.getGroupingKeys().isEmpty()) {
                preferredProperties = PreferredProperties.partitionedWithLocal(partitioningRequirement, grouped(node.getGroupingKeys()))
                        .mergeWithParent(parentPreferredProperties);
            }

            PlanWithProperties child = planChild(node, preferredProperties);

            if (child.getProperties().isSingleNode()) {
                // If already unpartitioned, just drop the single aggregation back on
                return rebaseAndDeriveProperties(node, child);
            }

            if (preferSingleNode) {
                child = withDerivedProperties(
                        gatheringExchange(idAllocator.getNextId(), REMOTE_STREAMING, child.getNode()),
                        child.getProperties());
            }
            else if (!child.getProperties().isStreamPartitionedOn(partitioningRequirement) && !child.getProperties().isNodePartitionedOn(partitioningRequirement)) {
                child = withDerivedProperties(
                        partitionedExchange(
                                idAllocator.getNextId(),
                                selectExchangeScopeForPartitionedRemoteExchange(child.getNode(), false),
                                child.getNode(),
                                createPartitioning(node.getGroupingKeys()),
                                node.getHashVariable()),
                        child.getProperties());
            }
            return rebaseAndDeriveProperties(node, child);
        }

        @Override
        public PlanWithProperties visitGroupId(GroupIdNode node, PreferredProperties preferredProperties)
        {
            PreferredProperties childPreference = preferredProperties.translate(translateGroupIdVariables(node));
            PlanWithProperties child = planChild(node, childPreference);
            return rebaseAndDeriveProperties(node, child);
        }

        private Function<VariableReferenceExpression, Optional<VariableReferenceExpression>> translateGroupIdVariables(GroupIdNode node)
        {
            return variable -> {
                if (node.getAggregationArguments().contains(variable)) {
                    return Optional.of(variable);
                }

                if (node.getCommonGroupingColumns().contains(variable)) {
                    return Optional.of((node.getGroupingColumns().get(variable)));
                }

                return Optional.empty();
            };
        }

        @Override
        public PlanWithProperties visitMarkDistinct(MarkDistinctNode node, PreferredProperties preferredProperties)
        {
            PreferredProperties preferredChildProperties = PreferredProperties.partitionedWithLocal(ImmutableSet.copyOf(node.getDistinctVariables()), grouped(node.getDistinctVariables()))
                    .mergeWithParent(preferredProperties);
            PlanWithProperties child = node.getSource().accept(this, preferredChildProperties);

            if (child.getProperties().isSingleNode() ||
                    !child.getProperties().isStreamPartitionedOn(node.getDistinctVariables())) {
                child = withDerivedProperties(
                        partitionedExchange(
                                idAllocator.getNextId(),
                                selectExchangeScopeForPartitionedRemoteExchange(child.getNode(), false),
                                child.getNode(),
                                createPartitioning(node.getDistinctVariables()),
                                node.getHashVariable()),
                        child.getProperties());
            }

            return rebaseAndDeriveProperties(node, child);
        }

        @Override
        public PlanWithProperties visitWindow(WindowNode node, PreferredProperties preferredProperties)
        {
            List<LocalProperty<VariableReferenceExpression>> desiredProperties = new ArrayList<>();
            if (!node.getPartitionBy().isEmpty()) {
                desiredProperties.add(new GroupingProperty<>(node.getPartitionBy()));
            }
            node.getOrderingScheme().ifPresent(orderingScheme ->
                    orderingScheme.getOrderByVariables().stream()
                            .map(variable -> new SortingProperty<>(variable, orderingScheme.getOrdering(variable)))
                            .forEach(desiredProperties::add));

            PlanWithProperties child = planChild(
                    node,
                    PreferredProperties.partitionedWithLocal(ImmutableSet.copyOf(node.getPartitionBy()), desiredProperties)
                            .mergeWithParent(preferredProperties));

            if (!child.getProperties().isStreamPartitionedOn(node.getPartitionBy()) &&
                    !child.getProperties().isNodePartitionedOn(node.getPartitionBy())) {
                if (node.getPartitionBy().isEmpty()) {
                    child = withDerivedProperties(
                            gatheringExchange(idAllocator.getNextId(), REMOTE_STREAMING, child.getNode()),
                            child.getProperties());
                }
                else {
                    child = withDerivedProperties(
                            partitionedExchange(
                                    idAllocator.getNextId(),
                                    selectExchangeScopeForPartitionedRemoteExchange(child.getNode(), false),
                                    child.getNode(),
                                    createPartitioning(node.getPartitionBy()),
                                    node.getHashVariable()),
                            child.getProperties());
                }
            }

            return rebaseAndDeriveProperties(node, child);
        }

        @Override
        public PlanWithProperties visitRowNumber(RowNumberNode node, PreferredProperties preferredProperties)
        {
            if (node.getPartitionBy().isEmpty()) {
                PlanWithProperties child = planChild(node, PreferredProperties.undistributed());

                if (!child.getProperties().isSingleNode()) {
                    child = withDerivedProperties(
                            gatheringExchange(idAllocator.getNextId(), REMOTE_STREAMING, child.getNode()),
                            child.getProperties());
                }

                return rebaseAndDeriveProperties(node, child);
            }

            PlanWithProperties child = planChild(
                    node,
                    PreferredProperties.partitionedWithLocal(ImmutableSet.copyOf(node.getPartitionBy()), grouped(node.getPartitionBy()))
                            .mergeWithParent(preferredProperties));

            // TODO: add config option/session property to force parallel plan if child is unpartitioned and window has a PARTITION BY clause
            if (!child.getProperties().isStreamPartitionedOn(node.getPartitionBy())
                    && !child.getProperties().isNodePartitionedOn(node.getPartitionBy())) {
                child = withDerivedProperties(
                        partitionedExchange(
                                idAllocator.getNextId(),
                                selectExchangeScopeForPartitionedRemoteExchange(child.getNode(), false),
                                child.getNode(),
                                createPartitioning(node.getPartitionBy()),
                                node.getHashVariable()),
                        child.getProperties());
            }

            // TODO: streaming

            return rebaseAndDeriveProperties(node, child);
        }

        @Override
        public PlanWithProperties visitTopNRowNumber(TopNRowNumberNode node, PreferredProperties preferredProperties)
        {
            PreferredProperties preferredChildProperties;
            Function<PlanNode, PlanNode> addExchange;

            if (node.getPartitionBy().isEmpty()) {
                preferredChildProperties = PreferredProperties.any();
                addExchange = partial -> gatheringExchange(idAllocator.getNextId(), REMOTE_STREAMING, partial);
            }
            else {
                preferredChildProperties = PreferredProperties.partitionedWithLocal(ImmutableSet.copyOf(node.getPartitionBy()), grouped(node.getPartitionBy()))
                        .mergeWithParent(preferredProperties);
                addExchange = partial -> partitionedExchange(
                        idAllocator.getNextId(),
                        selectExchangeScopeForPartitionedRemoteExchange(partial, false),
                        partial,
                        createPartitioning(node.getPartitionBy()),
                        node.getHashVariable());
            }

            PlanWithProperties child = planChild(node, preferredChildProperties);
            if (!child.getProperties().isStreamPartitionedOn(node.getPartitionBy())
                    && !child.getProperties().isNodePartitionedOn(node.getPartitionBy())) {
                // add exchange + push function to child
                child = withDerivedProperties(
                        new TopNRowNumberNode(
                                idAllocator.getNextId(),
                                child.getNode(),
                                node.getSpecification(),
                                node.getRowNumberVariable(),
                                node.getMaxRowCountPerPartition(),
                                true,
                                node.getHashVariable()),
                        child.getProperties());

                child = withDerivedProperties(addExchange.apply(child.getNode()), child.getProperties());
            }

            return rebaseAndDeriveProperties(node, child);
        }

        @Override
        public PlanWithProperties visitTopN(TopNNode node, PreferredProperties preferredProperties)
        {
            PlanWithProperties child;
            switch (node.getStep()) {
                case SINGLE:
                case FINAL:
                    child = planChild(node, PreferredProperties.undistributed());
                    if (!child.getProperties().isSingleNode()) {
                        child = withDerivedProperties(
                                gatheringExchange(idAllocator.getNextId(), REMOTE_STREAMING, child.getNode()),
                                child.getProperties());
                    }
                    break;
                case PARTIAL:
                    child = planChild(node, PreferredProperties.any());
                    break;
                default:
                    throw new UnsupportedOperationException(format("Unsupported step for TopN [%s]", node.getStep()));
            }
            return rebaseAndDeriveProperties(node, child);
        }

        @Override
        public PlanWithProperties visitSort(SortNode node, PreferredProperties preferredProperties)
        {
            PlanWithProperties child = planChild(node, PreferredProperties.undistributed());

            if (child.getProperties().isSingleNode()) {
                // current plan so far is single node, so local properties are effectively global properties
                // skip the SortNode if the local properties guarantee ordering on Sort keys
                // TODO: This should be extracted as a separate optimizer once the planner is able to reason about the ordering of each operator
                List<LocalProperty<VariableReferenceExpression>> desiredProperties = new ArrayList<>();
                for (VariableReferenceExpression variable : node.getOrderingScheme().getOrderByVariables()) {
                    desiredProperties.add(new SortingProperty<>(variable, node.getOrderingScheme().getOrdering(variable)));
                }

                if (LocalProperties.match(child.getProperties().getLocalProperties(), desiredProperties).stream()
                        .noneMatch(Optional::isPresent)) {
                    return child;
                }
            }

            if (isDistributedSortEnabled(session)) {
                child = planChild(node, PreferredProperties.any());
                // insert round robin exchange to eliminate skewness issues
                PlanNode source = roundRobinExchange(idAllocator.getNextId(), REMOTE_STREAMING, child.getNode());
                return withDerivedProperties(
                        mergingExchange(
                                idAllocator.getNextId(),
                                REMOTE_STREAMING,
                                new SortNode(
                                        idAllocator.getNextId(),
                                        source,
                                        node.getOrderingScheme()),
                                node.getOrderingScheme()),
                        child.getProperties());
            }

            if (!child.getProperties().isSingleNode()) {
                child = withDerivedProperties(
                        gatheringExchange(idAllocator.getNextId(), REMOTE_STREAMING, child.getNode()),
                        child.getProperties());
            }

            return rebaseAndDeriveProperties(node, child);
        }

        @Override
        public PlanWithProperties visitLimit(LimitNode node, PreferredProperties preferredProperties)
        {
            PlanWithProperties child = planChild(node, PreferredProperties.any());

            if (!child.getProperties().isSingleNode()) {
                child = withDerivedProperties(
                        new LimitNode(idAllocator.getNextId(), child.getNode(), node.getCount(), PARTIAL),
                        child.getProperties());

                child = withDerivedProperties(
                        gatheringExchange(idAllocator.getNextId(), REMOTE_STREAMING, child.getNode()),
                        child.getProperties());
            }

            return rebaseAndDeriveProperties(node, child);
        }

        @Override
        public PlanWithProperties visitDistinctLimit(DistinctLimitNode node, PreferredProperties preferredProperties)
        {
            PlanWithProperties child = planChild(node, PreferredProperties.any());

            if (!child.getProperties().isSingleNode()) {
                child = withDerivedProperties(
                        gatheringExchange(
                                idAllocator.getNextId(),
                                REMOTE_STREAMING,
                                new DistinctLimitNode(idAllocator.getNextId(), child.getNode(), node.getLimit(), true, node.getDistinctVariables(), node.getHashVariable())),
                        child.getProperties());
            }

            return rebaseAndDeriveProperties(node, child);
        }

        @Override
        public PlanWithProperties visitFilter(FilterNode node, PreferredProperties preferredProperties)
        {
            if (node.getSource() instanceof TableScanNode) {
                return planTableScan((TableScanNode) node.getSource(), castToExpression(node.getPredicate()));
            }

            return rebaseAndDeriveProperties(node, planChild(node, preferredProperties));
        }

        @Override
        public PlanWithProperties visitTableScan(TableScanNode node, PreferredProperties preferredProperties)
        {
            return planTableScan(node, TRUE_LITERAL);
        }

        @Override
        public PlanWithProperties visitTableWriter(TableWriterNode node, PreferredProperties preferredProperties)
        {
            PlanWithProperties source = node.getSource().accept(this, preferredProperties);

            Optional<PartitioningScheme> partitioningScheme = node.getPartitioningScheme();
            if (!partitioningScheme.isPresent()) {
                if (scaleWriters) {
                    partitioningScheme = Optional.of(new PartitioningScheme(Partitioning.create(SCALED_WRITER_DISTRIBUTION, ImmutableList.of()), source.getNode().getOutputVariables()));
                }
                else if (redistributeWrites) {
                    partitioningScheme = Optional.of(new PartitioningScheme(Partitioning.create(FIXED_ARBITRARY_DISTRIBUTION, ImmutableList.of()), source.getNode().getOutputVariables()));
                }
            }

            if (partitioningScheme.isPresent() &&
                    // TODO: Deprecate compatible table partitioning
                    !source.getProperties().isCompatibleTablePartitioningWith(partitioningScheme.get().getPartitioning(), false, metadata, session) &&
                    !(source.getProperties().isRefinedPartitioningOver(partitioningScheme.get().getPartitioning(), false, metadata, session) &&
                            canPushdownPartialMerge(source.getNode(), partialMergePushdownStrategy))) {
                source = withDerivedProperties(
                        partitionedExchange(
                                idAllocator.getNextId(),
                                REMOTE_STREAMING,
                                source.getNode(),
                                partitioningScheme.get()),
                        source.getProperties());
            }
            return rebaseAndDeriveProperties(node, source);
        }

        private PlanWithProperties planTableScan(TableScanNode node, Expression predicate)
        {
            PlanNode plan = pushPredicateIntoTableScan(node, predicate, true, session, types, idAllocator, metadata, parser, domainTranslator);
            // TODO: Support selecting layout with best local property once connector can participate in query optimization.
            return new PlanWithProperties(plan, derivePropertiesRecursively(plan));
        }

        @Override
        public PlanWithProperties visitValues(ValuesNode node, PreferredProperties preferredProperties)
        {
            return new PlanWithProperties(
                    node,
                    ActualProperties.builder()
                            .global(singleStreamPartition())
                            .build());
        }

        @Override
        public PlanWithProperties visitExplainAnalyze(ExplainAnalyzeNode node, PreferredProperties preferredProperties)
        {
            PlanWithProperties child = planChild(node, PreferredProperties.any());

            // if the child is already a gathering exchange, don't add another
            if ((child.getNode() instanceof ExchangeNode) && ((ExchangeNode) child.getNode()).getType() == ExchangeNode.Type.GATHER) {
                return rebaseAndDeriveProperties(node, child);
            }

            // Always add an exchange because ExplainAnalyze should be in its own stage
            child = withDerivedProperties(
                    gatheringExchange(idAllocator.getNextId(), REMOTE_STREAMING, child.getNode()),
                    child.getProperties());

            return rebaseAndDeriveProperties(node, child);
        }

        @Override
        public PlanWithProperties visitStatisticsWriterNode(StatisticsWriterNode node, PreferredProperties context)
        {
            PlanWithProperties child = planChild(node, PreferredProperties.any());

            // if the child is already a gathering exchange, don't add another
            if ((child.getNode() instanceof ExchangeNode) && ((ExchangeNode) child.getNode()).getType().equals(GATHER)) {
                return rebaseAndDeriveProperties(node, child);
            }

            if (!child.getProperties().isCoordinatorOnly()) {
                child = withDerivedProperties(
                        gatheringExchange(idAllocator.getNextId(), REMOTE_STREAMING, child.getNode()),
                        child.getProperties());
            }

            return rebaseAndDeriveProperties(node, child);
        }

        @Override
        public PlanWithProperties visitTableFinish(TableFinishNode node, PreferredProperties preferredProperties)
        {
            PlanWithProperties child = planChild(node, PreferredProperties.any());

            // if the child is already a gathering exchange, don't add another
            if ((child.getNode() instanceof ExchangeNode) && ((ExchangeNode) child.getNode()).getType().equals(GATHER)) {
                return rebaseAndDeriveProperties(node, child);
            }

            if (!child.getProperties().isCoordinatorOnly()) {
                child = withDerivedProperties(
                        gatheringExchange(idAllocator.getNextId(), REMOTE_STREAMING, child.getNode()),
                        child.getProperties());
            }

            return rebaseAndDeriveProperties(node, child);
        }

        private <T> SetMultimap<T, T> createMapping(List<T> keys, List<T> values)
        {
            checkArgument(keys.size() == values.size(), "Inputs must have the same size");
            ImmutableSetMultimap.Builder<T, T> builder = ImmutableSetMultimap.builder();
            for (int i = 0; i < keys.size(); i++) {
                builder.put(keys.get(i), values.get(i));
            }
            return builder.build();
        }

        private <T> Function<T, Optional<T>> createTranslator(SetMultimap<T, T> inputToOutput)
        {
            return input -> inputToOutput.get(input).stream().findAny();
        }

        private <T> Function<T, T> createDirectTranslator(SetMultimap<T, T> inputToOutput)
        {
            return input -> inputToOutput.get(input).iterator().next();
        }

        @Override
        public PlanWithProperties visitJoin(JoinNode node, PreferredProperties preferredProperties)
        {
            List<VariableReferenceExpression> leftVariables = node.getCriteria().stream()
                    .map(JoinNode.EquiJoinClause::getLeft)
                    .collect(toImmutableList());
            List<VariableReferenceExpression> rightVariables = node.getCriteria().stream()
                    .map(JoinNode.EquiJoinClause::getRight)
                    .collect(toImmutableList());

            JoinNode.DistributionType distributionType = node.getDistributionType().orElseThrow(() -> new IllegalArgumentException("distributionType not yet set"));

            if (distributionType == JoinNode.DistributionType.REPLICATED) {
                PlanWithProperties left = node.getLeft().accept(this, PreferredProperties.any());

                // use partitioned join if probe side is naturally partitioned on join symbols (e.g: because of aggregation)
                if (!node.getCriteria().isEmpty()
                        && left.getProperties().isNodePartitionedOn(leftVariables) && !left.getProperties().isSingleNode()) {
                    return planPartitionedJoin(node, leftVariables, rightVariables, left);
                }

                return planReplicatedJoin(node, left);
            }
            else {
                return planPartitionedJoin(node, leftVariables, rightVariables);
            }
        }

        private PlanWithProperties planPartitionedJoin(JoinNode node, List<VariableReferenceExpression> leftVariables, List<VariableReferenceExpression> rightVariables)
        {
            return planPartitionedJoin(node, leftVariables, rightVariables, node.getLeft().accept(this, PreferredProperties.partitioned(ImmutableSet.copyOf(leftVariables))));
        }

        private PlanWithProperties planPartitionedJoin(JoinNode node, List<VariableReferenceExpression> leftVariables, List<VariableReferenceExpression> rightVariables, PlanWithProperties left)
        {
            SetMultimap<VariableReferenceExpression, VariableReferenceExpression> rightToLeft = createMapping(rightVariables, leftVariables);
            SetMultimap<VariableReferenceExpression, VariableReferenceExpression> leftToRight = createMapping(leftVariables, rightVariables);

            PlanWithProperties right;

            if (left.getProperties().isNodePartitionedOn(leftVariables) && !left.getProperties().isSingleNode()) {
                Partitioning rightPartitioning = left.getProperties().translate(createTranslator(leftToRight)).getNodePartitioning().get();
                right = node.getRight().accept(this, PreferredProperties.partitioned(rightPartitioning));
                if (!right.getProperties().isCompatibleTablePartitioningWith(left.getProperties(), rightToLeft::get, metadata, session) &&
                        // TODO: Deprecate compatible table partitioning
                        !(right.getProperties().isRefinedPartitioningOver(left.getProperties(), rightToLeft::get, metadata, session) &&
                                canPushdownPartialMerge(right.getNode(), partialMergePushdownStrategy))) {
                    right = withDerivedProperties(
                            partitionedExchange(
                                    idAllocator.getNextId(),
                                    selectExchangeScopeForPartitionedRemoteExchange(right.getNode(), false),
                                    right.getNode(),
                                    new PartitioningScheme(rightPartitioning, right.getNode().getOutputVariables())),
                            right.getProperties());
                }
            }
            else {
                right = node.getRight().accept(this, PreferredProperties.partitioned(ImmutableSet.copyOf(rightVariables)));

                if (right.getProperties().isNodePartitionedOn(rightVariables) && !right.getProperties().isSingleNode()) {
                    Partitioning leftPartitioning = right.getProperties().translate(createTranslator(rightToLeft)).getNodePartitioning().get();
                    left = withDerivedProperties(
                            partitionedExchange(
                                    idAllocator.getNextId(),
                                    selectExchangeScopeForPartitionedRemoteExchange(left.getNode(), false),
                                    left.getNode(),
                                    new PartitioningScheme(leftPartitioning, left.getNode().getOutputVariables())),
                            left.getProperties());
                }
                else {
                    left = withDerivedProperties(
                            partitionedExchange(
                                    idAllocator.getNextId(),
                                    selectExchangeScopeForPartitionedRemoteExchange(left.getNode(), false),
                                    left.getNode(),
                                    createPartitioning(leftVariables),
                                    Optional.empty()),
                            left.getProperties());
                    right = withDerivedProperties(
                            partitionedExchange(
                                    idAllocator.getNextId(),
                                    selectExchangeScopeForPartitionedRemoteExchange(right.getNode(), false),
                                    right.getNode(),
                                    createPartitioning(rightVariables),
                                    Optional.empty()),
                            right.getProperties());
                }
            }

            // TODO: Deprecate compatible table partitioning
            verify(left.getProperties().isCompatibleTablePartitioningWith(right.getProperties(), leftToRight::get, metadata, session) ||
                    (right.getProperties().isRefinedPartitioningOver(left.getProperties(), rightToLeft::get, metadata, session) && canPushdownPartialMerge(right.getNode(), partialMergePushdownStrategy)));

            // if colocated joins are disabled, force redistribute when using a custom partitioning
            if (!isColocatedJoinEnabled(session) && hasMultipleTableScans(left.getNode(), right.getNode())) {
                Partitioning rightPartitioning = left.getProperties().translate(createTranslator(leftToRight)).getNodePartitioning().get();
                right = withDerivedProperties(
                        partitionedExchange(
                                idAllocator.getNextId(),
                                selectExchangeScopeForPartitionedRemoteExchange(right.getNode(), false),
                                right.getNode(),
                                new PartitioningScheme(rightPartitioning, right.getNode().getOutputVariables())),
                        right.getProperties());
            }

            return buildJoin(node, left, right, JoinNode.DistributionType.PARTITIONED);
        }

        private PlanWithProperties planReplicatedJoin(JoinNode node, PlanWithProperties left)
        {
            // Broadcast Join
            PlanWithProperties right = node.getRight().accept(this, PreferredProperties.any());

            if (left.getProperties().isSingleNode()) {
                if (!right.getProperties().isSingleNode() ||
                        (!isColocatedJoinEnabled(session) && hasMultipleTableScans(left.getNode(), right.getNode()))) {
                    right = withDerivedProperties(
                            gatheringExchange(idAllocator.getNextId(), REMOTE_STREAMING, right.getNode()),
                            right.getProperties());
                }
            }
            else {
                right = withDerivedProperties(
                        replicatedExchange(idAllocator.getNextId(), REMOTE_STREAMING, right.getNode()),
                        right.getProperties());
            }

            return buildJoin(node, left, right, JoinNode.DistributionType.REPLICATED);
        }

        private PlanWithProperties buildJoin(JoinNode node, PlanWithProperties newLeft, PlanWithProperties newRight, JoinNode.DistributionType newDistributionType)
        {
            JoinNode result = new JoinNode(node.getId(),
                    node.getType(),
                    newLeft.getNode(),
                    newRight.getNode(),
                    node.getCriteria(),
                    node.getOutputVariables(),
                    node.getFilter(),
                    node.getLeftHashVariable(),
                    node.getRightHashVariable(),
                    Optional.of(newDistributionType));

            return new PlanWithProperties(result, deriveProperties(result, ImmutableList.of(newLeft.getProperties(), newRight.getProperties())));
        }

        @Override
        public PlanWithProperties visitSpatialJoin(SpatialJoinNode node, PreferredProperties preferredProperties)
        {
            SpatialJoinNode.DistributionType distributionType = node.getDistributionType();

            PlanWithProperties left = node.getLeft().accept(this, PreferredProperties.any());
            PlanWithProperties right = node.getRight().accept(this, PreferredProperties.any());

            if (distributionType == SpatialJoinNode.DistributionType.REPLICATED) {
                if (left.getProperties().isSingleNode()) {
                    if (!right.getProperties().isSingleNode()) {
                        right = withDerivedProperties(
                                gatheringExchange(idAllocator.getNextId(), REMOTE_STREAMING, right.getNode()),
                                right.getProperties());
                    }
                }
                else {
                    right = withDerivedProperties(
                            replicatedExchange(idAllocator.getNextId(), REMOTE_STREAMING, right.getNode()),
                            right.getProperties());
                }
            }
            else {
                left = withDerivedProperties(
                        partitionedExchange(idAllocator.getNextId(), REMOTE_STREAMING, left.getNode(), createPartitioning(ImmutableList.of(node.getLeftPartitionVariable().get())), Optional.empty()),
                        left.getProperties());
                right = withDerivedProperties(
                        partitionedExchange(idAllocator.getNextId(), REMOTE_STREAMING, right.getNode(), createPartitioning(ImmutableList.of(node.getRightPartitionVariable().get())), Optional.empty()),
                        right.getProperties());
            }

            PlanNode newJoinNode = node.replaceChildren(ImmutableList.of(left.getNode(), right.getNode()));
            return new PlanWithProperties(newJoinNode, deriveProperties(newJoinNode, ImmutableList.of(left.getProperties(), right.getProperties())));
        }

        @Override
        public PlanWithProperties visitUnnest(UnnestNode node, PreferredProperties preferredProperties)
        {
            PreferredProperties translatedPreferred = preferredProperties.translate(variable -> node.getReplicateVariables().contains(variable) ? Optional.of(variable) : Optional.empty());

            return rebaseAndDeriveProperties(node, planChild(node, translatedPreferred));
        }

        @Override
        public PlanWithProperties visitSemiJoin(SemiJoinNode node, PreferredProperties preferredProperties)
        {
            PlanWithProperties source;
            PlanWithProperties filteringSource;

            SemiJoinNode.DistributionType distributionType = node.getDistributionType().orElseThrow(() -> new IllegalArgumentException("distributionType not yet set"));
            if (distributionType == SemiJoinNode.DistributionType.PARTITIONED) {
                List<VariableReferenceExpression> sourceVariables = ImmutableList.of(node.getSourceJoinVariable());
                List<VariableReferenceExpression> filteringSourceVariables = ImmutableList.of(node.getFilteringSourceJoinVariable());

                SetMultimap<VariableReferenceExpression, VariableReferenceExpression> sourceToFiltering = createMapping(sourceVariables, filteringSourceVariables);
                SetMultimap<VariableReferenceExpression, VariableReferenceExpression> filteringToSource = createMapping(filteringSourceVariables, sourceVariables);

                source = node.getSource().accept(this, PreferredProperties.partitioned(ImmutableSet.copyOf(sourceVariables)));

                if (source.getProperties().isNodePartitionedOn(sourceVariables) && !source.getProperties().isSingleNode()) {
                    Partitioning filteringPartitioning = source.getProperties().translate(createTranslator(sourceToFiltering)).getNodePartitioning().get();
                    filteringSource = node.getFilteringSource().accept(this, PreferredProperties.partitionedWithNullsAndAnyReplicated(filteringPartitioning));
                    // TODO: Deprecate compatible table partitioning
                    if (!source.getProperties().withReplicatedNulls(true).isCompatibleTablePartitioningWith(filteringSource.getProperties(), sourceToFiltering::get, metadata, session) &&
                            !(filteringSource.getProperties().withReplicatedNulls(true).isRefinedPartitioningOver(source.getProperties(), filteringToSource::get, metadata, session) &&
                                    canPushdownPartialMerge(filteringSource.getNode(), partialMergePushdownStrategy))) {
                        filteringSource = withDerivedProperties(
                                partitionedExchange(idAllocator.getNextId(), REMOTE_STREAMING, filteringSource.getNode(), new PartitioningScheme(
                                        filteringPartitioning,
                                        filteringSource.getNode().getOutputVariables(),
                                        Optional.empty(),
                                        true,
                                        Optional.empty())),
                                filteringSource.getProperties());
                    }
                }
                else {
                    filteringSource = node.getFilteringSource().accept(this, PreferredProperties.partitionedWithNullsAndAnyReplicated(ImmutableSet.copyOf(filteringSourceVariables)));

                    if (filteringSource.getProperties().isNodePartitionedOn(filteringSourceVariables, true) && !filteringSource.getProperties().isSingleNode()) {
                        Partitioning sourcePartitioning = filteringSource.getProperties().translate(createTranslator(filteringToSource)).getNodePartitioning().get();
                        source = withDerivedProperties(
                                partitionedExchange(idAllocator.getNextId(), REMOTE_STREAMING, source.getNode(), new PartitioningScheme(sourcePartitioning, source.getNode().getOutputVariables())),
                                source.getProperties());
                    }
                    else {
                        source = withDerivedProperties(
                                partitionedExchange(
                                        idAllocator.getNextId(),
                                        REMOTE_STREAMING,
                                        source.getNode(),
                                        createPartitioning(sourceVariables),
                                        Optional.empty()),
                                source.getProperties());
                        filteringSource = withDerivedProperties(
                                partitionedExchange(idAllocator.getNextId(), REMOTE_STREAMING, filteringSource.getNode(), createPartitioning(filteringSourceVariables), Optional.empty(), true),
                                filteringSource.getProperties());
                    }
                }

                verify(source.getProperties().withReplicatedNulls(true).isCompatibleTablePartitioningWith(filteringSource.getProperties(), sourceToFiltering::get, metadata, session) ||
                        // TODO: Deprecate compatible table partitioning
                        (filteringSource.getProperties().withReplicatedNulls(true).isRefinedPartitioningOver(source.getProperties(), filteringToSource::get, metadata, session) && canPushdownPartialMerge(filteringSource.getNode(), partialMergePushdownStrategy)));

                // if colocated joins are disabled, force redistribute when using a custom partitioning
                if (!isColocatedJoinEnabled(session) && hasMultipleTableScans(source.getNode(), filteringSource.getNode())) {
                    Partitioning filteringPartitioning = source.getProperties().translate(createTranslator(sourceToFiltering)).getNodePartitioning().get();
                    filteringSource = withDerivedProperties(
                            partitionedExchange(idAllocator.getNextId(), REMOTE_STREAMING, filteringSource.getNode(), new PartitioningScheme(
                                    filteringPartitioning,
                                    filteringSource.getNode().getOutputVariables(),
                                    Optional.empty(),
                                    true,
                                    Optional.empty())),
                            filteringSource.getProperties());
                }
            }
            else {
                source = node.getSource().accept(this, PreferredProperties.any());
                // Delete operator works fine even if TableScans on the filtering (right) side is not co-located with itself. It only cares about the corresponding TableScan,
                // which is always on the source (left) side. Therefore, hash-partitioned semi-join is always allowed on the filtering side.
                filteringSource = node.getFilteringSource().accept(this, PreferredProperties.any());

                // make filtering source match requirements of source
                if (source.getProperties().isSingleNode()) {
                    if (!filteringSource.getProperties().isSingleNode() ||
                            (!isColocatedJoinEnabled(session) && hasMultipleTableScans(source.getNode(), filteringSource.getNode()))) {
                        filteringSource = withDerivedProperties(
                                gatheringExchange(idAllocator.getNextId(), REMOTE_STREAMING, filteringSource.getNode()),
                                filteringSource.getProperties());
                    }
                }
                else {
                    filteringSource = withDerivedProperties(
                            replicatedExchange(idAllocator.getNextId(), REMOTE_STREAMING, filteringSource.getNode()),
                            filteringSource.getProperties());
                }
            }

            return rebaseAndDeriveProperties(node, ImmutableList.of(source, filteringSource));
        }

        @Override
        public PlanWithProperties visitIndexJoin(IndexJoinNode node, PreferredProperties preferredProperties)
        {
            List<VariableReferenceExpression> joinColumns = node.getCriteria().stream()
                    .map(IndexJoinNode.EquiJoinClause::getProbe)
                    .collect(toImmutableList());

            // Only prefer grouping on join columns if no parent local property preferences
            List<LocalProperty<VariableReferenceExpression>> desiredLocalProperties = preferredProperties.getLocalProperties().isEmpty() ? grouped(joinColumns) : ImmutableList.of();

            PlanWithProperties probeSource = node.getProbeSource().accept(this, PreferredProperties.partitionedWithLocal(ImmutableSet.copyOf(joinColumns), desiredLocalProperties)
                    .mergeWithParent(preferredProperties));
            ActualProperties probeProperties = probeSource.getProperties();

            PlanWithProperties indexSource = node.getIndexSource().accept(this, PreferredProperties.any());

            // TODO: allow repartitioning if unpartitioned to increase parallelism
            if (shouldRepartitionForIndexJoin(joinColumns, preferredProperties, probeProperties)) {
                probeSource = withDerivedProperties(
                        partitionedExchange(idAllocator.getNextId(), REMOTE_STREAMING, probeSource.getNode(), createPartitioning(joinColumns), node.getProbeHashVariable()),
                        probeProperties);
            }

            // TODO: if input is grouped, create streaming join

            // index side is really a nested-loops plan, so don't add exchanges
            PlanNode result = ChildReplacer.replaceChildren(node, ImmutableList.of(probeSource.getNode(), node.getIndexSource()));
            return new PlanWithProperties(result, deriveProperties(result, ImmutableList.of(probeSource.getProperties(), indexSource.getProperties())));
        }

        private boolean shouldRepartitionForIndexJoin(List<VariableReferenceExpression> joinColumns, PreferredProperties parentPreferredProperties, ActualProperties probeProperties)
        {
            // See if distributed index joins are enabled
            if (!distributedIndexJoins) {
                return false;
            }

            // No point in repartitioning if the plan is not distributed
            if (probeProperties.isSingleNode()) {
                return false;
            }

            Optional<PartitioningProperties> parentPartitioningPreferences = parentPreferredProperties.getGlobalProperties()
                    .flatMap(PreferredProperties.Global::getPartitioningProperties);

            // Disable repartitioning if it would disrupt a parent's partitioning preference when streaming is enabled
            boolean parentAlreadyPartitionedOnChild = parentPartitioningPreferences
                    .map(partitioning -> probeProperties.isStreamPartitionedOn(partitioning.getPartitioningColumns()))
                    .orElse(false);
            if (preferStreamingOperators && parentAlreadyPartitionedOnChild) {
                return false;
            }

            // Otherwise, repartition if we need to align with the join columns
            if (!probeProperties.isStreamPartitionedOn(joinColumns)) {
                return true;
            }

            // If we are already partitioned on the join columns because the data has been forced effectively into one stream,
            // then we should repartition if that would make a difference (from the single stream state).
            return probeProperties.isEffectivelySingleStream() && probeProperties.isStreamRepartitionEffective(joinColumns);
        }

        @Override
        public PlanWithProperties visitIndexSource(IndexSourceNode node, PreferredProperties preferredProperties)
        {
            return new PlanWithProperties(
                    node,
                    ActualProperties.builder()
                            .global(singleStreamPartition())
                            .build());
        }

        private Function<VariableReferenceExpression, Optional<VariableReferenceExpression>> outputToInputTranslator(UnionNode node, int sourceIndex, TypeProvider types)
        {
            return variable -> Optional.of(node.getVariableMapping().get(variable).get(sourceIndex));
        }

        private Partitioning selectUnionPartitioning(UnionNode node, PartitioningProperties parentPreference)
        {
            // Use the parent's requested partitioning if available
            if (parentPreference.getPartitioning().isPresent()) {
                return parentPreference.getPartitioning().get();
            }

            // Try planning the children to see if any of them naturally produce a partitioning (for now, just select the first)
            boolean nullsAndAnyReplicated = parentPreference.isNullsAndAnyReplicated();
            for (int sourceIndex = 0; sourceIndex < node.getSources().size(); sourceIndex++) {
                PreferredProperties.PartitioningProperties childPartitioning = parentPreference.translate(outputToInputTranslator(node, sourceIndex, types)).get();
                PreferredProperties childPreferred = PreferredProperties.builder()
                        .global(PreferredProperties.Global.distributed(childPartitioning.withNullsAndAnyReplicated(nullsAndAnyReplicated)))
                        .build();
                PlanWithProperties child = node.getSources().get(sourceIndex).accept(this, childPreferred);
                // Don't select a single node partitioning so that we maintain query parallelism
                // Theoretically, if all children are single partitioned on the same node we could choose a single
                // partitioning, but as this only applies to a union of two values nodes, it isn't worth the added complexity
                if (child.getProperties().isNodePartitionedOn(childPartitioning.getPartitioningColumns(), nullsAndAnyReplicated) && !child.getProperties().isSingleNode()) {
                    Function<VariableReferenceExpression, Optional<VariableReferenceExpression>> childToParent = createTranslator(createMapping(
                            node.sourceOutputLayout(sourceIndex),
                            node.getOutputVariables()));
                    return child.getProperties().translate(childToParent).getNodePartitioning().get();
                }
            }

            // Otherwise, choose an arbitrary partitioning over the columns
            return createPartitioning(ImmutableList.copyOf(parentPreference.getPartitioningColumns()));
        }

        @Override
        public PlanWithProperties visitUnion(UnionNode node, PreferredProperties parentPreference)
        {
            Optional<PreferredProperties.Global> parentPartitioningPreference = parentPreference.getGlobalProperties();

            // case 1: parent provides preferred distributed partitioning
            if (parentPartitioningPreference.isPresent() && parentPartitioningPreference.get().isDistributed() && parentPartitioningPreference.get().getPartitioningProperties().isPresent()) {
                PartitioningProperties parentPartitioningProperties = parentPartitioningPreference.get().getPartitioningProperties().get();
                boolean nullsAndAnyReplicated = parentPartitioningProperties.isNullsAndAnyReplicated();
                Partitioning desiredParentPartitioning = selectUnionPartitioning(node, parentPartitioningProperties);

                ImmutableList.Builder<PlanNode> partitionedSources = ImmutableList.builder();
                ImmutableListMultimap.Builder<VariableReferenceExpression, VariableReferenceExpression> outputToSourcesMapping = ImmutableListMultimap.builder();

                for (int sourceIndex = 0; sourceIndex < node.getSources().size(); sourceIndex++) {
                    Partitioning childPartitioning = desiredParentPartitioning.translate(createDirectTranslator(createMapping(
                            node.getOutputVariables(),
                            node.sourceOutputLayout(sourceIndex))));

                    PreferredProperties childPreferred = PreferredProperties.builder()
                            .global(PreferredProperties.Global.distributed(PartitioningProperties.partitioned(childPartitioning)
                                    .withNullsAndAnyReplicated(nullsAndAnyReplicated)))
                            .build();

                    PlanWithProperties source = node.getSources().get(sourceIndex).accept(this, childPreferred);
                    // TODO: Deprecate compatible table partitioning
                    if (!source.getProperties().isCompatibleTablePartitioningWith(childPartitioning, nullsAndAnyReplicated, metadata, session) &&
                            !(source.getProperties().isRefinedPartitioningOver(childPartitioning, nullsAndAnyReplicated, metadata, session) && canPushdownPartialMerge(source.getNode(), partialMergePushdownStrategy))) {
                        source = withDerivedProperties(
                                partitionedExchange(
                                        idAllocator.getNextId(),
                                        selectExchangeScopeForPartitionedRemoteExchange(source.getNode(), nullsAndAnyReplicated),
                                        source.getNode(),
                                        new PartitioningScheme(
                                                childPartitioning,
                                                source.getNode().getOutputVariables(),
                                                Optional.empty(),
                                                nullsAndAnyReplicated,
                                                Optional.empty())),
                                source.getProperties());
                    }
                    partitionedSources.add(source.getNode());

                    for (int column = 0; column < node.getOutputVariables().size(); column++) {
                        outputToSourcesMapping.put(node.getOutputVariables().get(column), node.sourceOutputLayout(sourceIndex).get(column));
                    }
                }
                UnionNode newNode = new UnionNode(
                        node.getId(),
                        partitionedSources.build(),
                        outputToSourcesMapping.build());

                return new PlanWithProperties(
                        newNode,
                        ActualProperties.builder()
                                .global(partitionedOn(desiredParentPartitioning, Optional.of(desiredParentPartitioning)))
                                .build()
                                .withReplicatedNulls(parentPartitioningProperties.isNullsAndAnyReplicated()));
            }

            // case 2: parent doesn't provide preferred distributed partitioning, this could be one of the following cases:
            //   * parentPartitioningPreference is Optional.empty()
            //   * parentPartitioningPreference is present, but is single node distribution
            //   * parentPartitioningPreference is present and is distributed, but does not have an explicit partitioning preference

            // first, classify children into single node and distributed
            List<PlanNode> singleNodeChildren = new ArrayList<>();
            List<List<VariableReferenceExpression>> singleNodeOutputLayouts = new ArrayList<>();

            List<PlanNode> distributedChildren = new ArrayList<>();
            List<List<VariableReferenceExpression>> distributedOutputLayouts = new ArrayList<>();

            for (int i = 0; i < node.getSources().size(); i++) {
                PlanWithProperties child = node.getSources().get(i).accept(this, PreferredProperties.any());
                if (child.getProperties().isSingleNode()) {
                    singleNodeChildren.add(child.getNode());
                    singleNodeOutputLayouts.add(node.sourceOutputLayout(i));
                }
                else {
                    distributedChildren.add(child.getNode());
                    // union may drop or duplicate symbols from the input so we must provide an exact mapping
                    distributedOutputLayouts.add(node.sourceOutputLayout(i));
                }
            }

            PlanNode result;
            if (!distributedChildren.isEmpty() && singleNodeChildren.isEmpty()) {
                // parent does not have preference or prefers some partitioning without any explicit partitioning - just use
                // children partitioning and don't GATHER partitioned inputs
                // TODO: add FIXED_ARBITRARY_DISTRIBUTION support on non empty singleNodeChildren
                if (!parentPartitioningPreference.isPresent() || parentPartitioningPreference.get().isDistributed()) {
                    return arbitraryDistributeUnion(node, distributedChildren, distributedOutputLayouts);
                }

                // add a gathering exchange above partitioned inputs
                result = new ExchangeNode(
                        idAllocator.getNextId(),
                        GATHER,
                        REMOTE_STREAMING,
                        new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), node.getOutputVariables()),
                        distributedChildren,
                        distributedOutputLayouts,
                        Optional.empty());
            }
            else if (!singleNodeChildren.isEmpty()) {
                if (!distributedChildren.isEmpty()) {
                    // add a gathering exchange above partitioned inputs and fold it into the set of unpartitioned inputs
                    // NOTE: new symbols for ExchangeNode output are required in order to keep plan logically correct with new local union below

                    List<VariableReferenceExpression> exchangeOutputLayout = node.getOutputVariables().stream()
                            .map(variableAllocator::newVariable)
                            .collect(toImmutableList());

                    result = new ExchangeNode(
                            idAllocator.getNextId(),
                            GATHER,
                            REMOTE_STREAMING,
                            new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), exchangeOutputLayout),
                            distributedChildren,
                            distributedOutputLayouts,
                            Optional.empty());

                    singleNodeChildren.add(result);
                    // TODO use result.getOutputVariable() after symbol to variable refactoring is done. This is a temporary hack since we know the value should be exchangeOutputLayout.
                    singleNodeOutputLayouts.add(exchangeOutputLayout);
                }

                ImmutableListMultimap.Builder<VariableReferenceExpression, VariableReferenceExpression> mappings = ImmutableListMultimap.builder();
                for (int i = 0; i < node.getOutputVariables().size(); i++) {
                    for (List<VariableReferenceExpression> outputLayout : singleNodeOutputLayouts) {
                        mappings.put(node.getOutputVariables().get(i), outputLayout.get(i));
                    }
                }

                // add local union for all unpartitioned inputs
                result = new UnionNode(node.getId(), singleNodeChildren, mappings.build());
            }
            else {
                throw new IllegalStateException("both singleNodeChildren distributedChildren are empty");
            }

            return new PlanWithProperties(
                    result,
                    ActualProperties.builder()
                            .global(singleStreamPartition())
                            .build());
        }

        private PlanWithProperties arbitraryDistributeUnion(
                UnionNode node,
                List<PlanNode> distributedChildren,
                List<List<VariableReferenceExpression>> distributedOutputLayouts)
        {
            // TODO: can we insert LOCAL exchange for one child SOURCE distributed and another HASH distributed?
            if (getNumberOfTableScans(distributedChildren) == 0 && isSameOrSystemCompatiblePartitions(extractRemoteExchangePartitioningHandles(distributedChildren))) {
                // No source distributed child, we can use insert LOCAL exchange
                // TODO: if all children have the same partitioning, pass this partitioning to the parent
                // instead of "arbitraryPartition".
                return new PlanWithProperties(node.replaceChildren(distributedChildren));
            }
            else {
                // Presto currently can not execute stage that has multiple table scans, so in that case
                // we have to insert REMOTE exchange with FIXED_ARBITRARY_DISTRIBUTION instead of local exchange
                return new PlanWithProperties(
                        new ExchangeNode(
                                idAllocator.getNextId(),
                                REPARTITION,
                                REMOTE_STREAMING,
                                new PartitioningScheme(Partitioning.create(FIXED_ARBITRARY_DISTRIBUTION, ImmutableList.of()), node.getOutputVariables()),
                                distributedChildren,
                                distributedOutputLayouts,
                                Optional.empty()));
            }
        }

        @Override
        public PlanWithProperties visitApply(ApplyNode node, PreferredProperties preferredProperties)
        {
            throw new IllegalStateException("Unexpected node: " + node.getClass().getName());
        }

        @Override
        public PlanWithProperties visitLateralJoin(LateralJoinNode node, PreferredProperties preferredProperties)
        {
            throw new IllegalStateException("Unexpected node: " + node.getClass().getName());
        }

        private PlanWithProperties planChild(PlanNode node, PreferredProperties preferredProperties)
        {
            return getOnlyElement(node.getSources()).accept(this, preferredProperties);
        }

        private PlanWithProperties rebaseAndDeriveProperties(PlanNode node, PlanWithProperties child)
        {
            return withDerivedProperties(
                    ChildReplacer.replaceChildren(node, ImmutableList.of(child.getNode())),
                    child.getProperties());
        }

        private PlanWithProperties rebaseAndDeriveProperties(PlanNode node, List<PlanWithProperties> children)
        {
            PlanNode result = node.replaceChildren(
                    children.stream()
                            .map(PlanWithProperties::getNode)
                            .collect(toList()));
            return new PlanWithProperties(result, deriveProperties(result, children.stream().map(PlanWithProperties::getProperties).collect(toList())));
        }

        private PlanWithProperties withDerivedProperties(PlanNode node, ActualProperties inputProperties)
        {
            return new PlanWithProperties(node, deriveProperties(node, inputProperties));
        }

        private ActualProperties deriveProperties(PlanNode result, ActualProperties inputProperties)
        {
            return deriveProperties(result, ImmutableList.of(inputProperties));
        }

        private ActualProperties deriveProperties(PlanNode result, List<ActualProperties> inputProperties)
        {
            // TODO: move this logic to PlanSanityChecker once PropertyDerivations.deriveProperties fully supports local exchanges
            ActualProperties outputProperties = PropertyDerivations.deriveProperties(result, inputProperties, metadata, session, types, parser);
            verify(result instanceof SemiJoinNode || inputProperties.stream().noneMatch(ActualProperties::isNullsAndAnyReplicated) || outputProperties.isNullsAndAnyReplicated(),
                    "SemiJoinNode is the only node that can strip null replication");
            return outputProperties;
        }

        private ActualProperties derivePropertiesRecursively(PlanNode result)
        {
            return PropertyDerivations.derivePropertiesRecursively(result, metadata, session, types, parser);
        }

        private Partitioning createPartitioning(List<VariableReferenceExpression> partitioningColumns)
        {
            // TODO: Use SystemTablesMetadata instead of introducing a special case
            if (GlobalSystemConnector.NAME.equals(partitioningProviderCatalog)) {
                return Partitioning.create(FIXED_HASH_DISTRIBUTION, ImmutableList.copyOf(partitioningColumns));
            }

            List<Type> partitioningTypes = partitioningColumns.stream()
                    .map(VariableReferenceExpression::getType)
                    .collect(toImmutableList());
            try {
                PartitioningHandle partitioningHandle = metadata.getPartitioningHandleForExchange(session, partitioningProviderCatalog, hashPartitionCount, partitioningTypes);
                return Partitioning.create(partitioningHandle, partitioningColumns);
            }
            catch (PrestoException e) {
                if (e.getErrorCode().equals(NOT_SUPPORTED.toErrorCode())) {
                    throw new PrestoException(
                            NOT_SUPPORTED,
                            format("Catalog \"%s\" does not support custom partitioning and cannot be used as a partitioning provider", partitioningProviderCatalog),
                            e);
                }
                throw e;
            }
        }

        // TODO: refactor this method into ExchangeNode#partitionedExchange once
        //   materialized exchange is supported for all nodes.
        private Scope selectExchangeScopeForPartitionedRemoteExchange(PlanNode exchangeSource, boolean nullsAndAnyReplicated)
        {
            if (nullsAndAnyReplicated || exchangeSource.getOutputVariables().isEmpty()) {
                // materialized remote exchange is not supported when
                //  * replicateNullsAndAny is needed
                //  * materializing 0 columns input is not supported
                return REMOTE_STREAMING;
            }

            switch (exchangeMaterializationStrategy) {
                case ALL:
                    return REMOTE_MATERIALIZED;
                case NONE:
                    return REMOTE_STREAMING;
                default:
                    throw new IllegalStateException("Unexpected exchange materialization strategy: " + exchangeMaterializationStrategy);
            }
        }
    }

    private boolean canPushdownPartialMerge(PlanNode node, PartialMergePushdownStrategy strategy)
    {
        switch (strategy) {
            case NONE:
                return false;
            case PUSH_THROUGH_LOW_MEMORY_OPERATORS:
                return canPushdownPartialMergeThroughLowMemoryOperators(node);
            default:
                throw new UnsupportedOperationException("Unsupported PartialMergePushdownStrategy: " + strategy);
        }
    }

    private boolean canPushdownPartialMergeThroughLowMemoryOperators(PlanNode node)
    {
        if (node instanceof TableScanNode) {
            return true;
        }
        if (node instanceof ExchangeNode && ((ExchangeNode) node).getScope() == REMOTE_MATERIALIZED) {
            return true;
        }

        // Don't pushdown partial merge through join and aggregations
        // since execution might requires more distributed memory.
        if (node instanceof JoinNode ||
                node instanceof AggregationNode ||
                node instanceof SemiJoinNode) {
            return false;
        }

        return node.getSources().stream()
                .allMatch(this::canPushdownPartialMergeThroughLowMemoryOperators);
    }

    public static Map<VariableReferenceExpression, VariableReferenceExpression> computeIdentityTranslations(Assignments assignments, TypeProvider types)
    {
        Map<VariableReferenceExpression, VariableReferenceExpression> outputToInput = new HashMap<>();
        for (Map.Entry<VariableReferenceExpression, RowExpression> assignment : assignments.getMap().entrySet()) {
            if (castToExpression(assignment.getValue()) instanceof SymbolReference) {
                outputToInput.put(assignment.getKey(), toVariableReference(castToExpression(assignment.getValue()), types));
            }
        }
        return outputToInput;
    }

    @VisibleForTesting
    static Comparator<ActualProperties> streamingExecutionPreference(PreferredProperties preferred)
    {
        // Calculating the matches can be a bit expensive, so cache the results between comparisons
        LoadingCache<List<LocalProperty<VariableReferenceExpression>>, List<Optional<LocalProperty<VariableReferenceExpression>>>> matchCache = CacheBuilder.newBuilder()
                .build(CacheLoader.from(actualProperties -> LocalProperties.match(actualProperties, preferred.getLocalProperties())));

        return (actual1, actual2) -> {
            List<Optional<LocalProperty<VariableReferenceExpression>>> matchLayout1 = matchCache.getUnchecked(actual1.getLocalProperties());
            List<Optional<LocalProperty<VariableReferenceExpression>>> matchLayout2 = matchCache.getUnchecked(actual2.getLocalProperties());

            return ComparisonChain.start()
                    .compareTrueFirst(hasLocalOptimization(preferred.getLocalProperties(), matchLayout1), hasLocalOptimization(preferred.getLocalProperties(), matchLayout2))
                    .compareTrueFirst(meetsPartitioningRequirements(preferred, actual1), meetsPartitioningRequirements(preferred, actual2))
                    .compare(matchLayout1, matchLayout2, matchedLayoutPreference())
                    .result();
        };
    }

    private static <T> boolean hasLocalOptimization(List<LocalProperty<T>> desiredLayout, List<Optional<LocalProperty<T>>> matchResult)
    {
        checkArgument(desiredLayout.size() == matchResult.size());
        if (matchResult.isEmpty()) {
            return false;
        }
        // Optimizations can be applied if the first LocalProperty has been modified in the match in any way
        return !matchResult.get(0).equals(Optional.of(desiredLayout.get(0)));
    }

    private static boolean meetsPartitioningRequirements(PreferredProperties preferred, ActualProperties actual)
    {
        if (!preferred.getGlobalProperties().isPresent()) {
            return true;
        }
        PreferredProperties.Global preferredGlobal = preferred.getGlobalProperties().get();
        if (!preferredGlobal.isDistributed()) {
            return actual.isSingleNode();
        }
        if (!preferredGlobal.getPartitioningProperties().isPresent()) {
            return !actual.isSingleNode();
        }
        return actual.isStreamPartitionedOn(preferredGlobal.getPartitioningProperties().get().getPartitioningColumns());
    }

    // Prefer the match result that satisfied the most requirements
    private static <T> Comparator<List<Optional<LocalProperty<T>>>> matchedLayoutPreference()
    {
        return (matchLayout1, matchLayout2) -> {
            Iterator<Optional<LocalProperty<T>>> match1Iterator = matchLayout1.iterator();
            Iterator<Optional<LocalProperty<T>>> match2Iterator = matchLayout2.iterator();
            while (match1Iterator.hasNext() && match2Iterator.hasNext()) {
                Optional<LocalProperty<T>> match1 = match1Iterator.next();
                Optional<LocalProperty<T>> match2 = match2Iterator.next();
                if (match1.isPresent() && match2.isPresent()) {
                    return Integer.compare(match1.get().getColumns().size(), match2.get().getColumns().size());
                }
                else if (match1.isPresent()) {
                    return 1;
                }
                else if (match2.isPresent()) {
                    return -1;
                }
            }
            checkState(!match1Iterator.hasNext() && !match2Iterator.hasNext()); // Should be the same size
            return 0;
        };
    }

    @VisibleForTesting
    static class PlanWithProperties
    {
        private final PlanNode node;
        private final ActualProperties properties;

        public PlanWithProperties(PlanNode node)
        {
            this(node, ActualProperties.builder().build());
        }

        public PlanWithProperties(PlanNode node, ActualProperties properties)
        {
            this.node = node;
            this.properties = properties;
        }

        public PlanNode getNode()
        {
            return node;
        }

        public ActualProperties getProperties()
        {
            return properties;
        }
    }

    private static boolean isSameOrSystemCompatiblePartitions(List<PartitioningHandle> partitioningHandles)
    {
        for (int i = 0; i < partitioningHandles.size() - 1; i++) {
            PartitioningHandle first = partitioningHandles.get(i);
            PartitioningHandle second = partitioningHandles.get(i + 1);
            if (!first.equals(second) && !isCompatibleSystemPartitioning(first, second)) {
                return false;
            }
        }
        return true;
    }

    private static List<PartitioningHandle> extractRemoteExchangePartitioningHandles(List<PlanNode> nodes)
    {
        ImmutableList.Builder<PartitioningHandle> handles = ImmutableList.builder();
        nodes.forEach(node -> node.accept(new ExchangePartitioningHandleExtractor(), handles));
        return handles.build();
    }

    private static class ExchangePartitioningHandleExtractor
            extends InternalPlanVisitor<Void, ImmutableList.Builder<PartitioningHandle>>
    {
        @Override
        public Void visitExchange(ExchangeNode node, ImmutableList.Builder<PartitioningHandle> handles)
        {
            checkArgument(node.getScope().isRemote(), "scope is expected to be remote: %s", node.getScope());
            handles.add(node.getPartitioningScheme().getPartitioning().getHandle());
            return null;
        }

        @Override
        public Void visitPlan(PlanNode node, ImmutableList.Builder<PartitioningHandle> handles)
        {
            for (PlanNode source : node.getSources()) {
                source.accept(this, handles);
            }
            return null;
        }
    }
}
