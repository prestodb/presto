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
import com.facebook.presto.spi.ConstantProperty;
import com.facebook.presto.spi.GroupingProperty;
import com.facebook.presto.spi.LocalProperty;
import com.facebook.presto.spi.SortingProperty;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.DeleteNode;
import com.facebook.presto.spi.plan.DistinctLimitNode;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.InputDistribution;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.MarkDistinctNode;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.OutputNode;
import com.facebook.presto.spi.plan.Partitioning;
import com.facebook.presto.spi.plan.PartitioningScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.SemiJoinNode;
import com.facebook.presto.spi.plan.SortNode;
import com.facebook.presto.spi.plan.SpatialJoinNode;
import com.facebook.presto.spi.plan.StatisticAggregations;
import com.facebook.presto.spi.plan.TableFinishNode;
import com.facebook.presto.spi.plan.TableWriterNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.plan.UnionNode;
import com.facebook.presto.spi.plan.WindowNode;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.SystemPartitioningHandle;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.optimizations.StreamPropertyDerivations.StreamProperties;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.ExplainAnalyzeNode;
import com.facebook.presto.sql.planner.plan.IndexJoinNode;
import com.facebook.presto.sql.planner.plan.InternalPlanVisitor;
import com.facebook.presto.sql.planner.plan.LateralJoinNode;
import com.facebook.presto.sql.planner.plan.MergeWriterNode;
import com.facebook.presto.sql.planner.plan.RowNumberNode;
import com.facebook.presto.sql.planner.plan.StatisticsWriterNode;
import com.facebook.presto.sql.planner.plan.TableWriterMergeNode;
import com.facebook.presto.sql.planner.plan.TopNRowNumberNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.getTaskConcurrency;
import static com.facebook.presto.SystemSessionProperties.getTaskPartitionedWriterCount;
import static com.facebook.presto.SystemSessionProperties.getTaskWriterCount;
import static com.facebook.presto.SystemSessionProperties.isDistributedSortEnabled;
import static com.facebook.presto.SystemSessionProperties.isEnforceFixedDistributionForOutputOperator;
import static com.facebook.presto.SystemSessionProperties.isJoinSpillingEnabled;
import static com.facebook.presto.SystemSessionProperties.isNativeExecutionScaleWritersThreadsEnabled;
import static com.facebook.presto.SystemSessionProperties.isNativeJoinBuildPartitionEnforced;
import static com.facebook.presto.SystemSessionProperties.isQuickDistinctLimitEnabled;
import static com.facebook.presto.SystemSessionProperties.isSegmentedAggregationEnabled;
import static com.facebook.presto.SystemSessionProperties.isSpillEnabled;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.operator.aggregation.AggregationUtils.hasSingleNodeExecutionPreference;
import static com.facebook.presto.operator.aggregation.AggregationUtils.isDecomposable;
import static com.facebook.presto.sql.TemporaryTableUtil.splitIntoPartialAndIntermediate;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.optimizations.StreamPreferredProperties.any;
import static com.facebook.presto.sql.planner.optimizations.StreamPreferredProperties.defaultParallelism;
import static com.facebook.presto.sql.planner.optimizations.StreamPreferredProperties.exactlyPartitionedOn;
import static com.facebook.presto.sql.planner.optimizations.StreamPreferredProperties.fixedParallelism;
import static com.facebook.presto.sql.planner.optimizations.StreamPreferredProperties.partitionedOn;
import static com.facebook.presto.sql.planner.optimizations.StreamPreferredProperties.singleStream;
import static com.facebook.presto.sql.planner.optimizations.StreamPropertyDerivations.StreamProperties.StreamDistribution.SINGLE;
import static com.facebook.presto.sql.planner.optimizations.StreamPropertyDerivations.derivePropertiesRecursively;
import static com.facebook.presto.sql.planner.plan.ChildReplacer.replaceChildren;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.GATHER;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.gatheringExchange;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.mergingExchange;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.partitionedExchange;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.roundRobinExchange;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.systemPartitionedExchange;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class AddLocalExchanges
        implements PlanOptimizer
{
    private final Metadata metadata;
    private final boolean nativeExecution;

    public AddLocalExchanges(Metadata metadata, boolean nativeExecution)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.nativeExecution = nativeExecution;
    }

    @Override
    public PlanOptimizerResult optimize(PlanNode plan, Session session, TypeProvider types, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        PlanWithProperties result = new Rewriter(variableAllocator, idAllocator, session, nativeExecution).accept(plan, any());
        boolean optimizerTriggered = PlanNodeSearcher.searchFrom(result.getNode()).where(node -> node instanceof ExchangeNode && ((ExchangeNode) node).getScope().isLocal()).findFirst().isPresent();
        return PlanOptimizerResult.optimizerResult(result.getNode(), optimizerTriggered);
    }

    private class Rewriter
            extends InternalPlanVisitor<PlanWithProperties, StreamPreferredProperties>
    {
        private final VariableAllocator variableAllocator;
        private final PlanNodeIdAllocator idAllocator;
        private final Session session;
        private final TypeProvider types;
        private final boolean nativeExecution;

        public Rewriter(VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, Session session, boolean nativeExecution)
        {
            this.variableAllocator = variableAllocator;
            this.types = TypeProvider.viewOf(variableAllocator.getVariables());
            this.idAllocator = idAllocator;
            this.session = session;
            this.nativeExecution = nativeExecution;
        }

        @Override
        public PlanWithProperties visitPlan(PlanNode node, StreamPreferredProperties parentPreferences)
        {
            return planAndEnforceChildren(
                    node,
                    parentPreferences.withoutPreference().withDefaultParallelism(session),
                    parentPreferences.withDefaultParallelism(session));
        }

        @Override
        public PlanWithProperties visitApply(ApplyNode node, StreamPreferredProperties parentPreferences)
        {
            throw new IllegalStateException("Unexpected node: " + node.getClass().getName());
        }

        @Override
        public PlanWithProperties visitLateralJoin(LateralJoinNode node, StreamPreferredProperties parentPreferences)
        {
            throw new IllegalStateException("Unexpected node: " + node.getClass().getName());
        }

        @Override
        public PlanWithProperties visitOutput(OutputNode node, StreamPreferredProperties parentPreferences)
        {
            return planAndEnforceChildren(
                    node,
                    any().withOrderSensitivity(),
                    any().withOrderSensitivity());
        }

        @Override
        public PlanWithProperties visitExplainAnalyze(ExplainAnalyzeNode node, StreamPreferredProperties parentPreferences)
        {
            // Although explain analyze discards all output, we want to maintain the behavior
            // of a normal output node, so declare the node to be order sensitive
            return planAndEnforceChildren(
                    node,
                    singleStream().withOrderSensitivity(),
                    singleStream().withOrderSensitivity());
        }

        //
        // Nodes that always require a single stream
        //

        @Override
        public PlanWithProperties visitSort(SortNode node, StreamPreferredProperties parentPreferences)
        {
            if (!node.getPartitionBy().isEmpty()) {
                return planSortWithPartition(node, parentPreferences);
            }
            return planSortWithoutPartition(node, parentPreferences);
        }

        private PlanWithProperties planSortWithPartition(SortNode node, StreamPreferredProperties parentPreferences)
        {
            checkArgument(!node.getPartitionBy().isEmpty());
            StreamPreferredProperties childRequirements = parentPreferences
                    .constrainTo(node.getSource().getOutputVariables())
                    .withDefaultParallelism(session)
                    .withPartitioning(node.getPartitionBy());

            PlanWithProperties child = planAndEnforce(node.getSource(), childRequirements, childRequirements);

            SortNode result = new SortNode(node.getSourceLocation(), idAllocator.getNextId(), child.getNode(), node.getOrderingScheme(), node.isPartial(), node.getPartitionBy());

            return deriveProperties(result, child.getProperties());
        }

        private PlanWithProperties planSortWithoutPartition(SortNode node, StreamPreferredProperties parentPreferences)
        {
            checkArgument(node.getPartitionBy().isEmpty());
            // Remove sort if the child is already sorted and in a single stream
            // TODO: extract to its own optimization after AddLocalExchanges once the
            // constraint optimization framework is in a better state to be extended
            PlanWithProperties childPlan = planAndEnforce(node.getSource(), any(), singleStream());
            if (childPlan.getProperties().isSingleStream() && childPlan.getProperties().isOrdered()) {
                OrderingScheme orderingScheme = node.getOrderingScheme();
                List<LocalProperty<VariableReferenceExpression>> desiredProperties = orderingScheme.getOrderByVariables().stream()
                        .map(variable -> new SortingProperty<>(variable, orderingScheme.getOrdering(variable)))
                        .collect(toImmutableList());
                if (LocalProperties.match(childPlan.getProperties().getLocalProperties(), desiredProperties).stream().noneMatch(Optional::isPresent)) {
                    return childPlan;
                }
            }

            if (isDistributedSortEnabled(session)) {
                PlanWithProperties sortPlan = planAndEnforceChildren(node, fixedParallelism(), fixedParallelism());

                if (!sortPlan.getProperties().isSingleStream()) {
                    SortNode sortNode = (SortNode) sortPlan.getNode();
                    return deriveProperties(
                            mergingExchange(
                                    idAllocator.getNextId(),
                                    LOCAL,
                                    new SortNode(
                                            sortNode.getSourceLocation(),
                                            sortNode.getId(),
                                            getOnlyElement(sortNode.getSources()),
                                            sortNode.getOrderingScheme(),
                                            true,
                                            sortNode.getPartitionBy()),
                                    node.getOrderingScheme()),
                            sortPlan.getProperties());
                }

                return sortPlan;
            }
            // sort requires that all data be in one stream
            // this node changes the input organization completely, so we do not pass through parent preferences
            return planAndEnforceChildren(node, singleStream(), defaultParallelism(session));
        }

        @Override
        public PlanWithProperties visitStatisticsWriterNode(StatisticsWriterNode node, StreamPreferredProperties context)
        {
            // analyze finish requires that all data be in one stream
            // this node changes the input organization completely, so we do not pass through parent preferences
            return planAndEnforceChildren(node, singleStream(), defaultParallelism(session));
        }

        @Override
        public PlanWithProperties visitTableFinish(TableFinishNode node, StreamPreferredProperties parentPreferences)
        {
            // table commit requires that all data be in one stream
            // this node changes the input organization completely, so we do not pass through parent preferences
            return planAndEnforceChildren(node, singleStream(), defaultParallelism(session));
        }

        @Override
        public PlanWithProperties visitTopN(TopNNode node, StreamPreferredProperties parentPreferences)
        {
            if (node.getStep().equals(TopNNode.Step.PARTIAL)) {
                return planAndEnforceChildren(
                        node,
                        parentPreferences.withoutPreference().withDefaultParallelism(session),
                        parentPreferences.withDefaultParallelism(session));
            }

            // final topN requires that all data be in one stream
            // also, a final changes the input organization completely, so we do not pass through parent preferences
            return planAndEnforceChildren(
                    node,
                    singleStream(),
                    defaultParallelism(session));
        }

        @Override
        public PlanWithProperties visitLimit(LimitNode node, StreamPreferredProperties parentPreferences)
        {
            if (node.isPartial()) {
                return planAndEnforceChildren(
                        node,
                        parentPreferences.withoutPreference().withDefaultParallelism(session),
                        parentPreferences.withDefaultParallelism(session));
            }

            // final limit requires that all data be in one stream
            return planAndEnforceChildren(
                    node,
                    singleStream(),
                    parentPreferences.withDefaultParallelism(session));
        }

        @Override
        public PlanWithProperties visitDistinctLimit(DistinctLimitNode node, StreamPreferredProperties parentPreferences)
        {
            // final limit requires that all data be in one stream
            StreamPreferredProperties requiredProperties;
            StreamPreferredProperties preferredProperties;
            if (node.isPartial()) {
                if (isQuickDistinctLimitEnabled(session)) {
                    PlanWithProperties source = accept(node.getSource(), defaultParallelism(session));
                    PlanWithProperties exchange = deriveProperties(
                            roundRobinExchange(idAllocator.getNextId(), LOCAL, source.getNode()),
                            source.getProperties());
                    return rebaseAndDeriveProperties(node, ImmutableList.of(exchange));
                }
                else {
                    requiredProperties = parentPreferences.withoutPreference().withDefaultParallelism(session);
                    preferredProperties = parentPreferences.withDefaultParallelism(session);
                }
            }
            else {
                // a final changes the input organization completely, so we do not pass through parent preferences
                requiredProperties = singleStream();
                preferredProperties = defaultParallelism(session);
            }

            return planAndEnforceChildren(node, requiredProperties, preferredProperties);
        }

        @Override
        public PlanWithProperties visitEnforceSingleRow(EnforceSingleRowNode node, StreamPreferredProperties parentPreferences)
        {
            return planAndEnforceChildren(node, singleStream(), defaultParallelism(session));
        }

        //
        // Nodes that require parallel streams to be partitioned
        //

        @Override
        public PlanWithProperties visitAggregation(AggregationNode node, StreamPreferredProperties parentPreferences)
        {
            checkState(node.getStep() == AggregationNode.Step.SINGLE, "step of aggregation is expected to be SINGLE, but it is %s", node.getStep());

            if (hasSingleNodeExecutionPreference(node, metadata.getFunctionAndTypeManager())) {
                return planAndEnforceChildren(node, singleStream(), defaultParallelism(session));
            }

            List<VariableReferenceExpression> groupingKeys = node.getGroupingKeys();
            if (node.hasDefaultOutput()) {
                checkState(isDecomposable(node, metadata.getFunctionAndTypeManager()));

                // Put fixed local exchange directly below final aggregation to ensure that final and partial aggregations are separated by exchange (in a local runner mode)
                // This is required so that default outputs from multiple instances of partial aggregations are passed to a single final aggregation.
                PlanWithProperties child = planAndEnforce(node.getSource(), any(), defaultParallelism(session));
                PlanWithProperties exchange = deriveProperties(
                        systemPartitionedExchange(
                                idAllocator.getNextId(),
                                LOCAL,
                                child.getNode(),
                                groupingKeys,
                                Optional.empty()),
                        child.getProperties());
                return rebaseAndDeriveProperties(node, ImmutableList.of(exchange));
            }

            StreamPreferredProperties childRequirements = parentPreferences
                    .constrainTo(node.getSource().getOutputVariables())
                    .withDefaultParallelism(session)
                    .withPartitioning(groupingKeys);

            PlanWithProperties child = planAndEnforce(node.getSource(), childRequirements, childRequirements);

            List<VariableReferenceExpression> preGroupedSymbols = ImmutableList.of();
            // Logic in LocalProperties.match(localProperties, groupingKeys)
            // 1. Extract the longest prefix of localProperties to a set that is a subset of groupingKeys
            // 2. Iterate grouped-by keys and add the elements that's not in the set to the result
            // Result would be a List of one element: Optional<GroupingProperty>, GroupingProperty would contain one/multiple elements from step 2
            // Eg:
            // [A, B] [(B, A)]     ->   List.of(Optional.empty())
            // [A, B] [B]          ->   List.of(Optional.of(GroupingProperty(B)))
            // [A, B] [A]          ->   List.of(Optional.empty())
            // [A, B] [(A, C)]     ->   List.of(Optional.of(GroupingProperty(C)))
            // [A, B] [(D, A, C)]  ->   List.of(Optional.of(GroupingProperty(D, C)))
            List<Optional<LocalProperty<VariableReferenceExpression>>> matchResult = LocalProperties.match(child.getProperties().getLocalProperties(), LocalProperties.grouped(groupingKeys));
            if (!matchResult.get(0).isPresent()) {
                // !isPresent() indicates the property was satisfied completely
                preGroupedSymbols = groupingKeys;
            }
            else if (matchResult.get(0).get().getColumns().size() < groupingKeys.size() && isSegmentedAggregationEnabled(session)) {
                // If the result size = original groupingKeys size: all grouping keys are not pre-grouped, can't enable segmented aggregation
                // Otherwise: partial grouping keys are pre-grouped, can enable segmented aggregation, the result represents the grouping keys that's not pre-grouped
                preGroupedSymbols = groupingKeys.stream().filter(groupingKey -> !matchResult.get(0).get().getColumns().contains(groupingKey)).collect(toImmutableList());
            }

            AggregationNode result = new AggregationNode(
                    node.getSourceLocation(),
                    node.getId(),
                    child.getNode(),
                    node.getAggregations(),
                    node.getGroupingSets(),
                    preGroupedSymbols,
                    node.getStep(),
                    node.getHashVariable(),
                    node.getGroupIdVariable(),
                    node.getAggregationId());

            return deriveProperties(result, child.getProperties());
        }

        @Override
        public PlanWithProperties visitWindow(WindowNode node, StreamPreferredProperties parentPreferences)
        {
            StreamPreferredProperties childRequirements = parentPreferences
                    .constrainTo(node.getSource().getOutputVariables())
                    .withDefaultParallelism(session)
                    .withPartitioning(node.getPartitionBy());

            PlanWithProperties child = planAndEnforce(node.getSource(), childRequirements, childRequirements);

            List<LocalProperty<VariableReferenceExpression>> desiredProperties = new ArrayList<>();
            if (!node.getPartitionBy().isEmpty()) {
                desiredProperties.add(new GroupingProperty<>(node.getPartitionBy()));
            }
            node.getOrderingScheme().ifPresent(orderingScheme ->
                    orderingScheme.getOrderByVariables().stream()
                            .map(variable -> new SortingProperty<>(variable, orderingScheme.getOrdering(variable)))
                            .forEach(desiredProperties::add));
            Iterator<Optional<LocalProperty<VariableReferenceExpression>>> matchIterator = LocalProperties.match(child.getProperties().getLocalProperties(), desiredProperties).iterator();

            Set<VariableReferenceExpression> prePartitionedInputs = ImmutableSet.of();
            if (!node.getPartitionBy().isEmpty()) {
                Optional<LocalProperty<VariableReferenceExpression>> groupingRequirement = matchIterator.next();
                Set<VariableReferenceExpression> unPartitionedInputs = groupingRequirement.map(LocalProperty::getColumns).orElse(ImmutableSet.of());
                prePartitionedInputs = node.getPartitionBy().stream()
                        .filter(variable -> !unPartitionedInputs.contains(variable))
                        .collect(toImmutableSet());
            }

            int preSortedOrderPrefix = 0;
            if (prePartitionedInputs.equals(ImmutableSet.copyOf(node.getPartitionBy()))) {
                while (matchIterator.hasNext() && !matchIterator.next().isPresent()) {
                    preSortedOrderPrefix++;
                }
            }

            WindowNode result = new WindowNode(
                    node.getSourceLocation(),
                    node.getId(),
                    child.getNode(),
                    node.getSpecification(),
                    node.getWindowFunctions(),
                    node.getHashVariable(),
                    prePartitionedInputs,
                    preSortedOrderPrefix);

            return deriveProperties(result, child.getProperties());
        }

        @Override
        public PlanWithProperties visitDelete(DeleteNode node, StreamPreferredProperties parentPreferences)
        {
            if (!node.getInputDistribution().isPresent()) {
                return visitPlan(node, parentPreferences);
            }
            InputDistribution inputDistribution = node.getInputDistribution().get();
            StreamPreferredProperties childRequirements = parentPreferences
                    .constrainTo(node.getSource().getOutputVariables())
                    .withDefaultParallelism(session)
                    .withPartitioning(inputDistribution.getPartitionBy());

            PlanWithProperties child = planAndEnforce(node.getSource(), childRequirements, childRequirements);
            DeleteNode result = new DeleteNode(
                    node.getSourceLocation(),
                    idAllocator.getNextId(),
                    node.getStatsEquivalentPlanNode(),
                    child.getNode(),
                    node.getRowId(),
                    node.getOutputVariables(),
                    node.getInputDistribution());

            return deriveProperties(result, child.getProperties());
        }

        @Override
        public PlanWithProperties visitMarkDistinct(MarkDistinctNode node, StreamPreferredProperties parentPreferences)
        {
            // mark distinct requires that all data partitioned
            StreamPreferredProperties childRequirements = parentPreferences
                    .constrainTo(node.getSource().getOutputVariables())
                    .withDefaultParallelism(session)
                    .withPartitioning(node.getDistinctVariables());

            PlanWithProperties child = planAndEnforce(node.getSource(), childRequirements, childRequirements);

            MarkDistinctNode result = new MarkDistinctNode(
                    node.getSourceLocation(),
                    node.getId(),
                    child.getNode(),
                    node.getMarkerVariable(),
                    pruneMarkDistinctVariables(node, child.getProperties().getLocalProperties()),
                    node.getHashVariable());

            return deriveProperties(result, child.getProperties());
        }

        /**
         * Prune redundant distinct symbols to reduce CPU cost of hashing corresponding values and amount of memory
         * needed to store all the distinct values.
         * <p>
         * Consider the following plan,
         * <pre>
         *  - MarkDistinctNode (unique, c1, c2)
         *      - Join
         *          - AssignUniqueId (unique)
         *              - probe (c1, c2)
         *          - build
         * </pre>
         * In this case MarkDistinctNode (unique, c1, c2) is equivalent to MarkDistinctNode (unique),
         * because if two rows match on `unique`, they must match on `c1` and `c2` as well.
         * <p>
         * More generally, any distinct symbol that is functionally dependent on a subset of
         * other distinct symbols can be dropped.
         * <p>
         * Ideally, this logic would be encapsulated in a separate rule, but currently no rule other
         * than AddLocalExchanges can reason about local properties.
         */
        private List<VariableReferenceExpression> pruneMarkDistinctVariables(MarkDistinctNode node, List<LocalProperty<VariableReferenceExpression>> localProperties)
        {
            if (localProperties.isEmpty()) {
                return node.getDistinctVariables();
            }

            // Identify functional dependencies between distinct symbols: in the list of local properties any constant
            // symbol is functionally dependent on the set of symbols that appears earlier.
            ImmutableSet.Builder<VariableReferenceExpression> redundantVariablesBuilder = ImmutableSet.builder();
            for (LocalProperty<VariableReferenceExpression> property : localProperties) {
                if (property instanceof ConstantProperty) {
                    redundantVariablesBuilder.add(((ConstantProperty<VariableReferenceExpression>) property).getColumn());
                }
                else if (!node.getDistinctVariables().containsAll(property.getColumns())) {
                    // Ran into a non-distinct symbol. There will be no more symbols that are functionally dependent on distinct symbols exclusively.
                    break;
                }
            }

            Set<VariableReferenceExpression> redundantVariables = redundantVariablesBuilder.build();
            List<VariableReferenceExpression> remainingSymbols = node.getDistinctVariables().stream()
                    .filter(variable -> !redundantVariables.contains(variable))
                    .collect(toImmutableList());
            if (remainingSymbols.isEmpty()) {
                // This happens when all distinct symbols are constants.
                // In that case, keep the first symbol (don't drop them all).
                return ImmutableList.of(node.getDistinctVariables().get(0));
            }
            return remainingSymbols;
        }

        @Override
        public PlanWithProperties visitRowNumber(RowNumberNode node, StreamPreferredProperties parentPreferences)
        {
            StreamPreferredProperties requiredProperties = parentPreferences.withDefaultParallelism(session);
            // final row number requires that all data be partitioned
            if (!node.isPartial()) {
                requiredProperties = requiredProperties.withPartitioning(node.getPartitionBy());
            }
            return planAndEnforceChildren(node, requiredProperties, requiredProperties);
        }

        @Override
        public PlanWithProperties visitTopNRowNumber(TopNRowNumberNode node, StreamPreferredProperties parentPreferences)
        {
            StreamPreferredProperties requiredProperties = parentPreferences.withDefaultParallelism(session);

            // final topN row number requires that all data be partitioned
            if (!node.isPartial()) {
                requiredProperties = requiredProperties.withPartitioning(node.getPartitionBy());
            }

            return planAndEnforceChildren(node, requiredProperties, requiredProperties);
        }

        //
        // Table Writer
        //

        @Override
        public PlanWithProperties visitTableWriter(TableWriterNode tableWrite, StreamPreferredProperties parentPreferences)
        {
            // When table is partitioned and single writer per partition is required (for example a bucketed table in Hive connector)
            if (tableWrite.isSingleWriterPerPartitionRequired()) {
                // special case when a single table writer per task is requested
                if (getTaskPartitionedWriterCount(session) == 1) {
                    return planAndEnforceChildren(tableWrite, singleStream(), defaultParallelism(session));
                }
                PlanWithProperties source = accept(tableWrite.getSource(), defaultParallelism(session));
                PlanWithProperties exchange = deriveProperties(
                        partitionedExchange(
                                idAllocator.getNextId(),
                                LOCAL,
                                source.getNode(),
                                tableWrite.getTablePartitioningScheme().get()),
                        source.getProperties());
                return planTableWriteWithTableWriteMerge(tableWrite, exchange);
            }

            // special case when a single table writer per task is requested
            if (getTaskWriterCount(session) == 1) {
                return planAndEnforceChildren(tableWrite, singleStream(), defaultParallelism(session));
            }

            // Writer thread scaling enabled and the output table allows multiple writers per partition (for example non bucketed table in Hive connector)
            if (nativeExecution && isNativeExecutionScaleWritersThreadsEnabled(session)) {
                PlanWithProperties source = accept(tableWrite.getSource(), defaultParallelism(session));
                PartitioningScheme partitioningScheme;
                if (tableWrite.getTablePartitioningScheme().isPresent()) {
                    // Partitioning scheme is present and more than a single writer per partition is allowed (for example when table is not bucketed but partitioned in Hive connector)
                    partitioningScheme = tableWrite.getTablePartitioningScheme().get();
                    verify(partitioningScheme.isScaleWriters());
                }
                else {
                    // When partitioning scheme is not present (for example when table is not partitioned and not bucketed in Hive connector)
                    partitioningScheme = new PartitioningScheme(
                            Partitioning.create(FIXED_ARBITRARY_DISTRIBUTION, ImmutableList.of()),
                            source.getNode().getOutputVariables(),
                            true);
                }
                PlanWithProperties exchange = deriveProperties(
                        partitionedExchange(
                                idAllocator.getNextId(),
                                LOCAL,
                                source.getNode(),
                                partitioningScheme),
                        source.getProperties());
                return planTableWriteWithTableWriteMerge(tableWrite, exchange);
            }

            // Writer thread scaling is disabled and there is no strict partitioning requirement
            int taskWriterCount = getTaskWriterCount(session);
            int taskConcurrency = getTaskConcurrency(session);
            if (taskWriterCount == taskConcurrency) {
                // When table write concurrency is equal to task concurrency do not add en extra local exchange for improved efficiency
                return planTableWriteWithTableWriteMerge(
                        tableWrite,
                        // When source distribution is MULTIPLE (for example a TableScan) add an exchange to achieve a fixed number of writer threads
                        planAndEnforce(tableWrite.getSource(), fixedParallelism(), fixedParallelism()));
            }
            else {
                // When concurrency settings are different add an exchange to achieve a specific level of parallelism for table write
                PlanWithProperties source = accept(tableWrite.getSource(), defaultParallelism(session));
                PlanWithProperties exchange = deriveProperties(
                        roundRobinExchange(idAllocator.getNextId(), LOCAL, source.getNode()),
                        source.getProperties());
                return planTableWriteWithTableWriteMerge(tableWrite, exchange);
            }
        }

        private PlanWithProperties planTableWriteWithTableWriteMerge(TableWriterNode tableWrite, PlanWithProperties source)
        {
            Optional<StatisticAggregations.Parts> statisticAggregations = tableWrite
                    .getStatisticsAggregation()
                    .map(aggregations -> splitIntoPartialAndIntermediate(
                            aggregations,
                            variableAllocator,
                            metadata.getFunctionAndTypeManager()));

            PlanWithProperties tableWriteWithProperties = deriveProperties(
                    new TableWriterNode(
                            tableWrite.getSourceLocation(),
                            tableWrite.getId(),
                            tableWrite.getStatsEquivalentPlanNode(),
                            source.getNode(),
                            tableWrite.getTarget(),
                            variableAllocator.newVariable("partialrowcount", BIGINT),
                            variableAllocator.newVariable("partialfragments", VARBINARY),
                            variableAllocator.newVariable("partialcontext", VARBINARY),
                            tableWrite.getColumns(),
                            tableWrite.getColumnNames(),
                            tableWrite.getNotNullColumnVariables(),
                            tableWrite.getTablePartitioningScheme(),
                            statisticAggregations.map(StatisticAggregations.Parts::getPartialAggregation),
                            tableWrite.getTaskCountIfScaledWriter(),
                            tableWrite.getIsTemporaryTableWriter()),
                    source.getProperties());

            PlanWithProperties gatherExchangeWithProperties = deriveProperties(
                    gatheringExchange(
                            idAllocator.getNextId(),
                            LOCAL,
                            tableWriteWithProperties.getNode()),
                    tableWriteWithProperties.getProperties());

            return deriveProperties(
                    new TableWriterMergeNode(
                            tableWrite.getSourceLocation(),
                            idAllocator.getNextId(),
                            gatherExchangeWithProperties.getNode(),
                            tableWrite.getRowCountVariable(),
                            tableWrite.getFragmentVariable(),
                            tableWrite.getTableCommitContextVariable(),
                            statisticAggregations.map(StatisticAggregations.Parts::getIntermediateAggregation)),
                    gatherExchangeWithProperties.getProperties());
        }

        private PlanWithProperties visitPartitionedWriter(PlanNode node, Optional<PartitioningScheme> optionalPartitioning, PlanNode source, StreamPreferredProperties parentPreferences)
        {
            if (getTaskWriterCount(session) == 1) {
                return planAndEnforceChildren(node, singleStream(), defaultParallelism(session));
            }

            if (!optionalPartitioning.isPresent()) {
                return planAndEnforceChildren(node, fixedParallelism(), fixedParallelism());
            }

            PartitioningScheme partitioningScheme = optionalPartitioning.get();

            if (partitioningScheme.getPartitioning().getHandle().equals(FIXED_HASH_DISTRIBUTION)) {
                // arbitrary hash function on predefined set of partition columns
                StreamPreferredProperties preference = partitionedOn(partitioningScheme.getPartitioning().getVariableReferences());
                return planAndEnforceChildren(node, preference, preference);
            }

            // connector provided hash function
            verify(!(partitioningScheme.getPartitioning().getHandle().getConnectorHandle() instanceof SystemPartitioningHandle));
            // TODO #20578: Check if the following verification is correct.
            verify(partitioningScheme.getPartitioning().getArguments().stream()
                    .noneMatch(argument -> argument instanceof ConstantExpression),
                    "Table writer partitioning has constant arguments");
            PlanWithProperties newSource = source.accept(this, parentPreferences);
            PlanWithProperties exchange = deriveProperties(
                    partitionedExchange(
                            idAllocator.getNextId(),
                            LOCAL,
                            newSource.getNode(),
                            partitioningScheme),
                    newSource.getProperties());

            return rebaseAndDeriveProperties(node, ImmutableList.of(exchange));
        }

        //
        // Merge
        //

        @Override
        public PlanWithProperties visitMergeWriter(MergeWriterNode node, StreamPreferredProperties parentPreferences)
        {
            return visitPartitionedWriter(node, node.getPartitioningScheme(), node.getSource(), parentPreferences);
        }

        @Override
        public PlanWithProperties visitTableWriteMerge(TableWriterMergeNode node, StreamPreferredProperties context)
        {
            throw new IllegalArgumentException("Unexpected TableWriterMergeNode");
        }

        //
        // Exchanges
        //

        @Override
        public PlanWithProperties visitExchange(ExchangeNode node, StreamPreferredProperties parentPreferences)
        {
            checkArgument(!node.getScope().isLocal(), "AddLocalExchanges can not process a plan containing a local exchange");
            // this node changes the input organization completely, so we do not pass through parent preferences
            if (node.getOrderingScheme().isPresent()) {
                return planAndEnforceChildren(
                        node,
                        any().withOrderSensitivity(),
                        any().withOrderSensitivity());
            }
            return planAndEnforceChildren(
                    node,
                    isEnforceFixedDistributionForOutputOperator(session) ? fixedParallelism() : any(),
                    defaultParallelism(session));
        }

        @Override
        public PlanWithProperties visitUnion(UnionNode node, StreamPreferredProperties preferredProperties)
        {
            // Union is replaced with an exchange which does not retain streaming properties from the children
            List<PlanWithProperties> sourcesWithProperties = node.getSources().stream()
                    .map(source -> accept(source, defaultParallelism(session)))
                    .collect(toImmutableList());

            List<PlanNode> sources = sourcesWithProperties.stream()
                    .map(PlanWithProperties::getNode)
                    .collect(toImmutableList());

            List<StreamProperties> inputProperties = sourcesWithProperties.stream()
                    .map(PlanWithProperties::getProperties)
                    .collect(toImmutableList());

            List<List<VariableReferenceExpression>> inputLayouts = new ArrayList<>(sources.size());
            for (int i = 0; i < sources.size(); i++) {
                inputLayouts.add(node.sourceOutputLayout(i));
            }

            if (preferredProperties.isSingleStreamPreferred()) {
                ExchangeNode exchangeNode = new ExchangeNode(
                        node.getSourceLocation(),
                        idAllocator.getNextId(),
                        GATHER,
                        LOCAL,
                        new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), node.getOutputVariables()),
                        sources,
                        inputLayouts,
                        false,
                        Optional.empty());
                return deriveProperties(exchangeNode, inputProperties);
            }

            Optional<List<VariableReferenceExpression>> preferredPartitionColumns = preferredProperties.getPartitioningColumns();
            if (preferredPartitionColumns.isPresent()) {
                ExchangeNode exchangeNode = new ExchangeNode(
                        node.getSourceLocation(),
                        idAllocator.getNextId(),
                        REPARTITION,
                        LOCAL,
                        new PartitioningScheme(
                                Partitioning.create(FIXED_HASH_DISTRIBUTION, preferredPartitionColumns.get()),
                                node.getOutputVariables()),
                        sources,
                        inputLayouts,
                        false,
                        Optional.empty());
                return deriveProperties(exchangeNode, inputProperties);
            }

            // multiple streams preferred
            ExchangeNode result = new ExchangeNode(
                    node.getSourceLocation(),
                    idAllocator.getNextId(),
                    REPARTITION,
                    LOCAL,
                    new PartitioningScheme(Partitioning.create(FIXED_ARBITRARY_DISTRIBUTION, ImmutableList.of()), node.getOutputVariables()),
                    sources,
                    inputLayouts,
                    false,
                    Optional.empty());
            ExchangeNode exchangeNode = result;

            return deriveProperties(exchangeNode, inputProperties);
        }

        //
        // Joins
        //

        @Override
        public PlanWithProperties visitJoin(JoinNode node, StreamPreferredProperties parentPreferences)
        {
            // Java-based implementation of spilling in join requires constant and known number of
            // LookupJoinOperator's, especially for broadcast joins, when LookupJoinOperator's can be SOURCE
            // distributed. Native implementation doesn't have this limitation.
            // Add LocalExchange with ARBITRARY distribution below join probe source to satisfy that requirement
            // for Java-based execution only.
            PlanWithProperties probe;
            if (isSpillEnabled(session) && isJoinSpillingEnabled(session) && !nativeExecution) {
                probe = planAndEnforce(
                        node.getLeft(),
                        fixedParallelism(),
                        parentPreferences.constrainTo(node.getLeft().getOutputVariables()).withFixedParallelism());
            }
            else {
                probe = planAndEnforce(
                        node.getLeft(),
                        defaultParallelism(session),
                        parentPreferences.constrainTo(node.getLeft().getOutputVariables()).withDefaultParallelism(session));
            }

            // this build consumes the input completely, so we do not pass through parent preferences
            List<VariableReferenceExpression> buildHashVariables = node.getCriteria().stream()
                    .map(EquiJoinClause::getRight)
                    .collect(toImmutableList());
            StreamPreferredProperties buildPreference;
            if (getTaskConcurrency(session) > 1) {
                if (nativeExecution && !isNativeJoinBuildPartitionEnforced(session)) {
                    buildPreference = defaultParallelism(session);
                }
                else {
                    buildPreference = exactlyPartitionedOn(buildHashVariables);
                }
            }
            else {
                buildPreference = singleStream();
            }
            PlanWithProperties build = planAndEnforce(node.getRight(), buildPreference, buildPreference);

            return rebaseAndDeriveProperties(node, ImmutableList.of(probe, build));
        }

        @Override
        public PlanWithProperties visitSemiJoin(SemiJoinNode node, StreamPreferredProperties parentPreferences)
        {
            PlanWithProperties source = planAndEnforce(
                    node.getSource(),
                    defaultParallelism(session),
                    parentPreferences.constrainTo(node.getSource().getOutputVariables()).withDefaultParallelism(session));

            // this filter source consumes the input completely, so we do not pass through parent preferences
            StreamPreferredProperties filteringPreference = nativeExecution ? defaultParallelism(session) : singleStream();
            PlanWithProperties filteringSource = planAndEnforce(node.getFilteringSource(), filteringPreference, filteringPreference);

            return rebaseAndDeriveProperties(node, ImmutableList.of(source, filteringSource));
        }

        @Override
        public PlanWithProperties visitSpatialJoin(SpatialJoinNode node, StreamPreferredProperties parentPreferences)
        {
            PlanWithProperties probe = planAndEnforce(
                    node.getLeft(),
                    defaultParallelism(session),
                    parentPreferences.constrainTo(node.getLeft().getOutputVariables())
                            .withDefaultParallelism(session));

            PlanWithProperties build = planAndEnforce(node.getRight(), singleStream(), singleStream());

            return rebaseAndDeriveProperties(node, ImmutableList.of(probe, build));
        }

        @Override
        public PlanWithProperties visitIndexJoin(IndexJoinNode node, StreamPreferredProperties parentPreferences)
        {
            PlanWithProperties probe = planAndEnforce(
                    node.getProbeSource(),
                    defaultParallelism(session),
                    parentPreferences.constrainTo(node.getProbeSource().getOutputVariables()).withDefaultParallelism(session));

            // index source does not support local parallel and must produce a single stream
            StreamProperties indexStreamProperties = derivePropertiesRecursively(node.getIndexSource(), metadata, session, nativeExecution);
            checkArgument(indexStreamProperties.getDistribution() == SINGLE, "index source must be single stream");
            PlanWithProperties index = new PlanWithProperties(node.getIndexSource(), indexStreamProperties);

            return rebaseAndDeriveProperties(node, ImmutableList.of(probe, index));
        }

        //
        // Helpers
        //

        private PlanWithProperties planAndEnforceChildren(PlanNode node, StreamPreferredProperties requiredProperties, StreamPreferredProperties preferredProperties)
        {
            // plan and enforce each child, but strip any requirement not in terms of symbols produced from the child
            // Note: this assumes the child uses the same symbols as the parent
            List<PlanWithProperties> children = node.getSources().stream()
                    .map(source -> planAndEnforce(
                            source,
                            requiredProperties.constrainTo(source.getOutputVariables()),
                            preferredProperties.constrainTo(source.getOutputVariables())))
                    .collect(toImmutableList());

            return rebaseAndDeriveProperties(node, children);
        }

        private PlanWithProperties planAndEnforce(PlanNode node, StreamPreferredProperties requiredProperties, StreamPreferredProperties preferredProperties)
        {
            // verify properties are in terms of symbols produced by the node
            checkArgument(requiredProperties.getPartitioningColumns().map(node.getOutputVariables()::containsAll).orElse(true));
            checkArgument(preferredProperties.getPartitioningColumns().map(node.getOutputVariables()::containsAll).orElse(true));

            // plan the node using the preferred properties
            PlanWithProperties result = accept(node, preferredProperties);

            // enforce the required properties
            result = enforce(result, requiredProperties);

            checkState(requiredProperties.isSatisfiedBy(result.getProperties()), "required properties not enforced");
            return result;
        }

        private PlanWithProperties enforce(PlanWithProperties planWithProperties, StreamPreferredProperties requiredProperties)
        {
            if (requiredProperties.isSatisfiedBy(planWithProperties.getProperties())) {
                return planWithProperties;
            }

            if (requiredProperties.isSingleStreamPreferred()) {
                ExchangeNode exchangeNode = gatheringExchange(idAllocator.getNextId(), LOCAL, planWithProperties.getNode());
                return deriveProperties(exchangeNode, planWithProperties.getProperties());
            }

            Optional<List<VariableReferenceExpression>> requiredPartitionColumns = requiredProperties.getPartitioningColumns();
            if (!requiredPartitionColumns.isPresent()) {
                // unpartitioned parallel streams required
                return deriveProperties(
                        roundRobinExchange(idAllocator.getNextId(), LOCAL, planWithProperties.getNode()),
                        planWithProperties.getProperties());
            }

            if (requiredProperties.isParallelPreferred()) {
                // partitioned parallel streams required
                ExchangeNode exchangeNode = systemPartitionedExchange(
                        idAllocator.getNextId(),
                        LOCAL,
                        planWithProperties.getNode(),
                        requiredPartitionColumns.get(),
                        Optional.empty());
                return deriveProperties(exchangeNode, planWithProperties.getProperties());
            }

            // no explicit parallel requirement, so gather to a single stream
            ExchangeNode exchangeNode = gatheringExchange(
                    idAllocator.getNextId(),
                    LOCAL,
                    planWithProperties.getNode());
            return deriveProperties(exchangeNode, planWithProperties.getProperties());
        }

        private PlanWithProperties rebaseAndDeriveProperties(PlanNode node, List<PlanWithProperties> children)
        {
            PlanNode result = replaceChildren(
                    node,
                    children.stream()
                            .map(PlanWithProperties::getNode)
                            .collect(toList()));

            List<StreamProperties> inputProperties = children.stream()
                    .map(PlanWithProperties::getProperties)
                    .collect(toImmutableList());

            return deriveProperties(result, inputProperties);
        }

        private PlanWithProperties deriveProperties(PlanNode result, StreamProperties inputProperties)
        {
            return new PlanWithProperties(result, StreamPropertyDerivations.deriveProperties(result, inputProperties, metadata, session, nativeExecution));
        }

        private PlanWithProperties deriveProperties(PlanNode result, List<StreamProperties> inputProperties)
        {
            return new PlanWithProperties(result, StreamPropertyDerivations.deriveProperties(result, inputProperties, metadata, session, nativeExecution));
        }

        private PlanWithProperties accept(PlanNode node, StreamPreferredProperties context)
        {
            PlanWithProperties result = node.accept(this, context);
            // TableWriter and TableWriterMergeNode has different output
            boolean passStatsEquivalentPlanNode = !(node instanceof TableWriterNode && result.getNode() instanceof TableWriterMergeNode);
            return new PlanWithProperties(
                    passStatsEquivalentPlanNode ? result.getNode().assignStatsEquivalentPlanNode(node.getStatsEquivalentPlanNode()) : result.getNode(),
                    result.getProperties());
        }
    }

    private static class PlanWithProperties
    {
        private final PlanNode node;
        private final StreamProperties properties;

        public PlanWithProperties(PlanNode node, StreamProperties properties)
        {
            this.node = requireNonNull(node, "node is null");
            this.properties = requireNonNull(properties, "StreamProperties is null");
        }

        public PlanNode getNode()
        {
            return node;
        }

        public StreamProperties getProperties()
        {
            return properties;
        }
    }
}
