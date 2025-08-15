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
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.cost.TaskCountEstimator;
import com.facebook.presto.cost.VariableStatsEstimate;
import com.facebook.presto.execution.TaskManagerConfig;
import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Partitioning;
import com.facebook.presto.spi.plan.PartitioningScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.optimizations.StreamPreferredProperties;
import com.facebook.presto.sql.planner.optimizations.StreamPropertyDerivations.StreamProperties;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.GroupIdNode;
import com.facebook.presto.sql.relational.ProjectNodeUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multiset;
import io.airlift.units.DataSize;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.getTaskConcurrency;
import static com.facebook.presto.SystemSessionProperties.isEnabledAddExchangeBelowGroupId;
import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.matching.Pattern.nonEmpty;
import static com.facebook.presto.matching.Pattern.typeOf;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static com.facebook.presto.sql.planner.optimizations.StreamPreferredProperties.fixedParallelism;
import static com.facebook.presto.sql.planner.optimizations.StreamPropertyDerivations.deriveProperties;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE_STREAMING;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.partitionedExchange;
import static com.facebook.presto.sql.planner.plan.Patterns.Aggregation.groupingColumns;
import static com.facebook.presto.sql.planner.plan.Patterns.Aggregation.step;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMultiset.toImmutableMultiset;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.lang.Double.isNaN;
import static java.lang.Math.min;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;

/**
 * Transforms
 * <pre>
 *   - Exchange
 *     - [ Projection ]
 *       - Partial Aggregation
 *         - GroupId
 * </pre>
 * to
 * <pre>
 *   - Exchange
 *     - [ Projection ]
 *       - Partial Aggregation
 *         - GroupId
 *           - LocalExchange
 *             - RemoteExchange
 * </pre>
 * <p>
 * Rationale: GroupId increases number of rows (number of times equal to number of grouping sets) and then
 * partial aggregation reduces number of rows. However, under certain conditions, exchanging the rows before
 * GroupId (before multiplication) makes partial aggregation more effective, resulting in less data being
 * exchanged afterwards.
 */
public class AddExchangesBelowPartialAggregationOverGroupIdRuleSet
{
    private static final Capture<ProjectNode> PROJECTION = newCapture();
    private static final Capture<AggregationNode> AGGREGATION = newCapture();
    private static final Capture<GroupIdNode> GROUP_ID = newCapture();
    private static final Capture<ExchangeNode> REMOTE_EXCHANGE = newCapture();

    private static final Pattern<ExchangeNode> WITH_PROJECTION =
            // If there was no exchange here, adding new exchanges could break property derivations logic of AddExchanges, AddLocalExchanges
            typeOf(ExchangeNode.class)
                    .matching(e -> e.getScope().isRemote()).capturedAs(REMOTE_EXCHANGE)
                    .with(source().matching(
                            // PushPartialAggregationThroughExchange adds a projection. However, it can be removed if RemoveRedundantIdentityProjections is run in the mean-time.
                            typeOf(ProjectNode.class).matching(ProjectNodeUtils::isIdentity).capturedAs(PROJECTION)
                                    .with(source().matching(
                                            typeOf(AggregationNode.class).capturedAs(AGGREGATION)
                                                    .with(step().equalTo(AggregationNode.Step.PARTIAL))
                                                    .with(nonEmpty(groupingColumns()))
                                                    .with(source().matching(
                                                            typeOf(GroupIdNode.class).capturedAs(GROUP_ID)))))));

    private static final Pattern<ExchangeNode> WITHOUT_PROJECTION =
            // If there was no exchange here, adding new exchanges could break property derivations logic of AddExchanges, AddLocalExchanges
            typeOf(ExchangeNode.class)
                    .matching(e -> e.getScope().isRemote()).capturedAs(REMOTE_EXCHANGE)
                    .with(source().matching(
                            typeOf(AggregationNode.class).capturedAs(AGGREGATION)
                                    .with(step().equalTo(AggregationNode.Step.PARTIAL))
                                    .with(nonEmpty(groupingColumns()))
                                    .with(source().matching(
                                            typeOf(GroupIdNode.class).capturedAs(GROUP_ID)))));

    private static final double GROUPING_SETS_SYMBOL_REQUIRED_FREQUENCY = 0.5;
    private static final double ANTI_SKEWNESS_MARGIN = 3;
    private final TaskCountEstimator taskCountEstimator;
    private final DataSize maxPartialAggregationMemoryUsage;
    private final Metadata metadata;
    private final boolean nativeExecution;

    public AddExchangesBelowPartialAggregationOverGroupIdRuleSet(
            TaskCountEstimator taskCountEstimator,
            TaskManagerConfig taskManagerConfig,
            Metadata metadata,
            boolean nativeExecution)
    {
        this.taskCountEstimator = requireNonNull(taskCountEstimator, "taskCountEstimator is null");
        this.maxPartialAggregationMemoryUsage = taskManagerConfig.getMaxPartialAggregationMemoryUsage();
        this.metadata = metadata;
        this.nativeExecution = nativeExecution;
    }

    public Set<Rule<?>> rules()
    {
        return ImmutableSet.of(
                belowProjectionRule(),
                belowExchangeRule());
    }

    @VisibleForTesting
    AddExchangesBelowExchangePartialAggregationGroupId belowExchangeRule()
    {
        return new AddExchangesBelowExchangePartialAggregationGroupId();
    }

    @VisibleForTesting
    AddExchangesBelowProjectionPartialAggregationGroupId belowProjectionRule()
    {
        return new AddExchangesBelowProjectionPartialAggregationGroupId();
    }

    @VisibleForTesting
    class AddExchangesBelowProjectionPartialAggregationGroupId
            extends BaseAddExchangesBelowExchangePartialAggregationGroupId
    {
        @Override
        public Pattern<ExchangeNode> getPattern()
        {
            return WITH_PROJECTION;
        }

        @Override
        public Result apply(ExchangeNode exchange, Captures captures, Context context)
        {
            ProjectNode project = captures.get(PROJECTION);
            AggregationNode aggregation = captures.get(AGGREGATION);
            GroupIdNode groupId = captures.get(GROUP_ID);
            return transform(aggregation, groupId, context)
                    .map(newAggregation -> Result.ofPlanNode(
                            exchange.replaceChildren(ImmutableList.of(
                                    project.replaceChildren(ImmutableList.of(
                                            newAggregation))))))
                    .orElseGet(Result::empty);
        }
    }

    @VisibleForTesting
    class AddExchangesBelowExchangePartialAggregationGroupId
            extends BaseAddExchangesBelowExchangePartialAggregationGroupId
    {
        @Override
        public Pattern<ExchangeNode> getPattern()
        {
            return WITHOUT_PROJECTION;
        }

        @Override
        public Result apply(ExchangeNode exchange, Captures captures, Context context)
        {
            AggregationNode aggregation = captures.get(AGGREGATION);
            GroupIdNode groupId = captures.get(GROUP_ID);
            return transform(aggregation, groupId, context)
                    .map(newAggregation -> {
                        PlanNode newExchange = exchange.replaceChildren(ImmutableList.of(newAggregation));
                        return Result.ofPlanNode(newExchange);
                    })
                    .orElseGet(Result::empty);
        }
    }

    private abstract class BaseAddExchangesBelowExchangePartialAggregationGroupId
            implements Rule<ExchangeNode>
    {
        @Override
        public boolean isEnabled(Session session)
        {
            return isEnabledAddExchangeBelowGroupId(session);
        }

        protected Optional<PlanNode> transform(AggregationNode aggregation, GroupIdNode groupId, Context context)
        {
            Set<VariableReferenceExpression> groupingKeys = aggregation.getGroupingKeys().stream()
                    .filter(symbol -> !groupId.getGroupIdVariable().equals(symbol))
                    .collect(toImmutableSet());

            Multiset<VariableReferenceExpression> groupingSetHistogram = groupId.getGroupingSets().stream()
                    .flatMap(Collection::stream)
                    .collect(toImmutableMultiset());

            if (!Objects.equals(groupingSetHistogram.elementSet(), groupingKeys)) {
                // TODO handle the case when some aggregation keys are pass-through in GroupId (e.g. common in all grouping sets)
                // TODO handle the case when some grouping set symbols are not used in aggregation (possible?)
                return Optional.empty();
            }

            double aggregationMemoryRequirements = estimateAggregationMemoryRequirements(groupingKeys, groupId, groupingSetHistogram, context);
            if (isNaN(aggregationMemoryRequirements) || aggregationMemoryRequirements < maxPartialAggregationMemoryUsage.toBytes()) {
                // Aggregation will be effective even without exchanges (or we have insufficient information).
                return Optional.empty();
            }

            List<VariableReferenceExpression> desiredHashVariables = groupingSetHistogram.entrySet().stream()
                    // Take only frequently used symbols
                    .filter(entry -> entry.getCount() >= groupId.getGroupingSets().size() * GROUPING_SETS_SYMBOL_REQUIRED_FREQUENCY)
                    .map(Multiset.Entry::getElement)
                    // And only the symbols used in the aggregation (these are usually all symbols)
                    .peek(symbol -> verify(groupingKeys.contains(symbol), "%s not found in the grouping keys [%s]", symbol, groupingKeys))
                    // Transform to symbols before GroupId
                    .map(groupId.getGroupingColumns()::get)
                    .collect(toImmutableList());

            // Use only the symbol with the highest cardinality (if we have statistics). This makes partial aggregation more efficient in case of
            // low correlation between symbol that are in every grouping set vs additional symbols.
            PlanNodeStatsEstimate sourceStats = context.getStatsProvider().getStats(groupId.getSource());
            desiredHashVariables = desiredHashVariables.stream()
                    .filter(symbol -> !isNaN(sourceStats.getVariableStatistics(symbol).getDistinctValuesCount()))
                    .max(comparing(symbol -> sourceStats.getVariableStatistics(symbol).getDistinctValuesCount()))
                    .map(symbol -> (List<VariableReferenceExpression>) ImmutableList.of(symbol)).orElse(desiredHashVariables);

            StreamPreferredProperties requiredProperties = fixedParallelism().withPartitioning(desiredHashVariables);
            StreamProperties sourceProperties = derivePropertiesRecursively(groupId.getSource(), context);
            if (requiredProperties.isSatisfiedBy(sourceProperties)) {
                // Stream is already (locally) partitioned just as we want.
                // In fact, there might be just a LocalExchange below and no Remote. For now, we give up in this situation anyway. To properly support such situation:
                //  1. aggregation effectiveness estimation below need to consider the (helpful) fact that stream is already partitioned, so each operator will need less memory
                //  2. if the local exchange becomes unnecessary (after we add a remove on top of it), it should be removed. What if the local exchange is somewhere further
                //     down the tree?
                return Optional.empty();
            }

            double estimatedGroups = estimateGroupCount(desiredHashVariables, context.getStatsProvider().getStats(groupId.getSource()));
            if (isNaN(estimatedGroups) || estimatedGroups * ANTI_SKEWNESS_MARGIN < maximalConcurrencyAfterRepartition(context)) {
                // Desired hash symbols form too few groups. Hashing over them would harm concurrency.
                // TODO instead of taking symbols with >GROUPING_SETS_SYMBOL_REQUIRED_FREQUENCY presence, we could take symbols from high freq to low until there are enough groups
                return Optional.empty();
            }

            PlanNode source = groupId.getSource();

            // Above we only checked the data is not yet locally partitioned and it could be already globally partitioned (but not locally). TODO avoid remote exchange in this case
            // TODO If the aggregation memory requirements are only slightly above `maxPartialAggregationMemoryUsage`, adding only LocalExchange could be enough
            source = partitionedExchange(
                    context.getIdAllocator().getNextId(),
                    REMOTE_STREAMING,
                    source,
                    new PartitioningScheme(
                            Partitioning.create(FIXED_HASH_DISTRIBUTION, desiredHashVariables),
                            source.getOutputVariables()));

            source = partitionedExchange(
                    context.getIdAllocator().getNextId(),
                    LOCAL,
                    source,
                    new PartitioningScheme(
                            Partitioning.create(FIXED_HASH_DISTRIBUTION, desiredHashVariables),
                            source.getOutputVariables()));

            PlanNode newGroupId = groupId.replaceChildren(ImmutableList.of(source));
            PlanNode newAggregation = aggregation.replaceChildren(ImmutableList.of(newGroupId));

            return Optional.of(newAggregation);
        }

        private int maximalConcurrencyAfterRepartition(Context context)
        {
            return getTaskConcurrency(context.getSession()) * taskCountEstimator.estimateHashedTaskCount(context.getSession());
        }

        private double estimateAggregationMemoryRequirements(Set<VariableReferenceExpression> groupingKeys,
                GroupIdNode groupId,
                Multiset<VariableReferenceExpression> groupingSetHistogram,
                Context context)
        {
            checkArgument(Objects.equals(groupingSetHistogram.elementSet(), groupingKeys)); // Otherwise math below would be off-topic

            PlanNodeStatsEstimate sourceStats = context.getStatsProvider().getStats(groupId.getSource());
            double keysMemoryRequirements = 0;

            for (List<VariableReferenceExpression> groupingSet : groupId.getGroupingSets()) {
                List<VariableReferenceExpression> sourceVariables = groupingSet.stream()
                        .map(groupId.getGroupingColumns()::get)
                        .collect(toImmutableList());

                double keyWidth = sourceStats.getOutputSizeForVariables(sourceVariables) / sourceStats.getOutputRowCount();
                double keyNdv = min(estimateGroupCount(sourceVariables, sourceStats), sourceStats.getOutputRowCount());

                keysMemoryRequirements += keyWidth * keyNdv;
            }

            // TODO consider also memory requirements for aggregation values
            return keysMemoryRequirements;
        }

        private double estimateGroupCount(List<VariableReferenceExpression> variables, PlanNodeStatsEstimate statsEstimate)
        {
            return variables.stream()
                    .map(statsEstimate::getVariableStatistics)
                    .mapToDouble(this::ndvIncludingNull)
                    // This assumes no correlation, maximum number of aggregation keys
                    .reduce(1, (a, b) -> a * b);
        }

        private double ndvIncludingNull(VariableStatsEstimate variableStatsEstimate)
        {
            if (variableStatsEstimate.getNullsFraction() == 0.) {
                return variableStatsEstimate.getDistinctValuesCount();
            }
            return variableStatsEstimate.getDistinctValuesCount() + 1;
        }

        private StreamProperties derivePropertiesRecursively(PlanNode node, Context context)
        {
            PlanNode resolvedPlanNode = context.getLookup().resolve(node);
            List<StreamProperties> inputProperties = resolvedPlanNode.getSources().stream()
                    .map(source -> derivePropertiesRecursively(source, context))
                    .collect(toImmutableList());
            return deriveProperties(resolvedPlanNode, inputProperties, metadata, context.getSession(), nativeExecution);
        }
    }
}
