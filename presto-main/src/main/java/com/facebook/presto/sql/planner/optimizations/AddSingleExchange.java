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
import com.facebook.presto.execution.QueryManagerConfig.ExchangeMaterializationStrategy;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.OutputNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.analyzer.FeaturesConfig.PartialMergePushdownStrategy;
import com.facebook.presto.sql.planner.PartitioningProviderManager;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.ChildReplacer;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.ExplainAnalyzeNode;
import com.facebook.presto.sql.planner.plan.InternalPlanVisitor;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.SystemSessionProperties.getExchangeMaterializationStrategy;
import static com.facebook.presto.SystemSessionProperties.getHashPartitionCount;
import static com.facebook.presto.SystemSessionProperties.getPartialMergePushdownStrategy;
import static com.facebook.presto.SystemSessionProperties.getPartitioningProviderCatalog;
import static com.facebook.presto.SystemSessionProperties.isDistributedIndexJoinEnabled;
import static com.facebook.presto.SystemSessionProperties.isForceSingleNodeOutput;
import static com.facebook.presto.SystemSessionProperties.isPreferDistributedUnion;
import static com.facebook.presto.SystemSessionProperties.isRedistributeWrites;
import static com.facebook.presto.SystemSessionProperties.isScaleWriters;
import static com.facebook.presto.SystemSessionProperties.preferStreamingOperators;
import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.sql.planner.PlannerUtils.containsSystemTableScan;
import static com.facebook.presto.sql.planner.iterative.rule.PickTableLayout.pushPredicateIntoTableScan;
import static com.facebook.presto.sql.planner.optimizations.ActualProperties.Global.singleStreamPartition;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE_STREAMING;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.gatheringExchange;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class AddSingleExchange
        implements PlanOptimizer
{
    private final Metadata metadata;
    private final PartitioningProviderManager partitioningProviderManager;
    private final boolean nativeExecution;

    public AddSingleExchange(Metadata metadata, PartitioningProviderManager partitioningProviderManager, boolean nativeExecution)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.partitioningProviderManager = requireNonNull(partitioningProviderManager, "partitioningProviderManager is null");
        this.nativeExecution = nativeExecution;
    }

    @Override
    public PlanOptimizerResult optimize(PlanNode plan, Session session, TypeProvider types, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        PlanWithProperties result = new Rewriter(idAllocator, variableAllocator, session, partitioningProviderManager, nativeExecution).accept(plan, PreferredProperties.any());
        boolean optimizerTriggered = PlanNodeSearcher.searchFrom(result.getNode()).where(node -> node instanceof ExchangeNode && ((ExchangeNode) node).getScope().isRemote()).findFirst().isPresent();
        return PlanOptimizerResult.optimizerResult(result.getNode(), optimizerTriggered);
    }

    private class Rewriter
            extends InternalPlanVisitor<PlanWithProperties, PreferredProperties>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final VariableAllocator variableAllocator;
        private final TypeProvider types;
        private final Session session;
        private final boolean distributedIndexJoins;
        private final boolean preferStreamingOperators;
        private final boolean redistributeWrites;
        private final boolean scaleWriters;
        private final boolean preferDistributedUnion;
        private final PartialMergePushdownStrategy partialMergePushdownStrategy;
        private final String partitioningProviderCatalog;
        private final int hashPartitionCount;
        private final ExchangeMaterializationStrategy exchangeMaterializationStrategy;
        private final PartitioningProviderManager partitioningProviderManager;
        private final boolean nativeExecution;

        public Rewriter(PlanNodeIdAllocator idAllocator, VariableAllocator variableAllocator, Session session, PartitioningProviderManager partitioningProviderManager, boolean nativeExecution)
        {
            this.idAllocator = idAllocator;
            this.variableAllocator = variableAllocator;
            this.types = TypeProvider.viewOf(variableAllocator.getVariables());
            this.session = session;
            this.distributedIndexJoins = isDistributedIndexJoinEnabled(session);
            this.redistributeWrites = isRedistributeWrites(session);
            this.scaleWriters = isScaleWriters(session);
            this.preferDistributedUnion = isPreferDistributedUnion(session);
            this.partialMergePushdownStrategy = getPartialMergePushdownStrategy(session);
            this.preferStreamingOperators = preferStreamingOperators(session);
            this.partitioningProviderCatalog = getPartitioningProviderCatalog(session);
            this.hashPartitionCount = getHashPartitionCount(session);
            this.exchangeMaterializationStrategy = getExchangeMaterializationStrategy(session);
            this.partitioningProviderManager = requireNonNull(partitioningProviderManager, "partitioningProviderManager is null");
            this.nativeExecution = nativeExecution;
        }

        @Override
        public AddSingleExchange.PlanWithProperties visitPlan(PlanNode node, PreferredProperties preferredProperties)
        {
            return rebaseAndDeriveProperties(node, planChild(node, preferredProperties));
        }

        @Override
        public PlanWithProperties visitTableScan(TableScanNode node, PreferredProperties preferredProperties)
        {
            return planTableScan(node, TRUE_CONSTANT);
        }

        @Override
        public PlanWithProperties visitOutput(OutputNode node, PreferredProperties preferredProperties)
        {
            PlanWithProperties child = planChild(node, PreferredProperties.undistributed());

            if (!child.getProperties().isSingleNode() && isForceSingleNodeOutput(session)) {
                child = withDerivedProperties(gatheringExchange(idAllocator.getNextId(), REMOTE_STREAMING, child.getNode()), child.getProperties());
            }

            return rebaseAndDeriveProperties(node, child);
        }

        @Override
        public AddSingleExchange.PlanWithProperties visitExplainAnalyze(ExplainAnalyzeNode node, PreferredProperties preferredProperties)
        {
            AddSingleExchange.PlanWithProperties child = planChild(node, PreferredProperties.any());

            // if the child is already a gathering exchange, don't add another
            if ((child.getNode() instanceof ExchangeNode) && ((ExchangeNode) child.getNode()).getType() == ExchangeNode.Type.GATHER) {
                return rebaseAndDeriveProperties(node, child);
            }

            // Always add an exchange because ExplainAnalyze should be in its own stage
            child = withDerivedProperties(gatheringExchange(idAllocator.getNextId(), REMOTE_STREAMING, child.getNode()), child.getProperties());

            return rebaseAndDeriveProperties(node, child);
        }

        @Override
        public AddSingleExchange.PlanWithProperties visitValues(ValuesNode node, PreferredProperties preferredProperties)
        {
            return new AddSingleExchange.PlanWithProperties(node, ActualProperties.builder().global(singleStreamPartition()).build());
        }

        private AddSingleExchange.PlanWithProperties planTableScan(TableScanNode node, RowExpression predicate)
        {
            PlanNode plan = pushPredicateIntoTableScan(node, predicate, true, session, idAllocator, metadata);
            // Presto Java and Presto Native use different hash functions for partitioning
            // An additional exchange makes sure the data flows through a native worker in case it need to be partitioned for downstream processing
            if (nativeExecution && containsSystemTableScan(plan)) {
                plan = gatheringExchange(idAllocator.getNextId(), REMOTE_STREAMING, plan);
            }
            // TODO: Support selecting layout with best local property once connector can participate in query optimization.
            return new AddSingleExchange.PlanWithProperties(plan, derivePropertiesRecursively(plan));
        }

        private ActualProperties deriveProperties(PlanNode result, ActualProperties inputProperties)
        {
            return deriveProperties(result, ImmutableList.of(inputProperties));
        }

        private ActualProperties deriveProperties(PlanNode result, List<ActualProperties> inputProperties)
        {
            // TODO: move this logic to PlanChecker once PropertyDerivations.deriveProperties fully supports local exchanges
            ActualProperties outputProperties = PropertyDerivations.deriveProperties(result, inputProperties, metadata, session);
            verify(result instanceof SemiJoinNode || inputProperties.stream().noneMatch(ActualProperties::isNullsAndAnyReplicated) || outputProperties.isNullsAndAnyReplicated(), "SemiJoinNode is the only node that can strip null replication");
            return outputProperties;
        }

        private AddSingleExchange.PlanWithProperties rebaseAndDeriveProperties(PlanNode node, AddSingleExchange.PlanWithProperties child)
        {
            return withDerivedProperties(ChildReplacer.replaceChildren(node, ImmutableList.of(child.getNode())), child.getProperties());
        }

        private AddSingleExchange.PlanWithProperties rebaseAndDeriveProperties(PlanNode node, List<AddSingleExchange.PlanWithProperties> children)
        {
            PlanNode result = node.replaceChildren(children.stream().map(AddSingleExchange.PlanWithProperties::getNode).collect(toList()));
            return new AddSingleExchange.PlanWithProperties(result, deriveProperties(result, children.stream().map(AddSingleExchange.PlanWithProperties::getProperties).collect(toList())));
        }

        private AddSingleExchange.PlanWithProperties withDerivedProperties(PlanNode node, ActualProperties inputProperties)
        {
            return new AddSingleExchange.PlanWithProperties(node, deriveProperties(node, inputProperties));
        }

        private AddSingleExchange.PlanWithProperties accept(PlanNode plan, PreferredProperties context)
        {
            AddSingleExchange.PlanWithProperties result = plan.accept(this, context);
            return new AddSingleExchange.PlanWithProperties(result.getNode().assignStatsEquivalentPlanNode(plan.getStatsEquivalentPlanNode()), result.getProperties());
        }

        private AddSingleExchange.PlanWithProperties planChild(PlanNode node, PreferredProperties preferredProperties)
        {
            return accept(getOnlyElement(node.getSources()), preferredProperties);
        }

        private ActualProperties derivePropertiesRecursively(PlanNode result)
        {
            return PropertyDerivations.derivePropertiesRecursively(result, metadata, session);
        }
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
}
