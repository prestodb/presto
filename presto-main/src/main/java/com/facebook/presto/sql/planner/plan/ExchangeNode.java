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
package com.facebook.presto.sql.planner.plan;

import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.Partitioning;
import com.facebook.presto.sql.planner.PartitioningHandle;
import com.facebook.presto.sql.planner.PartitioningScheme;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_BROADCAST_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_PASSTHROUGH_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE_MATERIALIZED;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.GATHER;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPLICATE;
import static com.facebook.presto.util.MoreLists.listOfListsCopy;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@Immutable
public class ExchangeNode
        extends InternalPlanNode
{
    public enum Type
    {
        GATHER,
        REPARTITION,
        REPLICATE
    }

    public enum Scope
    {
        LOCAL(false),
        REMOTE_STREAMING(true),
        REMOTE_MATERIALIZED(true),
        /**/;

        private boolean remote;

        Scope(boolean remote)
        {
            this.remote = remote;
        }

        public boolean isRemote()
        {
            return remote;
        }

        public boolean isLocal()
        {
            return !isRemote();
        }
    }

    private final Type type;
    private final Scope scope;

    private final List<PlanNode> sources;

    private final PartitioningScheme partitioningScheme;

    // for each source, the list of inputs corresponding to each output
    private final List<List<VariableReferenceExpression>> inputs;

    private final boolean ensureSourceOrdering;
    private final Optional<OrderingScheme> orderingScheme;

    @JsonCreator
    public ExchangeNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("type") Type type,
            @JsonProperty("scope") Scope scope,
            @JsonProperty("partitioningScheme") PartitioningScheme partitioningScheme,
            @JsonProperty("sources") List<PlanNode> sources,
            @JsonProperty("inputs") List<List<VariableReferenceExpression>> inputs,
            @JsonProperty("ensureSourceOrdering") boolean ensureSourceOrdering,
            @JsonProperty("orderingScheme") Optional<OrderingScheme> orderingScheme)
    {
        super(id);

        requireNonNull(type, "type is null");
        requireNonNull(scope, "scope is null");
        requireNonNull(sources, "sources is null");
        requireNonNull(partitioningScheme, "partitioningScheme is null");
        requireNonNull(inputs, "inputs is null");
        requireNonNull(orderingScheme, "orderingScheme is null");

        checkArgument(!inputs.isEmpty(), "inputs is empty");
        checkArgument(inputs.stream().allMatch(inputVariables -> inputVariables.size() == partitioningScheme.getOutputLayout().size()), "Input symbols do not match output symbols");
        checkArgument(inputs.size() == sources.size(), "Must have same number of input lists as sources");
        for (int i = 0; i < inputs.size(); i++) {
            checkArgument(sources.get(i).getOutputVariables().containsAll(inputs.get(i)), "Source does not supply all required input variables");
        }

        checkArgument(!scope.isLocal() || partitioningScheme.getPartitioning().getArguments().stream().allMatch(VariableReferenceExpression.class::isInstance),
                "local exchanges do not support constant partition function arguments");

        checkArgument(!scope.isRemote() || type == REPARTITION || !partitioningScheme.isReplicateNullsAndAny(), "Only REPARTITION can replicate remotely");
        checkArgument(scope != REMOTE_MATERIALIZED || type == REPARTITION, "Only REPARTITION can be REMOTE_MATERIALIZED: %s", type);

        orderingScheme.ifPresent(ordering -> {
            PartitioningHandle partitioningHandle = partitioningScheme.getPartitioning().getHandle();
            checkArgument(!scope.isRemote() || partitioningHandle.equals(SINGLE_DISTRIBUTION), "remote merging exchange requires single distribution");
            checkArgument(!scope.isLocal() || partitioningHandle.equals(FIXED_PASSTHROUGH_DISTRIBUTION), "local merging exchange requires passthrough distribution");
            checkArgument(partitioningScheme.getOutputLayout().containsAll(ordering.getOrderByVariables()), "Partitioning scheme does not supply all required ordering symbols");
        });
        this.type = type;
        this.sources = sources;
        this.scope = scope;
        this.partitioningScheme = partitioningScheme;
        this.inputs = listOfListsCopy(inputs);
        this.ensureSourceOrdering = ensureSourceOrdering;
        orderingScheme.ifPresent(scheme -> checkArgument(ensureSourceOrdering, "if ordering scheme is present the exchange must ensure source ordering"));
        this.orderingScheme = orderingScheme;
    }

    public static ExchangeNode systemPartitionedExchange(PlanNodeId id, Scope scope, PlanNode child, List<VariableReferenceExpression> partitioningColumns, Optional<VariableReferenceExpression> hashColumn)
    {
        return systemPartitionedExchange(id, scope, child, partitioningColumns, hashColumn, false);
    }

    public static ExchangeNode systemPartitionedExchange(PlanNodeId id, Scope scope, PlanNode child, List<VariableReferenceExpression> partitioningColumns, Optional<VariableReferenceExpression> hashColumn, boolean replicateNullsAndAny)
    {
        return partitionedExchange(
                id,
                scope,
                child,
                Partitioning.create(FIXED_HASH_DISTRIBUTION, partitioningColumns),
                hashColumn,
                replicateNullsAndAny);
    }

    public static ExchangeNode partitionedExchange(PlanNodeId id, Scope scope, PlanNode child, Partitioning partitioning, Optional<VariableReferenceExpression> hashColumn)
    {
        return partitionedExchange(id, scope, child, partitioning, hashColumn, false);
    }

    public static ExchangeNode partitionedExchange(PlanNodeId id, Scope scope, PlanNode child, Partitioning partitioning, Optional<VariableReferenceExpression> hashColumn, boolean replicateNullsAndAny)
    {
        return partitionedExchange(
                id,
                scope,
                child,
                new PartitioningScheme(
                        partitioning,
                        child.getOutputVariables(),
                        hashColumn,
                        replicateNullsAndAny,
                        Optional.empty()));
    }

    public static ExchangeNode partitionedExchange(PlanNodeId id, Scope scope, PlanNode child, PartitioningScheme partitioningScheme)
    {
        if (partitioningScheme.getPartitioning().getHandle().isSingleNode()) {
            return gatheringExchange(id, scope, child);
        }
        return new ExchangeNode(
                id,
                REPARTITION,
                scope,
                partitioningScheme,
                ImmutableList.of(child),
                ImmutableList.of(partitioningScheme.getOutputLayout()),
                false,
                Optional.empty());
    }

    public static ExchangeNode replicatedExchange(PlanNodeId id, Scope scope, PlanNode child)
    {
        return new ExchangeNode(
                id,
                REPLICATE,
                scope,
                new PartitioningScheme(Partitioning.create(FIXED_BROADCAST_DISTRIBUTION, ImmutableList.of()), child.getOutputVariables()),
                ImmutableList.of(child),
                ImmutableList.of(child.getOutputVariables()),
                false,
                Optional.empty());
    }

    public static ExchangeNode gatheringExchange(PlanNodeId id, Scope scope, PlanNode child)
    {
        return gatheringExchange(id, scope, child, false);
    }

    public static ExchangeNode ensureSourceOrderingGatheringExchange(PlanNodeId id, Scope scope, PlanNode child)
    {
        return gatheringExchange(id, scope, child, true);
    }

    private static ExchangeNode gatheringExchange(PlanNodeId id, Scope scope, PlanNode child, boolean ensureSourceOrdering)
    {
        return new ExchangeNode(
                id,
                GATHER,
                scope,
                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), child.getOutputVariables()),
                ImmutableList.of(child),
                ImmutableList.of(child.getOutputVariables()),
                ensureSourceOrdering,
                Optional.empty());
    }

    public static ExchangeNode roundRobinExchange(PlanNodeId id, Scope scope, PlanNode child)
    {
        return partitionedExchange(
                id,
                scope,
                child,
                new PartitioningScheme(Partitioning.create(FIXED_ARBITRARY_DISTRIBUTION, ImmutableList.of()), child.getOutputVariables()));
    }

    public static ExchangeNode mergingExchange(PlanNodeId id, Scope scope, PlanNode child, OrderingScheme orderingScheme)
    {
        PartitioningHandle partitioningHandle = scope.isLocal() ? FIXED_PASSTHROUGH_DISTRIBUTION : SINGLE_DISTRIBUTION;
        return new ExchangeNode(
                id,
                GATHER,
                scope,
                new PartitioningScheme(Partitioning.create(partitioningHandle, ImmutableList.of()), child.getOutputVariables()),
                ImmutableList.of(child),
                ImmutableList.of(child.getOutputVariables()),
                true,
                Optional.of(orderingScheme));
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @JsonProperty
    public Scope getScope()
    {
        return scope;
    }

    @Override
    @JsonProperty
    public List<PlanNode> getSources()
    {
        return sources;
    }

    @Override
    public List<VariableReferenceExpression> getOutputVariables()
    {
        return partitioningScheme.getOutputLayout();
    }

    @JsonProperty
    public PartitioningScheme getPartitioningScheme()
    {
        return partitioningScheme;
    }

    @JsonProperty
    public boolean isEnsureSourceOrdering()
    {
        return ensureSourceOrdering;
    }

    @JsonProperty
    public Optional<OrderingScheme> getOrderingScheme()
    {
        return orderingScheme;
    }

    @JsonProperty
    public List<List<VariableReferenceExpression>> getInputs()
    {
        return inputs;
    }

    @Override
    public <R, C> R accept(InternalPlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitExchange(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new ExchangeNode(getId(), type, scope, partitioningScheme, newChildren, inputs, ensureSourceOrdering, orderingScheme);
    }
}
