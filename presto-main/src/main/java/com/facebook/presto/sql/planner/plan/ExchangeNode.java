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

import com.facebook.presto.sql.planner.Partitioning;
import com.facebook.presto.sql.planner.Partitioning.ArgumentBinding;
import com.facebook.presto.sql.planner.PartitioningScheme;
import com.facebook.presto.sql.planner.Symbol;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_BROADCAST_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@Immutable
public class ExchangeNode
        extends PlanNode
{
    public enum Type
    {
        GATHER,
        REPARTITION,
        REPLICATE
    }

    public enum Scope
    {
        LOCAL,
        REMOTE
    }

    private final Type type;
    private final Scope scope;

    private final List<PlanNode> sources;

    private final PartitioningScheme partitioningScheme;

    // for each source, the list of inputs corresponding to each output
    private final List<List<Symbol>> inputs;

    @JsonCreator
    public ExchangeNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("type") Type type,
            @JsonProperty("scope") Scope scope,
            @JsonProperty("partitioningScheme") PartitioningScheme partitioningScheme,
            @JsonProperty("sources") List<PlanNode> sources,
            @JsonProperty("inputs") List<List<Symbol>> inputs)
    {
        super(id);

        requireNonNull(type, "type is null");
        requireNonNull(scope, "scope is null");
        requireNonNull(sources, "sources is null");
        requireNonNull(partitioningScheme, "partitioningScheme is null");
        requireNonNull(inputs, "inputs is null");

        checkArgument(!inputs.isEmpty(), "inputs is empty");
        checkArgument(inputs.stream().allMatch(inputSymbols -> inputSymbols.size() == partitioningScheme.getOutputLayout().size()), "Input symbols do not match output symbols");
        checkArgument(inputs.size() == sources.size(), "Must have same number of input lists as sources");
        for (int i = 0; i < inputs.size(); i++) {
            checkArgument(sources.get(i).getOutputSymbols().containsAll(inputs.get(i)), "Source does not supply all required input symbols");
        }

        checkArgument(scope != LOCAL || partitioningScheme.getPartitioning().getArguments().stream().allMatch(ArgumentBinding::isVariable),
                "local exchanges do not support constant partition function arguments");

        checkArgument(scope != REMOTE || type == Type.REPARTITION || !partitioningScheme.isReplicateNulls(), "Only REPARTITION can remotely replicate nulls");

        this.type = type;
        this.sources = sources;
        this.scope = scope;
        this.partitioningScheme = partitioningScheme;
        this.inputs = ImmutableList.copyOf(inputs);
    }

    public static ExchangeNode partitionedExchange(PlanNodeId id, Scope scope, PlanNode child, List<Symbol> partitioningColumns, Optional<Symbol> hashColumns)
    {
        return partitionedExchange(id, scope, child, partitioningColumns, hashColumns, false);
    }

    public static ExchangeNode partitionedExchange(PlanNodeId id, Scope scope, PlanNode child, List<Symbol> partitioningColumns, Optional<Symbol> hashColumns, boolean nullsReplicated)
    {
        return partitionedExchange(
                id,
                scope,
                child,
                new PartitioningScheme(
                        Partitioning.create(FIXED_HASH_DISTRIBUTION, partitioningColumns),
                        child.getOutputSymbols(),
                        hashColumns,
                        nullsReplicated,
                        Optional.empty()));
    }

    public static ExchangeNode partitionedExchange(PlanNodeId id, Scope scope, PlanNode child, PartitioningScheme partitioningScheme)
    {
        if (partitioningScheme.getPartitioning().getHandle().isSingleNode()) {
            return gatheringExchange(id, scope, child);
        }
        return new ExchangeNode(
                id,
                ExchangeNode.Type.REPARTITION,
                scope,
                partitioningScheme,
                ImmutableList.of(child),
                ImmutableList.of(partitioningScheme.getOutputLayout()));
    }

    public static ExchangeNode replicatedExchange(PlanNodeId id, Scope scope, PlanNode child)
    {
        return new ExchangeNode(
                id,
                ExchangeNode.Type.REPLICATE,
                scope,
                new PartitioningScheme(Partitioning.create(FIXED_BROADCAST_DISTRIBUTION, ImmutableList.of()), child.getOutputSymbols()),
                ImmutableList.of(child),
                ImmutableList.of(child.getOutputSymbols()));
    }

    public static ExchangeNode gatheringExchange(PlanNodeId id, Scope scope, PlanNode child)
    {
        return new ExchangeNode(
                id,
                ExchangeNode.Type.GATHER,
                scope,
                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), child.getOutputSymbols()),
                ImmutableList.of(child),
                ImmutableList.of(child.getOutputSymbols()));
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
    public List<Symbol> getOutputSymbols()
    {
        return partitioningScheme.getOutputLayout();
    }

    @JsonProperty
    public PartitioningScheme getPartitioningScheme()
    {
        return partitioningScheme;
    }

    @JsonProperty
    public List<List<Symbol>> getInputs()
    {
        return inputs;
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context)
    {
        return visitor.visitExchange(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new ExchangeNode(getId(), type, scope, partitioningScheme, newChildren, inputs);
    }
}
