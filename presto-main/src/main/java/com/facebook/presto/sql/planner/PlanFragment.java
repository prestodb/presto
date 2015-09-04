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
package com.facebook.presto.sql.planner;

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableSet;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@Immutable
public class PlanFragment
{
    public enum PlanDistribution
    {
        SINGLE,
        FIXED,
        SOURCE,
        COORDINATOR_ONLY
    }

    public enum OutputPartitioning
    {
        NONE,
        HASH,
        ROUND_ROBIN
    }

    public static enum NullPartitioning
    {
        HASH, // Nulls are treated no different from any other elements
        REPLICATE, // Instead of being hashed to one partition, nulls are replicated to every partition
    }

    private final PlanFragmentId id;
    private final PlanNode root;
    private final Map<Symbol, Type> symbols;
    private final List<Symbol> outputLayout;
    private final PlanDistribution distribution;
    private final PlanNodeId partitionedSource;
    private final List<Type> types;
    private final PlanNode partitionedSourceNode;
    private final List<RemoteSourceNode> remoteSourceNodes;
    private final OutputPartitioning outputPartitioning;
    private final Optional<List<Symbol>> partitionBy;
    private final Optional<NullPartitioning> nullPartitionPolicy;
    private final Optional<Symbol> hash;

    @JsonCreator
    public PlanFragment(
            @JsonProperty("id") PlanFragmentId id,
            @JsonProperty("root") PlanNode root,
            @JsonProperty("symbols") Map<Symbol, Type> symbols,
            @JsonProperty("outputLayout") List<Symbol> outputLayout,
            @JsonProperty("distribution") PlanDistribution distribution,
            @JsonProperty("partitionedSource") PlanNodeId partitionedSource,
            @JsonProperty("outputPartitioning") OutputPartitioning outputPartitioning,
            @JsonProperty("partitionBy") Optional<List<Symbol>> partitionBy,
            @JsonProperty("nullPartitionPolicy") Optional<NullPartitioning> nullPartitionPolicy,
            @JsonProperty("hash") Optional<Symbol> hash)
    {
        this.id = requireNonNull(id, "id is null");
        this.root = requireNonNull(root, "root is null");
        this.symbols = requireNonNull(symbols, "symbols is null");
        this.outputLayout = requireNonNull(outputLayout, "outputLayout is null");
        this.distribution = requireNonNull(distribution, "distribution is null");
        this.partitionedSource = partitionedSource;
        this.partitionBy = requireNonNull(partitionBy, "partitionBy is null").map(ImmutableList::copyOf);
        this.nullPartitionPolicy = requireNonNull(nullPartitionPolicy, "nullPartitioningPolicy is null");
        this.hash = hash;

        checkArgument(ImmutableSet.copyOf(root.getOutputSymbols()).containsAll(outputLayout),
                "Root node outputs (%s) don't include all fragment outputs (%s)", root.getOutputSymbols(), outputLayout);

        types = outputLayout.stream()
                .map(symbols::get)
                .collect(toImmutableList());

        this.partitionedSourceNode = findSource(root, partitionedSource);

        ImmutableList.Builder<RemoteSourceNode> remoteSourceNodes = ImmutableList.builder();
        findRemoteSourceNodes(root, remoteSourceNodes);
        this.remoteSourceNodes = remoteSourceNodes.build();

        this.outputPartitioning = requireNonNull(outputPartitioning, "outputPartitioning is null");
    }

    @JsonProperty
    public PlanFragmentId getId()
    {
        return id;
    }

    @JsonProperty
    public PlanNode getRoot()
    {
        return root;
    }

    @JsonProperty
    public Map<Symbol, Type> getSymbols()
    {
        return symbols;
    }

    @JsonProperty
    public List<Symbol> getOutputLayout()
    {
        return outputLayout;
    }

    @JsonProperty
    public PlanDistribution getDistribution()
    {
        return distribution;
    }

    @JsonProperty
    public PlanNodeId getPartitionedSource()
    {
        return partitionedSource;
    }

    @JsonProperty
    public OutputPartitioning getOutputPartitioning()
    {
        return outputPartitioning;
    }

    @JsonProperty
    public Optional<List<Symbol>> getPartitionBy()
    {
        return partitionBy;
    }

    public Optional<NullPartitioning> getNullPartitionPolicy()
    {
        return nullPartitionPolicy;
    }

    @JsonProperty
    public Optional<Symbol> getHash()
    {
        return hash;
    }

    public List<Type> getTypes()
    {
        return types;
    }

    public PlanNode getPartitionedSourceNode()
    {
        return partitionedSourceNode;
    }

    public boolean isLeaf()
    {
        return remoteSourceNodes.isEmpty();
    }

    public List<RemoteSourceNode> getRemoteSourceNodes()
    {
        return remoteSourceNodes;
    }

    private static PlanNode findSource(PlanNode node, PlanNodeId nodeId)
    {
        if (node.getId().equals(nodeId)) {
            return node;
        }

        return node.getSources().stream()
                .map(source -> findSource(source, nodeId))
                .filter(Objects::nonNull)
                .findAny()
                .orElse(null);
    }

    private static void findRemoteSourceNodes(PlanNode node, Builder<RemoteSourceNode> builder)
    {
        for (PlanNode source : node.getSources()) {
            findRemoteSourceNodes(source, builder);
        }

        if (node instanceof RemoteSourceNode) {
            builder.add((RemoteSourceNode) node);
        }
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("id", id)
                .add("distribution", distribution)
                .add("partitionedSource", partitionedSource)
                .add("outputPartitioning", outputPartitioning)
                .add("hash", hash)
                .toString();
    }
}
