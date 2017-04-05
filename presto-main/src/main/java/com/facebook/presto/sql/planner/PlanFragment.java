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
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

@Immutable
public class PlanFragment
{
    private final PlanFragmentId id;
    private final PlanNode root;
    private final Map<Symbol, Type> symbols;
    private final PartitioningHandle partitioning;
    private final List<PlanNodeId> partitionedSources;
    private final Set<PlanNodeId> partitionedSourcesSet;
    private final List<Type> types;
    private final Set<PlanNode> partitionedSourceNodes;
    private final List<RemoteSourceNode> remoteSourceNodes;
    private final PartitioningScheme partitioningScheme;

    @JsonCreator
    public PlanFragment(
            @JsonProperty("id") PlanFragmentId id,
            @JsonProperty("root") PlanNode root,
            @JsonProperty("symbols") Map<Symbol, Type> symbols,
            @JsonProperty("partitioning") PartitioningHandle partitioning,
            @JsonProperty("partitionedSources") List<PlanNodeId> partitionedSources,
            @JsonProperty("partitioningScheme") PartitioningScheme partitioningScheme)
    {
        this.id = requireNonNull(id, "id is null");
        this.root = requireNonNull(root, "root is null");
        this.symbols = requireNonNull(symbols, "symbols is null");
        this.partitioning = requireNonNull(partitioning, "partitioning is null");
        this.partitionedSources = ImmutableList.copyOf(requireNonNull(partitionedSources, "partitionedSources is null"));
        this.partitionedSourcesSet = ImmutableSet.copyOf(partitionedSources);

        checkArgument(partitionedSourcesSet.size() == partitionedSources.size(), "partitionedSources contains duplicates");
        checkArgument(ImmutableSet.copyOf(root.getOutputSymbols()).containsAll(partitioningScheme.getOutputLayout()),
                "Root node outputs (%s) does not include all fragment outputs (%s)", root.getOutputSymbols(), partitioningScheme.getOutputLayout());

        types = partitioningScheme.getOutputLayout().stream()
                .map(symbols::get)
                .collect(toImmutableList());

        this.partitionedSourceNodes = findSources(root, partitionedSources);

        ImmutableList.Builder<RemoteSourceNode> remoteSourceNodes = ImmutableList.builder();
        findRemoteSourceNodes(root, remoteSourceNodes);
        this.remoteSourceNodes = remoteSourceNodes.build();

        this.partitioningScheme = requireNonNull(partitioningScheme, "partitioningScheme is null");
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
    public PartitioningHandle getPartitioning()
    {
        return partitioning;
    }

    @JsonProperty
    public List<PlanNodeId> getPartitionedSources()
    {
        return partitionedSources;
    }

    public boolean isPartitionedSources(PlanNodeId nodeId)
    {
        return partitionedSourcesSet.contains(nodeId);
    }

    @JsonProperty
    public PartitioningScheme getPartitioningScheme()
    {
        return partitioningScheme;
    }

    public List<Type> getTypes()
    {
        return types;
    }

    public Set<PlanNode> getPartitionedSourceNodes()
    {
        return partitionedSourceNodes;
    }

    public boolean isLeaf()
    {
        return remoteSourceNodes.isEmpty();
    }

    public List<RemoteSourceNode> getRemoteSourceNodes()
    {
        return remoteSourceNodes;
    }

    private static Set<PlanNode> findSources(PlanNode node, Iterable<PlanNodeId> nodeIds)
    {
        ImmutableSet.Builder<PlanNode> nodes = ImmutableSet.builder();
        findSources(node, ImmutableSet.copyOf(nodeIds), nodes);
        return nodes.build();
    }

    private static void findSources(PlanNode node, Set<PlanNodeId> nodeIds, ImmutableSet.Builder<PlanNode> nodes)
    {
        if (nodeIds.contains(node.getId())) {
            nodes.add(node);
        }

        node.getSources().stream()
                .flatMap(source -> findSources(source, nodeIds).stream())
                .forEach(nodes::add);
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

    public PlanFragment withBucketToPartition(Optional<int[]> bucketToPartition)
    {
        return new PlanFragment(id, root, symbols, partitioning, partitionedSources, partitioningScheme.withBucketToPartition(bucketToPartition));
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("id", id)
                .add("partitioning", partitioning)
                .add("partitionedSource", partitionedSources)
                .add("partitionFunction", partitioningScheme)
                .toString();
    }
}
