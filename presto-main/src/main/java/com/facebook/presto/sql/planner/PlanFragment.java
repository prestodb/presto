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
import com.facebook.presto.sql.planner.plan.IndexSourceNode;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
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

import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

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

    public static enum OutputPartitioning
    {
        NONE,
        HASH
    }

    private final PlanFragmentId id;
    private final PlanNode root;
    private final Map<Symbol, Type> symbols;
    private final List<Symbol> outputLayout;
    private final PlanDistribution distribution;
    private final PlanNodeId partitionedSource;
    private final List<Type> types;
    private final List<PlanNode> sources;
    private final Set<PlanNodeId> sourceIds;
    private final OutputPartitioning outputPartitioning;
    private final List<Symbol> partitionBy;
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
            @JsonProperty("partitionBy") List<Symbol> partitionBy,
            @JsonProperty("hash") Optional<Symbol> hash)
    {
        this.id = checkNotNull(id, "id is null");
        this.root = checkNotNull(root, "root is null");
        this.symbols = checkNotNull(symbols, "symbols is null");
        this.outputLayout = checkNotNull(outputLayout, "outputLayout is null");
        this.distribution = checkNotNull(distribution, "distribution is null");
        this.partitionedSource = partitionedSource;
        this.partitionBy = ImmutableList.copyOf(checkNotNull(partitionBy, "partitionBy is null"));
        this.hash = hash;

        checkArgument(ImmutableSet.copyOf(root.getOutputSymbols()).containsAll(outputLayout),
                "Root node outputs (%s) don't include all fragment outputs (%s)", root.getOutputSymbols(), outputLayout);

        types = root.getOutputSymbols().stream()
                .map(symbols::get)
                .collect(toImmutableList());

        ImmutableList.Builder<PlanNode> sources = ImmutableList.builder();
        findSources(root, sources, partitionedSource);
        this.sources = sources.build();

        ImmutableSet.Builder<PlanNodeId> sourceIds = ImmutableSet.builder();
        for (PlanNode source : this.sources) {
            sourceIds.add(source.getId());
        }
        if (partitionedSource != null) {
            sourceIds.add(partitionedSource);
        }
        this.sourceIds = sourceIds.build();

        this.outputPartitioning = checkNotNull(outputPartitioning, "outputPartitioning is null");
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
    public List<Symbol> getPartitionBy()
    {
        return partitionBy;
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

    public List<PlanNode> getSources()
    {
        return sources;
    }

    public Set<PlanNodeId> getSourceIds()
    {
        return sourceIds;
    }

    private static void findSources(PlanNode node, Builder<PlanNode> builder, PlanNodeId partitionedSource)
    {
        for (PlanNode source : node.getSources()) {
            findSources(source, builder, partitionedSource);
        }

        if ((node.getSources().isEmpty() && !(node instanceof IndexSourceNode)) || node.getId().equals(partitionedSource)) {
            builder.add(node);
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
