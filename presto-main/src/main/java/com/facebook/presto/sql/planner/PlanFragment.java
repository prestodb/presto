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

import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.util.IterableTransformer;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableSet;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

@Immutable
public class PlanFragment
{
    public enum PlanDistribution
    {
        NONE,
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
    private final PlanDistribution distribution;
    private final PlanNodeId partitionedSource;
    private final List<TupleInfo> tupleInfos;
    private final List<PlanNode> sources;
    private final Set<PlanNodeId> sourceIds;
    private final OutputPartitioning outputPartitioning;

    @JsonCreator
    public PlanFragment(
            @JsonProperty("id") PlanFragmentId id,
            @JsonProperty("root") PlanNode root,
            @JsonProperty("symbols") Map<Symbol, Type> symbols,
            @JsonProperty("distribution") PlanDistribution distribution,
            @JsonProperty("partitionedSource") PlanNodeId partitionedSource,
            @JsonProperty("outputPartitioning") OutputPartitioning outputPartitioning)
    {
        this.id = checkNotNull(id, "id is null");
        this.root = checkNotNull(root, "root is null");
        this.symbols = checkNotNull(symbols, "symbols is null");
        this.distribution = checkNotNull(distribution, "distribution is null");
        this.partitionedSource = partitionedSource;

        tupleInfos = IterableTransformer.on(root.getOutputSymbols())
                .transform(Functions.forMap(symbols))
                .transform(Type.toRaw())
                .transform(new Function<TupleInfo.Type, TupleInfo>()
                {
                    @Override
                    public TupleInfo apply(TupleInfo.Type input)
                    {
                        return new TupleInfo(input);
                    }
                })
                .list();

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

    public List<TupleInfo> getTupleInfos()
    {
        return tupleInfos;
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

        if (node.getSources().isEmpty() || node.getId().equals(partitionedSource)) {
            builder.add(node);
        }
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("id", id)
                .add("distribution", distribution)
                .add("partitionedSource", partitionedSource)
                .add("outputPartitioning", outputPartitioning)
                .toString();
    }

    public static Function<PlanFragment, PlanFragmentId> idGetter()
    {
        return new Function<PlanFragment, PlanFragmentId>()
        {
            @Override
            public PlanFragmentId apply(PlanFragment input)
            {
                return input.getId();
            }
        };
    }
}
