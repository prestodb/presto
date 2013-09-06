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

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

@Immutable
public class PlanFragment
{
    public enum Partitioning {
        NONE,
        HASH,
        SOURCE
    }

    private final PlanFragmentId id;
    private final PlanNode root;
    private final Map<Symbol, Type> symbols;
    private final Partitioning partitioning;
    private final PlanNodeId partitionedSource;

    @JsonCreator
    public PlanFragment(
            @JsonProperty("id") PlanFragmentId id,
            @JsonProperty("root") PlanNode root,
            @JsonProperty("symbols") Map<Symbol, Type> symbols,
            @JsonProperty("partitioning") Partitioning partitioning,
            @JsonProperty("partitionedSource") PlanNodeId partitionedSource)
    {
        this.id = checkNotNull(id, "id is null");
        this.root = checkNotNull(root, "root is null");
        this.symbols = checkNotNull(symbols, "symbols is null");
        this.partitioning = checkNotNull(partitioning, "partitioning is null");
        this.partitionedSource = partitionedSource;
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
    public Partitioning getPartitioning()
    {
        return partitioning;
    }

    @JsonProperty
    public PlanNodeId getPartitionedSource()
    {
        return partitionedSource;
    }

    public List<TupleInfo> getTupleInfos()
    {
        return ImmutableList.copyOf(IterableTransformer.on(getRoot().getOutputSymbols())
                .transform(Functions.forMap(getSymbols()))
                .transform(com.facebook.presto.sql.analyzer.Type.toRaw())
                .transform(new Function<TupleInfo.Type, TupleInfo>()
                {
                    @Override
                    public TupleInfo apply(TupleInfo.Type input)
                    {
                        return new TupleInfo(input);
                    }
                })
                .list());
    }

    public List<PlanNode> getSources()
    {
        ImmutableList.Builder<PlanNode> sources = ImmutableList.builder();
        findSources(root, sources);
        return sources.build();
    }

    private void findSources(PlanNode node, ImmutableList.Builder<PlanNode> builder)
    {
        for (PlanNode source : node.getSources()) {
            findSources(source, builder);
        }

        if (node.getSources().isEmpty()) {
            builder.add(node);
        }
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("id", id)
                .add("partitioning", partitioning)
                .add("partitionedSource", partitionedSource)
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
