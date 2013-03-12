package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.analyzer.Symbol;
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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.Immutable;
import java.util.List;
import java.util.Map;

@Immutable
public class PlanFragment
{
    private final PlanFragmentId id;
    private final PlanNode root;
    private final PlanNodeId partitionedSource;
    private final Map<Symbol, Type> symbols;

    @JsonCreator
    public PlanFragment(@JsonProperty("id") PlanFragmentId id, @JsonProperty("partitionedSource") PlanNodeId partitionedSource, @JsonProperty("symbols") Map<Symbol, Type> symbols, @JsonProperty("root") PlanNode root)
    {
        Preconditions.checkNotNull(id, "id is null");
        Preconditions.checkNotNull(symbols, "symbols is null");
        Preconditions.checkNotNull(root, "root is null");

        this.id = id;
        this.root = root;
        this.partitionedSource = partitionedSource;
        this.symbols = symbols;
    }

    @JsonProperty("id")
    public PlanFragmentId getId()
    {
        return id;
    }

    public boolean isPartitioned()
    {
        return partitionedSource != null;
    }

    @JsonProperty("partitionedSource")
    public PlanNodeId getPartitionedSource()
    {
        return partitionedSource;
    }

    @JsonProperty("root")
    public PlanNode getRoot()
    {
        return root;
    }

    @JsonProperty("symbols")
    public Map<Symbol, Type> getSymbols()
    {
        return symbols;
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
